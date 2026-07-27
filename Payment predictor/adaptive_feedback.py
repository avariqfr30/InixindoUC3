"""Structured, scoped feedback memory for an internal generation workflow."""
from __future__ import annotations

import hashlib
import os
import sqlite3
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping


class FeedbackError(ValueError):
    """Base error for feedback operations."""


class FeedbackValidationError(FeedbackError):
    """Raised when feedback input violates the bounded contract."""


class FeedbackNotFoundError(FeedbackError):
    """Raised when feedback is missing or owned by another user."""


def _bounded_text(value: Any, *, field: str, max_length: int, required: bool = False) -> str:
    text = str(value or "").strip()
    if required and not text:
        raise FeedbackValidationError(f"{field} wajib diisi.")
    if len(text) > max_length:
        raise FeedbackValidationError(f"{field} melebihi batas {max_length} karakter.")
    if any(ord(character) < 32 and character not in {"\n", "\t"} for character in text):
        raise FeedbackValidationError(f"{field} mengandung karakter yang tidak valid.")
    return text


class FeedbackPolicy:
    """Allowlisted Indonesian feedback reasons for one use case."""

    ALLOWED_MODES = {"adapt", "review", "positive"}

    def __init__(self, app_id: str, reasons: Mapping[str, Mapping[str, str]]):
        self.app_id = _bounded_text(app_id, field="app_id", max_length=32, required=True)
        normalized: Dict[str, Dict[str, str]] = {}
        for raw_code, raw_reason in reasons.items():
            code = _bounded_text(raw_code, field="reason_code", max_length=64, required=True)
            label = _bounded_text(raw_reason.get("label"), field="label", max_length=160, required=True)
            guidance = _bounded_text(raw_reason.get("guidance"), field="guidance", max_length=600)
            mode = _bounded_text(raw_reason.get("mode"), field="mode", max_length=16, required=True)
            if mode not in self.ALLOWED_MODES:
                raise FeedbackValidationError(f"Mode feedback tidak didukung: {mode}")
            if mode == "adapt" and not guidance:
                raise FeedbackValidationError(f"Panduan adaptasi wajib tersedia untuk {code}.")
            normalized[code] = {"label": label, "guidance": guidance, "mode": mode}
        if not normalized:
            raise FeedbackValidationError("Minimal satu alasan feedback wajib dikonfigurasi.")
        self.reasons = normalized

    def reason(self, reason_code: Any) -> Dict[str, str]:
        code = _bounded_text(reason_code, field="reason_code", max_length=64, required=True)
        reason = self.reasons.get(code)
        if reason is None:
            raise FeedbackValidationError("Alasan feedback tidak tersedia.")
        return {"reason_code": code, **reason}

    def public_options(self) -> Dict[str, List[Dict[str, str]]]:
        helpful = []
        needs_improvement = []
        for code, reason in self.reasons.items():
            item = {"reason_code": code, "label": reason["label"]}
            if reason["mode"] == "positive":
                helpful.append(item)
            else:
                needs_improvement.append(item)
        return {"helpful": helpful, "needs_improvement": needs_improvement}


def build_feedback_policy(
    app_id: str,
    reasons: Mapping[str, Mapping[str, str]],
) -> FeedbackPolicy:
    return FeedbackPolicy(app_id, reasons)


class FeedbackStore:
    """Persist feedback without storing generated outputs or source datasets."""

    def __init__(self, db_path: Path | str, policy: FeedbackPolicy):
        self.db_path = Path(db_path).expanduser()
        self.policy = policy
        self._lock = threading.RLock()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()
        try:
            os.chmod(self.db_path, 0o600)
        except OSError:
            pass

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(str(self.db_path), timeout=10)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        return connection

    def _initialize(self) -> None:
        with self._lock, self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS generation_feedback (
                    feedback_id TEXT PRIMARY KEY,
                    app_id TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    owner_hash TEXT NOT NULL,
                    context_key TEXT NOT NULL,
                    rating TEXT NOT NULL,
                    reason_code TEXT NOT NULL,
                    reason_label TEXT NOT NULL,
                    reason_mode TEXT NOT NULL,
                    guidance TEXT NOT NULL,
                    note TEXT NOT NULL DEFAULT '',
                    remember INTEGER NOT NULL DEFAULT 0,
                    regenerated_run_id TEXT NOT NULL DEFAULT '',
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    UNIQUE(app_id, run_id, owner_hash)
                );
                CREATE INDEX IF NOT EXISTS idx_generation_feedback_owner
                    ON generation_feedback(app_id, owner_hash, context_key, updated_at DESC);
                CREATE INDEX IF NOT EXISTS idx_generation_feedback_reason
                    ON generation_feedback(app_id, reason_code, updated_at DESC);

                CREATE TABLE IF NOT EXISTS approved_feedback_guidance (
                    app_id TEXT NOT NULL,
                    reason_code TEXT NOT NULL,
                    guidance TEXT NOT NULL,
                    approved_by_hash TEXT NOT NULL,
                    active INTEGER NOT NULL DEFAULT 1,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    PRIMARY KEY(app_id, reason_code)
                );
                """
            )
            connection.commit()

    @staticmethod
    def _owner_hash(owner_key: Any) -> str:
        normalized = _bounded_text(owner_key, field="owner_key", max_length=256, required=True)
        return hashlib.sha256(normalized.casefold().encode("utf-8")).hexdigest()

    def record(
        self,
        *,
        owner_key: Any,
        run_id: Any,
        context_key: Any,
        rating: Any,
        reason_code: Any,
        note: Any = "",
        remember: Any = False,
    ) -> Dict[str, Any]:
        owner_hash = self._owner_hash(owner_key)
        normalized_run = _bounded_text(run_id, field="run_id", max_length=128, required=True)
        normalized_context = _bounded_text(
            context_key, field="context_key", max_length=128, required=True
        )
        normalized_rating = _bounded_text(rating, field="rating", max_length=32, required=True)
        if normalized_rating not in {"helpful", "needs_improvement"}:
            raise FeedbackValidationError("Nilai feedback tidak didukung.")
        reason = self.policy.reason(reason_code)
        if normalized_rating == "helpful" and reason["mode"] != "positive":
            raise FeedbackValidationError("Alasan tidak cocok dengan feedback positif.")
        if normalized_rating == "needs_improvement" and reason["mode"] == "positive":
            raise FeedbackValidationError("Alasan tidak cocok dengan feedback perbaikan.")
        normalized_note = _bounded_text(note, field="note", max_length=500)
        remember_value = bool(remember) and reason["mode"] == "adapt"
        now = time.time()

        with self._lock, self._connect() as connection:
            existing = connection.execute(
                "SELECT feedback_id, created_at FROM generation_feedback "
                "WHERE app_id=? AND run_id=? AND owner_hash=?",
                (self.policy.app_id, normalized_run, owner_hash),
            ).fetchone()
            feedback_id = existing["feedback_id"] if existing else uuid.uuid4().hex
            created_at = float(existing["created_at"]) if existing else now
            connection.execute(
                """
                INSERT INTO generation_feedback (
                    feedback_id, app_id, run_id, owner_hash, context_key,
                    rating, reason_code, reason_label, reason_mode, guidance,
                    note, remember, regenerated_run_id, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', ?, ?)
                ON CONFLICT(app_id, run_id, owner_hash) DO UPDATE SET
                    context_key=excluded.context_key,
                    rating=excluded.rating,
                    reason_code=excluded.reason_code,
                    reason_label=excluded.reason_label,
                    reason_mode=excluded.reason_mode,
                    guidance=excluded.guidance,
                    note=excluded.note,
                    remember=excluded.remember,
                    regenerated_run_id='',
                    updated_at=excluded.updated_at
                """,
                (
                    feedback_id,
                    self.policy.app_id,
                    normalized_run,
                    owner_hash,
                    normalized_context,
                    normalized_rating,
                    reason["reason_code"],
                    reason["label"],
                    reason["mode"],
                    reason["guidance"],
                    normalized_note,
                    1 if remember_value else 0,
                    created_at,
                    now,
                ),
            )
            consensus = connection.execute(
                "SELECT COUNT(*) AS feedback_count, COUNT(DISTINCT owner_hash) AS distinct_user_count "
                "FROM generation_feedback WHERE app_id=? AND reason_code=? AND rating='needs_improvement'",
                (self.policy.app_id, reason["reason_code"]),
            ).fetchone()
            connection.commit()

        understood = reason["guidance"] or (
            "Masukan dicatat untuk pemeriksaan data dan aturan sebelum perubahan diterapkan."
            if reason["mode"] == "review"
            else "Hasil ini dicatat sebagai pengalaman yang membantu."
        )
        return {
            "feedback_id": feedback_id,
            "rating": normalized_rating,
            "reason_code": reason["reason_code"],
            "reason_label": reason["label"],
            "understood_guidance": understood,
            "can_regenerate": normalized_rating == "needs_improvement" and reason["mode"] == "adapt",
            "remembered": remember_value,
            "consensus": {
                "feedback_count": int(consensus["feedback_count"] or 0),
                "distinct_user_count": int(consensus["distinct_user_count"] or 0),
            },
        }

    def regeneration_guidance(
        self,
        feedback_id: Any,
        *,
        owner_key: Any,
        run_id: Any,
    ) -> Dict[str, str]:
        normalized_id = _bounded_text(
            feedback_id, field="feedback_id", max_length=128, required=True
        )
        normalized_run = _bounded_text(run_id, field="run_id", max_length=128, required=True)
        owner_hash = self._owner_hash(owner_key)
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM generation_feedback WHERE feedback_id=? AND app_id=? "
                "AND owner_hash=? AND run_id=?",
                (normalized_id, self.policy.app_id, owner_hash, normalized_run),
            ).fetchone()
        if row is None:
            raise FeedbackNotFoundError("Feedback tidak ditemukan atau bukan milik pengguna ini.")
        if row["rating"] != "needs_improvement" or row["reason_mode"] != "adapt":
            raise FeedbackValidationError("Feedback ini tidak dapat digunakan untuk membuat ulang hasil.")
        return {
            "feedback_id": row["feedback_id"],
            "reason_code": row["reason_code"],
            "guidance": row["guidance"],
            "note": row["note"],
        }

    def link_regeneration(
        self,
        feedback_id: Any,
        *,
        owner_key: Any,
        child_run_id: Any,
    ) -> None:
        normalized_id = _bounded_text(
            feedback_id, field="feedback_id", max_length=128, required=True
        )
        normalized_child = _bounded_text(
            child_run_id, field="child_run_id", max_length=128, required=True
        )
        owner_hash = self._owner_hash(owner_key)
        with self._lock, self._connect() as connection:
            cursor = connection.execute(
                "UPDATE generation_feedback SET regenerated_run_id=?, updated_at=? "
                "WHERE feedback_id=? AND app_id=? AND owner_hash=?",
                (normalized_child, time.time(), normalized_id, self.policy.app_id, owner_hash),
            )
            if cursor.rowcount != 1:
                raise FeedbackNotFoundError("Feedback tidak ditemukan atau bukan milik pengguna ini.")
            connection.commit()

    def guidance_for(self, owner_key: Any, context_key: Any, *, limit: int = 3) -> List[str]:
        owner_hash = self._owner_hash(owner_key)
        normalized_context = _bounded_text(
            context_key, field="context_key", max_length=128, required=True
        )
        bounded_limit = min(5, max(1, int(limit)))
        items: List[str] = []
        with self._connect() as connection:
            user_rows = connection.execute(
                """
                SELECT guidance FROM generation_feedback
                WHERE app_id=? AND owner_hash=? AND context_key=?
                    AND rating='needs_improvement' AND reason_mode='adapt'
                    AND remember=1 AND guidance<>''
                ORDER BY updated_at DESC
                """,
                (self.policy.app_id, owner_hash, normalized_context),
            ).fetchall()
            global_rows = connection.execute(
                "SELECT guidance FROM approved_feedback_guidance "
                "WHERE app_id=? AND active=1 ORDER BY updated_at DESC",
                (self.policy.app_id,),
            ).fetchall()
        for row in [*user_rows, *global_rows]:
            guidance = str(row["guidance"] or "").strip()
            if guidance and guidance not in items:
                items.append(guidance)
            if len(items) >= bounded_limit:
                break
        return items

    def approve_reason(self, reason_code: Any, *, approved_by: Any) -> None:
        reason = self.policy.reason(reason_code)
        if reason["mode"] != "adapt":
            raise FeedbackValidationError("Hanya panduan adaptasi yang dapat diaktifkan secara global.")
        approver_hash = self._owner_hash(approved_by)
        now = time.time()
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO approved_feedback_guidance (
                    app_id, reason_code, guidance, approved_by_hash,
                    active, created_at, updated_at
                ) VALUES (?, ?, ?, ?, 1, ?, ?)
                ON CONFLICT(app_id, reason_code) DO UPDATE SET
                    guidance=excluded.guidance,
                    approved_by_hash=excluded.approved_by_hash,
                    active=1,
                    updated_at=excluded.updated_at
                """,
                (
                    self.policy.app_id,
                    reason["reason_code"],
                    reason["guidance"],
                    approver_hash,
                    now,
                    now,
                ),
            )
            connection.commit()

    def retire_reason(self, reason_code: Any) -> None:
        reason = self.policy.reason(reason_code)
        with self._lock, self._connect() as connection:
            connection.execute(
                "UPDATE approved_feedback_guidance SET active=0, updated_at=? "
                "WHERE app_id=? AND reason_code=?",
                (time.time(), self.policy.app_id, reason["reason_code"]),
            )
            connection.commit()

    def review_snapshot(self) -> Dict[str, Any]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT reason_code, reason_label, reason_mode,
                    COUNT(*) AS feedback_count,
                    COUNT(DISTINCT owner_hash) AS distinct_user_count,
                    SUM(CASE WHEN rating='helpful' THEN 1 ELSE 0 END) AS helpful_count,
                    SUM(CASE WHEN rating='needs_improvement' THEN 1 ELSE 0 END) AS improvement_count
                FROM generation_feedback
                WHERE app_id=?
                GROUP BY reason_code, reason_label, reason_mode
                ORDER BY improvement_count DESC, helpful_count DESC, reason_code ASC
                """,
                (self.policy.app_id,),
            ).fetchall()
            approved = {
                row["reason_code"]: bool(row["active"])
                for row in connection.execute(
                    "SELECT reason_code, active FROM approved_feedback_guidance WHERE app_id=?",
                    (self.policy.app_id,),
                ).fetchall()
            }
        return {
            "app_id": self.policy.app_id,
            "reasons": [
                {
                    "reason_code": row["reason_code"],
                    "reason_label": row["reason_label"],
                    "reason_mode": row["reason_mode"],
                    "feedback_count": int(row["feedback_count"] or 0),
                    "distinct_user_count": int(row["distinct_user_count"] or 0),
                    "helpful_count": int(row["helpful_count"] or 0),
                    "improvement_count": int(row["improvement_count"] or 0),
                    "approved_global": bool(approved.get(row["reason_code"], False)),
                }
                for row in rows
            ],
        }

    def options(self) -> Dict[str, List[Dict[str, str]]]:
        return self.policy.public_options()
