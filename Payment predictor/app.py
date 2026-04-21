import argparse
import calendar
import copy
import concurrent.futures
import logging
import os
import re
import sqlite3
import threading
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path

from flask import Flask, current_app, g, jsonify, redirect, render_template, request, send_file, session, url_for
from flask_cors import CORS
import pandas as pd
from werkzeug.security import check_password_hash, generate_password_hash

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


class QueueCapacityError(Exception):
    def __init__(self, active_jobs, max_pending_jobs):
        self.active_jobs = active_jobs
        self.max_pending_jobs = max_pending_jobs
        super().__init__(
            f"Queue is full ({active_jobs}/{max_pending_jobs} active jobs). Please try again in a few minutes."
        )


class SessionLimitError(Exception):
    pass


class UserStore:
    USERNAME_PATTERN = re.compile(r"^[A-Za-z0-9_]{3,32}$")

    def __init__(self, db_path):
        self.db_path = str(db_path)
        self.lock = threading.Lock()
        self._initialize()

    def _connect(self):
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self):
        with self.lock, self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT NOT NULL UNIQUE,
                    password_hash TEXT NOT NULL,
                    created_at REAL NOT NULL
                )
                """
            )
            connection.commit()

    @classmethod
    def validate_username(cls, username):
        normalized = str(username or "").strip()
        if not cls.USERNAME_PATTERN.fullmatch(normalized):
            raise ValueError("Nama pengguna harus 3-32 karakter dan hanya boleh berisi huruf, angka, atau garis bawah.")
        return normalized

    @staticmethod
    def validate_password(password):
        normalized = str(password or "")
        if len(normalized) < 8:
            raise ValueError("Kata sandi harus minimal 8 karakter.")
        return normalized

    def create_user(self, username, password):
        normalized_username = self.validate_username(username)
        normalized_password = self.validate_password(password)
        password_hash = generate_password_hash(normalized_password, method="pbkdf2:sha256")

        with self.lock, self._connect() as connection:
            try:
                connection.execute(
                    """
                    INSERT INTO users (username, password_hash, created_at)
                    VALUES (?, ?, ?)
                    """,
                    (normalized_username, password_hash, time.time()),
                )
                connection.commit()
            except sqlite3.IntegrityError as exc:
                raise ValueError("Nama pengguna sudah terdaftar.") from exc

        return normalized_username

    def authenticate(self, username, password):
        normalized_username = str(username or "").strip()
        normalized_password = str(password or "")
        with self.lock, self._connect() as connection:
            row = connection.execute(
                "SELECT username, password_hash FROM users WHERE username = ?",
                (normalized_username,),
            ).fetchone()

        if not row or not check_password_hash(row["password_hash"], normalized_password):
            return None
        return row["username"]

    def has_users(self):
        with self.lock, self._connect() as connection:
            row = connection.execute("SELECT COUNT(*) AS total FROM users").fetchone()
        return bool(row and row["total"] > 0)


class ActiveSessionStore:
    def __init__(self, db_path):
        self.db_path = str(db_path)
        self.lock = threading.Lock()
        self._initialize()

    def _connect(self):
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self):
        with self.lock, self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS auth_sessions (
                    session_id TEXT PRIMARY KEY,
                    username TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    last_seen_at REAL NOT NULL,
                    revoked_at REAL,
                    revoked_reason TEXT,
                    ip_address TEXT,
                    user_agent TEXT
                )
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_auth_sessions_active
                ON auth_sessions (username, revoked_at, last_seen_at)
                """
            )
            connection.commit()

    def _cleanup_expired_unlocked(self, connection, now, idle_timeout_seconds, absolute_timeout_seconds):
        conditions = []
        params = []
        if idle_timeout_seconds > 0:
            conditions.append("last_seen_at <= ?")
            params.append(now - idle_timeout_seconds)
        if absolute_timeout_seconds > 0:
            conditions.append("created_at <= ?")
            params.append(now - absolute_timeout_seconds)
        if not conditions:
            return

        where_clause = " OR ".join(conditions)
        connection.execute(
            f"""
            UPDATE auth_sessions
            SET revoked_at = ?, revoked_reason = 'timeout'
            WHERE revoked_at IS NULL
              AND ({where_clause})
            """,
            (now, *params),
        )

    def _count_active_unlocked(self, connection):
        row = connection.execute(
            """
            SELECT COUNT(*) AS total
            FROM auth_sessions
            WHERE revoked_at IS NULL
            """
        ).fetchone()
        return int(row["total"] if row else 0)

    def create_session(
        self,
        username,
        ip_address,
        user_agent,
        idle_timeout_seconds,
        absolute_timeout_seconds,
        max_global_sessions,
        max_sessions_per_user,
    ):
        now = time.time()
        max_global_sessions = int(max_global_sessions or 0)
        max_sessions_per_user = int(max_sessions_per_user or 0)
        with self.lock, self._connect() as connection:
            self._cleanup_expired_unlocked(connection, now, idle_timeout_seconds, absolute_timeout_seconds)

            active_global = self._count_active_unlocked(connection)
            if max_global_sessions > 0 and active_global >= max_global_sessions:
                raise SessionLimitError(
                    "Akses sementara penuh karena sesi aktif sudah mencapai batas server. Coba lagi beberapa menit lagi."
                )

            if max_sessions_per_user > 0:
                active_rows = connection.execute(
                    """
                    SELECT session_id
                    FROM auth_sessions
                    WHERE username = ? AND revoked_at IS NULL
                    ORDER BY last_seen_at ASC
                    """,
                    (username,),
                ).fetchall()
                overflow = len(active_rows) - max_sessions_per_user + 1
                if overflow > 0:
                    session_ids_to_revoke = [row["session_id"] for row in active_rows[:overflow]]
                    connection.executemany(
                        """
                        UPDATE auth_sessions
                        SET revoked_at = ?, revoked_reason = 'superseded'
                        WHERE session_id = ? AND revoked_at IS NULL
                        """,
                        [(now, session_id) for session_id in session_ids_to_revoke],
                    )

            session_id = uuid.uuid4().hex
            connection.execute(
                """
                INSERT INTO auth_sessions (
                    session_id,
                    username,
                    created_at,
                    last_seen_at,
                    ip_address,
                    user_agent
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (session_id, username, now, now, ip_address, user_agent),
            )
            connection.commit()
            return session_id

    def revoke_session(self, session_id, reason="logout"):
        if not session_id:
            return
        now = time.time()
        with self.lock, self._connect() as connection:
            connection.execute(
                """
                UPDATE auth_sessions
                SET revoked_at = ?, revoked_reason = ?
                WHERE session_id = ? AND revoked_at IS NULL
                """,
                (now, reason, session_id),
            )
            connection.commit()

    def validate_and_touch(self, session_id, username, idle_timeout_seconds, absolute_timeout_seconds):
        if not session_id or not username:
            return False, "missing"

        now = time.time()
        with self.lock, self._connect() as connection:
            self._cleanup_expired_unlocked(connection, now, idle_timeout_seconds, absolute_timeout_seconds)
            row = connection.execute(
                """
                SELECT session_id, username, revoked_at
                FROM auth_sessions
                WHERE session_id = ?
                """,
                (session_id,),
            ).fetchone()

            if not row:
                return False, "not_found"
            if row["revoked_at"] is not None:
                return False, "revoked"
            if row["username"] != username:
                connection.execute(
                    """
                    UPDATE auth_sessions
                    SET revoked_at = ?, revoked_reason = 'identity_mismatch'
                    WHERE session_id = ? AND revoked_at IS NULL
                    """,
                    (now, session_id),
                )
                connection.commit()
                return False, "identity_mismatch"

            connection.execute(
                """
                UPDATE auth_sessions
                SET last_seen_at = ?
                WHERE session_id = ?
                """,
                (now, session_id),
            )
            connection.commit()
            return True, "active"

    def get_security_snapshot(
        self,
        idle_timeout_seconds,
        absolute_timeout_seconds,
        max_global_sessions,
        max_sessions_per_user,
    ):
        now = time.time()
        with self.lock, self._connect() as connection:
            self._cleanup_expired_unlocked(connection, now, idle_timeout_seconds, absolute_timeout_seconds)
            active_sessions_row = connection.execute(
                """
                SELECT COUNT(*) AS total
                FROM auth_sessions
                WHERE revoked_at IS NULL
                """
            ).fetchone()
            active_users_row = connection.execute(
                """
                SELECT COUNT(DISTINCT username) AS total
                FROM auth_sessions
                WHERE revoked_at IS NULL
                """
            ).fetchone()
            connection.commit()

        return {
            "activeSessions": int(active_sessions_row["total"] if active_sessions_row else 0),
            "activeUsers": int(active_users_row["total"] if active_users_row else 0),
            "maxActiveSessions": int(max_global_sessions or 0),
            "maxSessionsPerUser": int(max_sessions_per_user or 0),
            "idleTimeoutMinutes": round((idle_timeout_seconds or 0) / 60, 2),
            "absoluteTimeoutHours": round((absolute_timeout_seconds or 0) / 3600, 2),
        }


class ReportJobStore:
    def __init__(self, db_path, artifacts_dir):
        self.db_path = str(db_path)
        self.artifacts_dir = Path(artifacts_dir)
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)
        self.lock = threading.Lock()
        self._initialize()

    def _connect(self):
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self):
        with self.lock, self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS report_jobs (
                    job_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    started_at REAL,
                    duration_seconds REAL,
                    error TEXT,
                    filename TEXT,
                    artifact_path TEXT,
                    notes_preview TEXT,
                    fallback_used INTEGER DEFAULT 0,
                    osint_available INTEGER DEFAULT 0,
                    visuals_included INTEGER DEFAULT 0,
                    quality_gate_passed INTEGER DEFAULT 0,
                    completeness_score REAL DEFAULT 0
                )
                """
            )
            existing_columns = {
                row["name"]
                for row in connection.execute("PRAGMA table_info(report_jobs)").fetchall()
            }
            column_migrations = {
                "fallback_used": "ALTER TABLE report_jobs ADD COLUMN fallback_used INTEGER DEFAULT 0",
                "osint_available": "ALTER TABLE report_jobs ADD COLUMN osint_available INTEGER DEFAULT 0",
                "visuals_included": "ALTER TABLE report_jobs ADD COLUMN visuals_included INTEGER DEFAULT 0",
                "quality_gate_passed": "ALTER TABLE report_jobs ADD COLUMN quality_gate_passed INTEGER DEFAULT 0",
                "completeness_score": "ALTER TABLE report_jobs ADD COLUMN completeness_score REAL DEFAULT 0",
            }
            for column_name, migration_sql in column_migrations.items():
                if column_name not in existing_columns:
                    try:
                        connection.execute(migration_sql)
                    except sqlite3.OperationalError as exc:
                        if "duplicate column name" not in str(exc).lower():
                            raise
            connection.commit()

    def recover_incomplete_jobs(self):
        with self.lock, self._connect() as connection:
            connection.execute(
                """
                UPDATE report_jobs
                SET status = 'error',
                    error = 'Service restarted before this job completed.',
                    updated_at = ?
                WHERE status IN ('queued', 'running')
                """,
                (time.time(),),
            )
            connection.commit()

    def cleanup_expired(self, retention_seconds):
        cutoff_time = time.time() - retention_seconds
        with self.lock, self._connect() as connection:
            rows = connection.execute(
                """
                SELECT job_id, artifact_path
                FROM report_jobs
                WHERE status IN ('ready', 'error') AND updated_at < ?
                """,
                (cutoff_time,),
            ).fetchall()

            for row in rows:
                artifact_path = row["artifact_path"]
                if artifact_path:
                    try:
                        Path(artifact_path).unlink(missing_ok=True)
                    except OSError:
                        logger.warning("Unable to remove expired artifact for job %s.", row["job_id"])

            connection.execute(
                """
                DELETE FROM report_jobs
                WHERE status IN ('ready', 'error') AND updated_at < ?
                """,
                (cutoff_time,),
            )
            connection.commit()

    def create_job(self, job_id, notes_preview):
        now = time.time()
        with self.lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO report_jobs (
                    job_id,
                    status,
                    created_at,
                    updated_at,
                    notes_preview
                ) VALUES (?, 'queued', ?, ?, ?)
                """,
                (job_id, now, now, notes_preview),
            )
            connection.commit()

    def update_job(self, job_id, **fields):
        if not fields:
            return

        assignments = []
        values = []
        for field_name, field_value in fields.items():
            assignments.append(f"{field_name} = ?")
            values.append(field_value)
        values.append(job_id)

        with self.lock, self._connect() as connection:
            connection.execute(
                f"UPDATE report_jobs SET {', '.join(assignments)} WHERE job_id = ?",
                values,
            )
            connection.commit()

    def get_job(self, job_id):
        with self.lock, self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM report_jobs WHERE job_id = ?",
                (job_id,),
            ).fetchone()

        if not row:
            return None

        job = dict(row)
        for field_name in ("fallback_used", "osint_available", "visuals_included", "quality_gate_passed"):
            job[field_name] = bool(job.get(field_name))
        job["completeness_score"] = float(job.get("completeness_score") or 0)
        return job

    def count_active_jobs(self):
        with self.lock, self._connect() as connection:
            row = connection.execute(
                "SELECT COUNT(*) AS total FROM report_jobs WHERE status IN ('queued', 'running')"
            ).fetchone()
        return int(row["total"] if row else 0)

    def count_queued_ahead(self, job_id, created_at):
        with self.lock, self._connect() as connection:
            row = connection.execute(
                """
                SELECT COUNT(*) AS total
                FROM report_jobs
                WHERE job_id != ?
                  AND status IN ('queued', 'running')
                  AND created_at < ?
                """,
                (job_id, created_at),
            ).fetchone()
        return int(row["total"] if row else 0)

    def get_status_counts(self):
        counts = {"queuedJobs": 0, "runningJobs": 0, "readyJobs": 0, "errorJobs": 0}
        with self.lock, self._connect() as connection:
            rows = connection.execute(
                """
                SELECT status, COUNT(*) AS total
                FROM report_jobs
                GROUP BY status
                """
            ).fetchall()

        for row in rows:
            status_key = f"{row['status']}Jobs"
            if status_key in counts:
                counts[status_key] = int(row["total"])
        return counts

    def get_recent_metrics(self, window_hours):
        since_timestamp = time.time() - (window_hours * 3600)
        with self.lock, self._connect() as connection:
            row = connection.execute(
                """
                SELECT
                    COUNT(*) AS completed_jobs,
                    SUM(CASE WHEN status = 'ready' THEN 1 ELSE 0 END) AS ready_jobs,
                    SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS error_jobs,
                    SUM(CASE WHEN fallback_used = 1 THEN 1 ELSE 0 END) AS fallback_jobs,
                    SUM(CASE WHEN quality_gate_passed = 1 THEN 1 ELSE 0 END) AS accepted_jobs,
                    AVG(completeness_score) AS avg_completeness_score,
                    AVG(duration_seconds) AS avg_duration
                FROM report_jobs
                WHERE status IN ('ready', 'error') AND updated_at >= ?
                """,
                (since_timestamp,),
            ).fetchone()

        completed_jobs = int(row["completed_jobs"] or 0)
        ready_jobs = int(row["ready_jobs"] or 0)
        error_jobs = int(row["error_jobs"] or 0)
        fallback_jobs = int(row["fallback_jobs"] or 0)
        accepted_jobs = int(row["accepted_jobs"] or 0)
        avg_duration = round(float(row["avg_duration"]), 2) if row["avg_duration"] is not None else None
        avg_completeness = round(float(row["avg_completeness_score"]), 1) if row["avg_completeness_score"] is not None else None
        success_rate = round((ready_jobs / completed_jobs) * 100, 1) if completed_jobs else None
        fallback_rate = round((fallback_jobs / completed_jobs) * 100, 1) if completed_jobs else None
        accepted_rate = round((accepted_jobs / completed_jobs) * 100, 1) if completed_jobs else None

        return {
            "completedJobs": completed_jobs,
            "readyJobs": ready_jobs,
            "errorJobs": error_jobs,
            "fallbackJobs": fallback_jobs,
            "acceptedJobs": accepted_jobs,
            "averageDurationSeconds": avg_duration,
            "averageCompletenessScore": avg_completeness,
            "successRatePct": success_rate,
            "fallbackRatePct": fallback_rate,
            "acceptedRatePct": accepted_rate,
        }


class ReportJobManager:
    def __init__(self, report_generator, max_workers, max_pending_jobs, retention_seconds, artifacts_dir, job_store, metrics_window_hours):
        self.report_generator = report_generator
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        self.max_workers = max_workers
        self.max_pending_jobs = max(max_pending_jobs, max_workers)
        self.retention_seconds = retention_seconds
        self.artifacts_dir = Path(artifacts_dir)
        self.job_store = job_store
        self.metrics_window_hours = metrics_window_hours
        self.lock = threading.Lock()
        self.job_store.recover_incomplete_jobs()
        self.job_store.cleanup_expired(self.retention_seconds)

    def _cleanup_locked(self):
        self.job_store.cleanup_expired(self.retention_seconds)

    def _serialize_job(self, job):
        queued_ahead = self.job_store.count_queued_ahead(job["job_id"], job["created_at"])
        return {
            "jobId": job["job_id"],
            "status": job["status"],
            "queuedAhead": queued_ahead,
            "durationSeconds": round(job["duration_seconds"], 2) if job["duration_seconds"] is not None else None,
            "error": job["error"],
            "fallbackUsed": job["fallback_used"],
            "osintAvailable": job["osint_available"],
            "visualsIncluded": job["visuals_included"],
            "qualityGatePassed": job["quality_gate_passed"],
            "completenessScore": job["completeness_score"],
        }

    def submit(self, notes, analysis_context="", analysis_payload=None):
        job_id = uuid.uuid4().hex
        preview_source = (notes or "").strip() or (analysis_context or "").strip()
        notes_preview = preview_source.replace("\n", " ")[:240]

        with self.lock:
            self._cleanup_locked()
            active_jobs = self.job_store.count_active_jobs()
            if active_jobs >= self.max_pending_jobs:
                raise QueueCapacityError(active_jobs, self.max_pending_jobs)
            self.job_store.create_job(job_id, notes_preview)

        self.executor.submit(self._run_job, job_id, notes, analysis_context, analysis_payload)
        return job_id

    def _run_job(self, job_id, notes, analysis_context="", analysis_payload=None):
        started_at = time.time()
        self.job_store.update_job(
            job_id,
            status="running",
            started_at=started_at,
            updated_at=started_at,
            error=None,
        )

        try:
            document, file_name, run_metadata = self.report_generator.run(
                notes,
                analysis_context,
                analysis_payload=analysis_payload,
            )
            artifact_path = self.artifacts_dir / f"{job_id}_{file_name}.docx"
            document.save(str(artifact_path))
        except Exception as exc:
            logger.exception("Background report generation failed: %s", exc)
            self.job_store.update_job(
                job_id,
                status="error",
                updated_at=time.time(),
                duration_seconds=time.time() - started_at,
                error="Document generation failed. Please verify the service configuration.",
            )
            return

        self.job_store.update_job(
            job_id,
            status="ready",
            updated_at=time.time(),
            duration_seconds=time.time() - started_at,
            filename=file_name,
            artifact_path=str(artifact_path),
            fallback_used=1 if run_metadata.get("fallback_used") else 0,
            osint_available=1 if run_metadata.get("osint_available") else 0,
            visuals_included=1 if run_metadata.get("visuals_included") else 0,
            quality_gate_passed=1 if run_metadata.get("quality_gate_passed") else 0,
            completeness_score=float(run_metadata.get("completeness_score") or 0),
            error=None,
        )

    def get_status(self, job_id):
        with self.lock:
            self._cleanup_locked()
            job = self.job_store.get_job(job_id)
        if not job:
            return None
        return self._serialize_job(job)

    def get_download(self, job_id):
        with self.lock:
            self._cleanup_locked()
            job = self.job_store.get_job(job_id)
        if not job:
            return None
        if job["status"] != "ready":
            return {"status": job["status"], "error": job["error"]}
        artifact_path = job.get("artifact_path")
        if not artifact_path or not Path(artifact_path).exists():
            return {"status": "error", "error": "Generated artifact is no longer available."}
        return {
            "filename": job["filename"],
            "artifactPath": artifact_path,
        }

    def get_health(self):
        with self.lock:
            self._cleanup_locked()
            status_counts = self.job_store.get_status_counts()
            metrics = self.job_store.get_recent_metrics(self.metrics_window_hours)

        active_jobs = status_counts["queuedJobs"] + status_counts["runningJobs"]
        return {
            **status_counts,
            "activeJobs": active_jobs,
            "maxConcurrentJobs": self.max_workers,
            "maxPendingJobs": self.max_pending_jobs,
            "acceptingJobs": active_jobs < self.max_pending_jobs,
            "metricsWindowHours": self.metrics_window_hours,
            "recentMetrics": metrics,
        }


class ForecastSnapshotCache:
    def __init__(self, ttl_seconds=300):
        self.ttl_seconds = max(int(ttl_seconds or 0), 0)
        self.lock = threading.Lock()
        self._items = {}

    def _purge_locked(self):
        if not self._items:
            return
        now = time.time()
        expired_keys = [
            key for key, item in self._items.items()
            if now - item["stored_at"] > self.ttl_seconds
        ]
        for key in expired_keys:
            self._items.pop(key, None)

    def get(self, key):
        if self.ttl_seconds <= 0:
            return None
        with self.lock:
            self._purge_locked()
            item = self._items.get(key)
            if not item:
                return None
            return copy.deepcopy(item["value"])

    def set(self, key, value):
        if self.ttl_seconds <= 0:
            return copy.deepcopy(value)
        with self.lock:
            self._purge_locked()
            self._items[key] = {
                "stored_at": time.time(),
                "value": copy.deepcopy(value),
            }
        return copy.deepcopy(value)

    def clear(self):
        with self.lock:
            self._items.clear()


class BackgroundRefreshCoordinator:
    def __init__(self, knowledge_base, cash_out_store, forecast_cache, interval_seconds):
        self.knowledge_base = knowledge_base
        self.cash_out_store = cash_out_store
        self.forecast_cache = forecast_cache
        self.interval_seconds = max(int(interval_seconds or 0), 0)
        self.thread = None
        self.stop_event = threading.Event()

    def refresh_all(self):
        knowledge_ok = self.knowledge_base.refresh_data()
        cash_out_ok = self.cash_out_store.refresh_data() if self.cash_out_store else False
        self.forecast_cache.clear()
        return {
            "knowledgeBase": knowledge_ok,
            "cashOutSource": cash_out_ok if self.cash_out_store and self.cash_out_store.client.is_configured() else None,
        }

    def start(self):
        if self.interval_seconds <= 0 or self.thread is not None:
            return

        def _runner():
            while not self.stop_event.wait(self.interval_seconds):
                try:
                    self.refresh_all()
                except Exception:
                    logger.exception("Periodic data refresh failed.")

        self.thread = threading.Thread(
            target=_runner,
            name="background-data-refresh",
            daemon=True,
        )
        self.thread.start()


def create_app():
    from config import (
        APP_SECRET_KEY,
        AUTH_MAX_ACTIVE_SESSIONS,
        AUTH_MAX_SESSIONS_PER_USER,
        AUTH_SESSION_ABSOLUTE_TIMEOUT_HOURS,
        AUTH_SESSION_IDLE_TIMEOUT_MINUTES,
        DATA_REFRESH_INTERVAL_SECONDS,
        DB_URI,
        FORECAST_CACHE_TTL_SECONDS,
        JOB_STATE_DB_PATH,
        PERMANENT_SESSION_LIFETIME,
        REPORT_ARTIFACTS_DIR,
        REPORT_JOB_RETENTION_SECONDS,
        REPORT_MAX_CONCURRENT_JOBS,
        REPORT_MAX_PENDING_JOBS,
        REPORT_MIN_COMPLETENESS_SCORE,
        REPORT_METRICS_WINDOW_HOURS,
        REPORT_STATUS_POLL_INTERVAL_MS,
        SESSION_COOKIE_SECURE,
        SMART_SUGGESTIONS,
    )
    from core import CashOutStore, KnowledgeBase, ReportGenerator, Researcher
    from data_sources import summarize_source_profile
    from forecast_engine import CashflowForecaster, parse_idr_amount

    app = Flask(__name__)
    app.secret_key = APP_SECRET_KEY
    app.config["SESSION_COOKIE_HTTPONLY"] = True
    app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
    app.config["SESSION_COOKIE_SECURE"] = SESSION_COOKIE_SECURE
    app.config["PERMANENT_SESSION_LIFETIME"] = PERMANENT_SESSION_LIFETIME
    CORS(app)

    knowledge_base = KnowledgeBase(DB_URI)
    cash_out_store = CashOutStore()
    report_generator = ReportGenerator(knowledge_base)
    job_store = ReportJobStore(JOB_STATE_DB_PATH, REPORT_ARTIFACTS_DIR)
    user_store = UserStore(JOB_STATE_DB_PATH)
    session_store = ActiveSessionStore(JOB_STATE_DB_PATH)
    forecast_cache = ForecastSnapshotCache(FORECAST_CACHE_TTL_SECONDS)
    job_manager = ReportJobManager(
        report_generator=report_generator,
        max_workers=REPORT_MAX_CONCURRENT_JOBS,
        max_pending_jobs=REPORT_MAX_PENDING_JOBS,
        retention_seconds=REPORT_JOB_RETENTION_SECONDS,
        artifacts_dir=REPORT_ARTIFACTS_DIR,
        job_store=job_store,
        metrics_window_hours=REPORT_METRICS_WINDOW_HOURS,
    )
    
    # Initialize forecaster
    forecaster = CashflowForecaster(monthly_operating_cost_idr=200_000_000)
    refresh_coordinator = BackgroundRefreshCoordinator(
        knowledge_base=knowledge_base,
        cash_out_store=cash_out_store,
        forecast_cache=forecast_cache,
        interval_seconds=DATA_REFRESH_INTERVAL_SECONDS,
    )
    refresh_coordinator.start()

    # Boot-time endpoint validation (non-blocking, logs warnings)
    if knowledge_base.internal_api_client and knowledge_base.internal_api_client.is_configured():
        ok, msg = knowledge_base.internal_api_client.validate_endpoint_url()
        if ok:
            logger.info("Boot check: %s", msg)
        else:
            logger.warning("Boot check: %s", msg)
    if cash_out_store.client.is_configured():
        ok, msg = cash_out_store.client.validate_endpoint_url()
        if ok:
            logger.info("Boot check (cash-out): %s", msg)
        else:
            logger.warning("Boot check (cash-out): %s", msg)

    app.config["knowledge_base"] = knowledge_base
    app.config["cash_out_store"] = cash_out_store
    app.config["job_manager"] = job_manager
    app.config["forecaster"] = forecaster
    app.config["user_store"] = user_store
    app.config["session_store"] = session_store
    app.config["min_completeness_score"] = REPORT_MIN_COMPLETENESS_SCORE
    app.config["status_poll_interval_ms"] = REPORT_STATUS_POLL_INTERVAL_MS
    app.config["forecast_cache"] = forecast_cache
    app.config["data_refresh_interval_seconds"] = DATA_REFRESH_INTERVAL_SECONDS
    app.config["refresh_coordinator"] = refresh_coordinator
    app.config["auth_max_active_sessions"] = max(int(AUTH_MAX_ACTIVE_SESSIONS), 1)
    app.config["auth_max_sessions_per_user"] = max(int(AUTH_MAX_SESSIONS_PER_USER), 1)
    app.config["auth_session_idle_timeout_seconds"] = max(int(AUTH_SESSION_IDLE_TIMEOUT_MINUTES), 1) * 60
    app.config["auth_session_absolute_timeout_seconds"] = max(int(AUTH_SESSION_ABSOLUTE_TIMEOUT_HOURS), 1) * 3600

    def _start_authenticated_session(username):
        session_id = session_store.create_session(
            username=username,
            ip_address=request.headers.get("X-Forwarded-For", request.remote_addr or ""),
            user_agent=request.headers.get("User-Agent", ""),
            idle_timeout_seconds=app.config["auth_session_idle_timeout_seconds"],
            absolute_timeout_seconds=app.config["auth_session_absolute_timeout_seconds"],
            max_global_sessions=app.config["auth_max_active_sessions"],
            max_sessions_per_user=app.config["auth_max_sessions_per_user"],
        )
        session.clear()
        session.permanent = True
        session["username"] = username
        session["auth_session_id"] = session_id

    def _invalidate_authenticated_session(reason):
        session_id = session.get("auth_session_id")
        if session_id:
            session_store.revoke_session(session_id, reason=reason)
        session.clear()

    def _is_authenticated():
        username = str(session.get("username") or "").strip()
        session_id = str(session.get("auth_session_id") or "").strip()
        if not username or not session_id:
            return False
        is_valid, reason = session_store.validate_and_touch(
            session_id=session_id,
            username=username,
            idle_timeout_seconds=app.config["auth_session_idle_timeout_seconds"],
            absolute_timeout_seconds=app.config["auth_session_absolute_timeout_seconds"],
        )
        if not is_valid:
            logger.info("Auth session rejected for user=%s reason=%s", username, reason)
            session.clear()
            return False
        g.current_username = username
        return True

    def _is_api_request():
        return request.path.startswith("/api/") or request.path.startswith("/jobs/") or request.path in {
            "/get-config",
            "/generate",
            "/refresh-knowledge",
        }

    def _attach_no_store_headers(response):
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        return response

    @app.before_request
    def require_authentication():
        allowed_endpoints = {
            "static",
            "login",
            "signup",
            "logout",
            "health",
        }
        if request.endpoint in allowed_endpoints:
            return None

        if _is_authenticated():
            return None

        if _is_api_request():
            return jsonify({"error": "Autentikasi diperlukan.", "loginUrl": url_for("login")}), 401
        return redirect(url_for("login"))

    @app.after_request
    def apply_security_headers(response):
        if request.endpoint == "static" or request.endpoint == "health":
            return response
        return _attach_no_store_headers(response)

    def _render_auth(mode="login", error=None, username=""):
        return render_template(
            "auth.html",
            mode=mode,
            error=error,
            username=username,
            has_users=user_store.has_users(),
        )

    @app.route("/login", methods=["GET", "POST"])
    def login():
        if _is_authenticated():
            return redirect(url_for("home"))

        if request.method == "GET":
            return _render_auth(mode="login")

        username = str(request.form.get("username", "")).strip()
        password = request.form.get("password", "")
        authenticated_username = user_store.authenticate(username, password)
        if not authenticated_username:
            return _render_auth(
                mode="login",
                error="Nama pengguna atau kata sandi salah.",
                username=username,
            ), 401

        try:
            _start_authenticated_session(authenticated_username)
        except SessionLimitError as exc:
            return _render_auth(mode="login", error=str(exc), username=username), 429
        return redirect(url_for("home"))

    @app.route("/signup", methods=["GET", "POST"])
    def signup():
        if _is_authenticated():
            return redirect(url_for("home"))

        if request.method == "GET":
            return _render_auth(mode="signup")

        username = str(request.form.get("username", "")).strip()
        password = request.form.get("password", "")
        confirm_password = request.form.get("confirm_password", "")
        if password != confirm_password:
            return _render_auth(
                mode="signup",
                error="Konfirmasi kata sandi tidak cocok.",
                username=username,
            ), 400

        try:
            created_username = user_store.create_user(username, password)
        except ValueError as exc:
            return _render_auth(mode="signup", error=str(exc), username=username), 400

        try:
            _start_authenticated_session(created_username)
        except SessionLimitError as exc:
            return _render_auth(mode="signup", error=str(exc), username=username), 429
        return redirect(url_for("home"))

    @app.route("/logout", methods=["POST"])
    def logout():
        _invalidate_authenticated_session(reason="logout")
        response = redirect(url_for("login"))
        response.delete_cookie(
            app.config.get("SESSION_COOKIE_NAME", "session"),
            path=app.config.get("SESSION_COOKIE_PATH", "/"),
            domain=app.config.get("SESSION_COOKIE_DOMAIN"),
            secure=app.config.get("SESSION_COOKIE_SECURE", False),
            samesite=app.config.get("SESSION_COOKIE_SAMESITE", "Lax"),
            httponly=app.config.get("SESSION_COOKIE_HTTPONLY", True),
        )
        return response

    def _build_forecast_periods(month_count=3):
        base_date = datetime.now().replace(day=1)
        periods = []
        windows = [
            (1, 10, "1-10"),
            (11, 20, "11-20"),
            (21, None, "21-akhir bulan"),
        ]

        for offset in range(month_count):
            year = base_date.year + ((base_date.month - 1 + offset) // 12)
            month = ((base_date.month - 1 + offset) % 12) + 1
            first_day = datetime(year, month, 1)
            last_day = calendar.monthrange(year, month)[1]

            for start_day, end_day, label in windows:
                start = first_day.replace(day=start_day)
                resolved_end_day = last_day if end_day is None else min(end_day, last_day)
                end = first_day.replace(day=resolved_end_day)
                periods.append(
                    {
                        "id": f"{year}-{month:02d}_{label.replace(' ', '_')}",
                        "label": f"{label} {first_day.strftime('%B %Y')}",
                        "start": start.isoformat(),
                        "end": end.isoformat(),
                    }
                )

        return periods

    def _build_external_context(start_date, end_date):
        dataset = knowledge_base.df
        partner_types = []
        services = []
        if dataset is not None and not dataset.empty:
            partner_column = next(
                (column for column in dataset.columns if str(column).strip().lower() in {"tipe partner", "partner type", "partner_type"}),
                None,
            )
            service_column = next(
                (column for column in dataset.columns if str(column).strip().lower() in {"layanan", "service", "service_name"}),
                None,
            )
            if partner_column:
                partner_types = (
                    dataset[partner_column]
                    .dropna()
                    .astype(str)
                    .value_counts()
                    .head(3)
                    .index
                    .tolist()
                )
            if service_column:
                services = (
                    dataset[service_column]
                    .dropna()
                    .astype(str)
                    .value_counts()
                    .head(3)
                    .index
                    .tolist()
                )
        partner_snippet = ", ".join(partner_types)
        service_snippet = ", ".join(services)
        return (
            f"periode {start_date.strftime('%d %B %Y')} sampai {end_date.strftime('%d %B %Y')} "
            f"partner {partner_snippet} "
            f"layanan {service_snippet}"
        ).strip()

    def _parse_request_idr_amount(raw_value, field_name, default_value):
        value = default_value if raw_value is None else raw_value
        try:
            return parse_idr_amount(value)
        except ValueError as exc:
            raise ValueError(
                f"{field_name} must be provided in Rupiah (IDR) only. Foreign currencies are not supported."
            ) from exc

    def _validate_currency_code(payload):
        currency = str(payload.get("currency", "IDR")).strip().upper()
        if currency not in {"IDR", "RP", "RUPIAH"}:
            raise ValueError("This app only accepts Rupiah (IDR) amounts.")
        return "IDR"

    def _build_sync_snapshot():
        refresh_interval = current_app.config["data_refresh_interval_seconds"]
        knowledge_status = current_app.config["knowledge_base"].get_sync_status(refresh_interval)
        cash_out_status = current_app.config["cash_out_store"].get_status(refresh_interval)
        return {
            "financialData": knowledge_status,
            "cashOutSource": cash_out_status,
            "refreshIntervalSeconds": refresh_interval,
        }

    def _get_cash_out_records():
        return current_app.config["cash_out_store"].get_records()

    def _build_forecast_cache_key(kind, cash_on_hand, monthly_cost, start_date, end_date=None):
        knowledge_state = current_app.config["knowledge_base"].get_sync_status()
        cash_out_state = current_app.config["cash_out_store"].get_status()
        return (
            kind,
            knowledge_state["dataVersion"],
            cash_out_state["version"],
            int(cash_on_hand),
            int(monthly_cost),
            start_date.isoformat(),
            end_date.isoformat() if end_date else None,
        )

    def _get_or_build_single_forecast(cash_on_hand, monthly_cost, start_date, end_date):
        cache_key = _build_forecast_cache_key("single_forecast", cash_on_hand, monthly_cost, start_date, end_date)
        cached_value = current_app.config["forecast_cache"].get(cache_key)
        if cached_value is not None:
            return cached_value

        forecaster = CashflowForecaster(monthly_operating_cost_idr=monthly_cost)
        forecast = forecaster.forecast(
            df=current_app.config["knowledge_base"].df,
            cash_on_hand=cash_on_hand,
            start_date=start_date,
            end_date=end_date,
            cash_out_records=_get_cash_out_records(),
        )
        return current_app.config["forecast_cache"].set(cache_key, forecast)

    def _get_or_build_horizon_forecasts(cash_on_hand, monthly_cost, start_date):
        cache_key = _build_forecast_cache_key("horizon_forecast", cash_on_hand, monthly_cost, start_date)
        cached_value = current_app.config["forecast_cache"].get(cache_key)
        if cached_value is not None:
            return cached_value

        forecaster = CashflowForecaster(monthly_operating_cost_idr=monthly_cost)
        forecasts = forecaster.forecast_by_horizon(
            df=current_app.config["knowledge_base"].df,
            cash_on_hand=cash_on_hand,
            start_date=start_date,
            cash_out_records=_get_cash_out_records(),
        )
        return current_app.config["forecast_cache"].set(cache_key, forecasts)

    def _build_payment_class_trend():
        dataset = current_app.config["knowledge_base"].df
        if dataset is None or dataset.empty:
            return {"series": [], "topPeriods": []}

        resolved_columns = current_app.config["knowledge_base"].data_contract_summary.get("sourceColumns", {})
        period_column = resolved_columns.get("period")
        payment_class_column = resolved_columns.get("payment_class")
        invoice_value_column = resolved_columns.get("invoice_value")
        if not period_column or not payment_class_column or not invoice_value_column:
            return {"series": [], "topPeriods": []}

        working_df = dataset[[period_column, payment_class_column, invoice_value_column]].copy()
        working_df.columns = ["period", "payment_class", "invoice_value"]
        working_df["payment_class"] = working_df["payment_class"].astype(str).str.extract(r"(Kelas [A-E])", expand=False).fillna("Tidak Diketahui")
        working_df["invoice_value"] = working_df["invoice_value"].apply(
            lambda value: parse_idr_amount(value) if value is not None and str(value).strip() else 0
        )
        working_df["period"] = working_df["period"].astype(str).fillna("Tidak Diketahui")

        grouped = (
            working_df.groupby(["period", "payment_class"], as_index=False)
            .agg(amount=("invoice_value", "sum"), invoice_count=("invoice_value", "size"))
        )
        period_totals = (
            grouped.groupby("period", as_index=False)
            .agg(total_amount=("amount", "sum"))
            .sort_values("total_amount", ascending=False)
        )
        return {
            "series": grouped.to_dict(orient="records"),
            "topPeriods": period_totals.head(10).to_dict(orient="records"),
        }

    def _build_concentration_view(invoices):
        if not invoices:
            return {"partners": [], "services": []}

        partner_totals = {}
        service_totals = {}
        total_amount = sum(invoice["amount"] for invoice in invoices) or 1

        for invoice in invoices:
            partner = invoice["partner_type"] or "Tidak Diketahui"
            service = invoice["service"] or "Tidak Diketahui"
            partner_totals[partner] = partner_totals.get(partner, 0) + invoice["amount"]
            service_totals[service] = service_totals.get(service, 0) + invoice["amount"]

        def _rank_items(source_map):
            return [
                {
                    "label": label,
                    "amount": amount,
                    "sharePct": round((amount / total_amount) * 100, 1),
                }
                for label, amount in sorted(source_map.items(), key=lambda item: item[1], reverse=True)[:10]
            ]

        return {
            "partners": _rank_items(partner_totals),
            "services": _rank_items(service_totals),
        }

    @app.route("/")
    def home():
        return render_template("index.html", current_username=session.get("username", ""))

    @app.route("/get-config")
    def get_config():
        active_knowledge_base = current_app.config["knowledge_base"]
        if active_knowledge_base.df is None or active_knowledge_base.df.empty:
            return jsonify({"error": "Financial data is currently unavailable.", "syncStatus": _build_sync_snapshot()})
        review_context = active_knowledge_base.get_review_context()

        return jsonify(
            {
                "suggestions": SMART_SUGGESTIONS,
                "statusPollIntervalMs": current_app.config["status_poll_interval_ms"],
                "reviewContext": review_context,
                "syncStatus": _build_sync_snapshot(),
                "dataSourceContract": active_knowledge_base.get_internal_data_contract(),
                "authSecurity": session_store.get_security_snapshot(
                    idle_timeout_seconds=app.config["auth_session_idle_timeout_seconds"],
                    absolute_timeout_seconds=app.config["auth_session_absolute_timeout_seconds"],
                    max_global_sessions=app.config["auth_max_active_sessions"],
                    max_sessions_per_user=app.config["auth_max_sessions_per_user"],
                ),
            }
        )

    @app.route("/generate", methods=["POST"])
    def generate_doc():
        payload = request.get_json(silent=True) or {}
        notes = payload.get("notes", "")
        analysis_context = (payload.get("analysis_context") or "").strip()
        analysis_payload = payload.get("analysis_payload") if isinstance(payload.get("analysis_payload"), dict) else None
        active_job_manager = current_app.config["job_manager"]
        try:
            job_id = active_job_manager.submit(notes, analysis_context, analysis_payload=analysis_payload)
        except QueueCapacityError as exc:
            return (
                jsonify(
                    {
                        "error": str(exc),
                        "activeJobs": exc.active_jobs,
                        "maxPendingJobs": exc.max_pending_jobs,
                    }
                ),
                429,
            )
        return jsonify({"jobId": job_id}), 202

    @app.route("/jobs/<job_id>")
    def get_job_status(job_id):
        active_job_manager = current_app.config["job_manager"]
        status = active_job_manager.get_status(job_id)
        if status is None:
            return jsonify({"error": "Job not found."}), 404
        return jsonify(status)

    @app.route("/jobs/<job_id>/download")
    def download_job(job_id):
        active_job_manager = current_app.config["job_manager"]
        download_payload = active_job_manager.get_download(job_id)
        if download_payload is None:
            return jsonify({"error": "Job not found."}), 404
        if "artifactPath" not in download_payload:
            return jsonify(download_payload), 409
        return send_file(
            download_payload["artifactPath"],
            as_attachment=True,
            download_name=f"{download_payload['filename']}.docx",
            mimetype="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        )

    @app.route("/refresh-knowledge", methods=["POST"])
    def refresh_knowledge():
        refresh_result = current_app.config["refresh_coordinator"].refresh_all()
        return jsonify(
            {
                "status": "success" if refresh_result["knowledgeBase"] else "error",
                "refreshResult": refresh_result,
                "syncStatus": _build_sync_snapshot(),
            }
        )

    @app.route("/health")
    def health():
        health_snapshot = current_app.config["job_manager"].get_health()
        internal_data_contract = current_app.config["knowledge_base"].get_internal_data_contract()
        health_snapshot["dataReady"] = bool(
            current_app.config["knowledge_base"].df is not None
            and not current_app.config["knowledge_base"].df.empty
        )
        health_snapshot["internalDataContractReady"] = bool(
            internal_data_contract.get("currentSummary", {}).get("isReady")
        )
        health_snapshot["minimumCompletenessScore"] = current_app.config["min_completeness_score"]
        health_snapshot["authSecurity"] = session_store.get_security_snapshot(
            idle_timeout_seconds=app.config["auth_session_idle_timeout_seconds"],
            absolute_timeout_seconds=app.config["auth_session_absolute_timeout_seconds"],
            max_global_sessions=app.config["auth_max_active_sessions"],
            max_sessions_per_user=app.config["auth_max_sessions_per_user"],
        )
        health_snapshot["syncStatus"] = _build_sync_snapshot()
        return jsonify(health_snapshot)

    @app.route("/api/internal-data/contract", methods=["GET"])
    def get_internal_data_contract():
        return jsonify(current_app.config["knowledge_base"].get_internal_data_contract())

    @app.route("/api/data-source/validate", methods=["POST"])
    def validate_data_source():
        payload = request.get_json(silent=True) or {}
        source_key = str(payload.get("sourceKey") or "").strip().lower()
        preview_mode = bool(payload.get("preview"))
        preview_rows = int(payload.get("previewRows") or 5)
        if not source_key:
            return jsonify({"error": "sourceKey wajib diisi."}), 400

        if preview_mode:
            # Dry-run: fetch limited records to verify mapping without loading full dataset
            try:
                active_kb = current_app.config["knowledge_base"]
                active_kb._reload_source_registry()
                profile = active_kb.source_registry.get(source_key)
                if not profile:
                    return jsonify({"error": f"Sumber data `{source_key}` tidak tersedia."}), 404

                from core import InternalAPIClient
                from data_contract import build_internal_data_summary, normalize_financial_dataframe
                if profile.get("type") == "json_api":
                    client = InternalAPIClient(source_profile=profile)
                    records, extraction_summary = client.fetch_records(preview_limit=preview_rows)
                    raw_df = active_kb._normalize_records(records)
                    if raw_df.empty:
                        return jsonify({
                            "preview": True,
                            "ready": False,
                            "message": "Preview fetch returned no records.",
                            "recordCount": 0,
                            "syncStatus": _build_sync_snapshot(),
                        })
                    _, data_summary = normalize_financial_dataframe(
                        raw_df, explicit_field_map=client.field_map,
                    )
                    sample_records = raw_df.head(preview_rows).to_dict(orient="records")
                    return jsonify({
                        "preview": True,
                        "ready": bool(data_summary.get("isReady")),
                        "message": "Preview berhasil." if data_summary.get("isReady") else "Field wajib belum lengkap.",
                        "recordCount": len(records),
                        "previewRows": len(sample_records),
                        "sampleRecords": sample_records,
                        "contractSummary": data_summary,
                        "extractionSummary": extraction_summary,
                        "syncStatus": _build_sync_snapshot(),
                    })
                else:
                    return jsonify({
                        "preview": True,
                        "message": "Preview hanya tersedia untuk sumber tipe json_api.",
                        "syncStatus": _build_sync_snapshot(),
                    })
            except Exception as exc:
                return jsonify({
                    "preview": True,
                    "ready": False,
                    "message": str(exc),
                    "syncStatus": _build_sync_snapshot(),
                }), 400

        try:
            validation = current_app.config["knowledge_base"].validate_source(source_key)
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 404
        return jsonify(
            {
                **validation,
                "syncStatus": _build_sync_snapshot(),
            }
        )

    @app.route("/api/data-source/activate", methods=["POST"])
    def activate_data_source():
        payload = request.get_json(silent=True) or {}
        source_key = str(payload.get("sourceKey") or "").strip().lower()
        if not source_key:
            return jsonify({"error": "sourceKey wajib diisi."}), 400

        activation = current_app.config["knowledge_base"].activate_source(source_key)
        current_app.config["forecast_cache"].clear()
        current_app.config["cash_out_store"].refresh_data()
        response_payload = {
            **activation,
            "syncStatus": _build_sync_snapshot(),
            "reviewContext": current_app.config["knowledge_base"].get_review_context()
            if activation.get("activated")
            else None,
        }
        if not activation.get("activated"):
            return jsonify(response_payload), 409
        return jsonify(response_payload)

    @app.route("/api/data-source/reload-profiles", methods=["POST"])
    def reload_data_source_profiles():
        """Reload source profiles from disk/env without restarting the app."""
        try:
            active_kb = current_app.config["knowledge_base"]
            active_kb._reload_source_registry()
            return jsonify({
                "reloaded": True,
                "activeSourceKey": active_kb.active_source_key,
                "availableSources": [
                    summarize_source_profile(profile)
                    for _, profile in sorted(active_kb.source_registry.items())
                ],
                "registryIssues": list(active_kb.source_registry_issues),
                "syncStatus": _build_sync_snapshot(),
            })
        except Exception as exc:
            return jsonify({"reloaded": False, "error": str(exc)}), 500

    @app.route("/api/data-source/check-connectivity", methods=["POST"])
    def check_data_source_connectivity():
        """Check if the configured API endpoint is reachable."""
        payload = request.get_json(silent=True) or {}
        source_key = str(payload.get("sourceKey") or "").strip().lower()

        active_kb = current_app.config["knowledge_base"]
        active_kb._reload_source_registry()
        profile = active_kb.source_registry.get(source_key)
        if not profile:
            return jsonify({"error": f"Sumber data `{source_key}` tidak tersedia."}), 404

        if profile.get("type") != "json_api":
            return jsonify({"reachable": True, "message": "Sumber CSV lokal tidak memerlukan koneksi jaringan."})

        from core import InternalAPIClient
        client = InternalAPIClient(source_profile=profile)
        ok, message = client.validate_endpoint_url()
        return jsonify({"reachable": ok, "message": message})

    # ==================== CASHFLOW FORECAST ENDPOINTS ====================
    
    @app.route("/api/forecast/periods", methods=["GET"])
    def get_forecast_periods():
        """Get available date range periods for forecasting"""
        return jsonify({"periods": _build_forecast_periods()})
    
    @app.route("/api/forecast", methods=["POST"])
    def generate_forecast():
        """
        Generate cashflow forecast
        Request body:
        {
            "period_id": "2026-03_week_1",
            "cash_on_hand": 500000000,
            "monthly_operating_cost": 200000000
        }
        """
        payload = request.get_json(silent=True) or {}
        
        # Get data
        knowledge_base = current_app.config["knowledge_base"]
        if knowledge_base.df is None or knowledge_base.df.empty:
            return jsonify({"error": "Financial data not available"}), 400
        
        # Parse inputs
        try:
            currency_code = _validate_currency_code(payload)
            cash_on_hand = _parse_request_idr_amount(payload.get("cash_on_hand"), "cash_on_hand", 500_000_000)
            monthly_cost = _parse_request_idr_amount(payload.get("monthly_operating_cost"), "monthly_operating_cost", 200_000_000)
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        
        # Parse period dates
        try:
            start_iso = payload.get("start_date")
            end_iso = payload.get("end_date")
            
            if not start_iso or not end_iso:
                return jsonify({"error": "start_date and end_date required"}), 400
            
            start_date = datetime.fromisoformat(start_iso)
            end_date = datetime.fromisoformat(end_iso)
        except (ValueError, TypeError):
            return jsonify({"error": "Invalid date format (use ISO format)"}), 400
        
        # Generate forecast
        try:
            forecast = _get_or_build_single_forecast(
                cash_on_hand=cash_on_hand,
                monthly_cost=monthly_cost,
                start_date=start_date,
                end_date=end_date,
            )
            forecast["currency"] = currency_code
            forecast["external_factors"] = Researcher.get_payment_delay_risks(
                _build_external_context(start_date, end_date)
            )
            forecast["sync_status"] = _build_sync_snapshot()
            return jsonify(forecast)
        except Exception as e:
            logger.error(f"Forecast error: {e}", exc_info=True)
            return jsonify({"error": str(e)}), 500
    
    @app.route("/api/forecast/by-horizon", methods=["POST"])
    def generate_forecast_by_horizon():
        """
        Generate cashflow forecasts for all time horizons (0-30d, 1-3m, 3-12m)
        Request body:
        {
            "cash_on_hand": 500000000,
            "monthly_operating_cost": 200000000,
            "start_date": "2026-03-31"
        }
        """
        payload = request.get_json(silent=True) or {}
        
        # Get data
        knowledge_base = current_app.config["knowledge_base"]
        if knowledge_base.df is None or knowledge_base.df.empty:
            return jsonify({"error": "Financial data not available"}), 400
        
        # Parse inputs
        try:
            currency_code = _validate_currency_code(payload)
            cash_on_hand = _parse_request_idr_amount(payload.get("cash_on_hand"), "cash_on_hand", 500_000_000)
            monthly_cost = _parse_request_idr_amount(payload.get("monthly_operating_cost"), "monthly_operating_cost", 200_000_000)
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        start_date_iso = payload.get("start_date")
        
        if not start_date_iso:
            start_date = datetime.now()
        else:
            try:
                start_date = datetime.fromisoformat(start_date_iso)
            except (ValueError, TypeError):
                return jsonify({"error": "Invalid start_date format (use ISO format)"}), 400
        
        # Generate forecasts for all horizons
        try:
            forecasts = _get_or_build_horizon_forecasts(
                cash_on_hand=cash_on_hand,
                monthly_cost=monthly_cost,
                start_date=start_date,
            )
            horizon_end = start_date + timedelta(days=365)
            return jsonify({
                'start_date': start_date.isoformat(),
                'cash_on_hand': cash_on_hand,
                'currency': currency_code,
                'forecasts': forecasts,
                'time_horizons': CashflowForecaster.TIME_HORIZONS,
                'external_factors': Researcher.get_payment_delay_risks(
                    _build_external_context(start_date, horizon_end)
                ),
                'sync_status': _build_sync_snapshot(),
            })
        except Exception as e:
            logger.error(f"Multi-horizon forecast error: {e}", exc_info=True)
            return jsonify({"error": str(e)}), 500
    
    @app.route("/api/forecast/outstanding", methods=["GET"])
    def get_outstanding():
        """Get outstanding invoices analysis"""
        knowledge_base = current_app.config["knowledge_base"]
        if knowledge_base.df is None or knowledge_base.df.empty:
            return jsonify({"error": "Financial data not available"}), 400
        
        try:
            forecaster = current_app.config["forecaster"]
            invoices = forecaster._parse_invoices(
                knowledge_base.df,
                start_date=datetime.now(),
                end_date=datetime.now(),
            )
            result = forecaster._analyze_outstanding(invoices)
            result["invoice_count"] = len(invoices)
            result["sync_status"] = _build_sync_snapshot()
            return jsonify(result)
        except Exception as e:
            logger.error(f"Outstanding analysis error: {e}", exc_info=True)
            return jsonify({"error": str(e)}), 500

    @app.route("/api/forecast/drilldown/top-overdue", methods=["POST"])
    def get_top_overdue_drilldown():
        payload = request.get_json(silent=True) or {}
        try:
            _validate_currency_code(payload)
            cash_on_hand = _parse_request_idr_amount(payload.get("cash_on_hand"), "cash_on_hand", 500_000_000)
            monthly_cost = _parse_request_idr_amount(payload.get("monthly_operating_cost"), "monthly_operating_cost", 200_000_000)
            start_date = datetime.fromisoformat(payload.get("start_date")) if payload.get("start_date") else datetime.now()
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        except TypeError:
            return jsonify({"error": "Invalid start_date format (use ISO format)"}), 400
        mode = str(payload.get("horizon", "short_term")).strip() or "short_term"
        forecasts = _get_or_build_horizon_forecasts(cash_on_hand, monthly_cost, start_date)
        active_forecast = forecasts.get(mode) or forecasts.get("short_term")
        dashboard_snapshot = active_forecast.get("dashboard_snapshot", {}) if active_forecast else {}
        return jsonify(
            {
                "horizon": mode,
                "items": dashboard_snapshot.get("top_overdue_accounts", []),
                "alertLines": dashboard_snapshot.get("alert_recommendation_lines", []),
                "sync_status": _build_sync_snapshot(),
            }
        )

    @app.route("/api/forecast/drilldown/payment-class-trend", methods=["GET"])
    def get_payment_class_trend_drilldown():
        return jsonify(
            {
                **_build_payment_class_trend(),
                "sync_status": _build_sync_snapshot(),
            }
        )

    @app.route("/api/forecast/drilldown/concentration", methods=["POST"])
    def get_concentration_drilldown():
        payload = request.get_json(silent=True) or {}
        try:
            _validate_currency_code(payload)
            cash_on_hand = _parse_request_idr_amount(payload.get("cash_on_hand"), "cash_on_hand", 500_000_000)
            monthly_cost = _parse_request_idr_amount(payload.get("monthly_operating_cost"), "monthly_operating_cost", 200_000_000)
            start_date = datetime.fromisoformat(payload.get("start_date")) if payload.get("start_date") else datetime.now()
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        except TypeError:
            return jsonify({"error": "Invalid start_date format (use ISO format)"}), 400
        mode = str(payload.get("horizon", "short_term")).strip() or "short_term"
        active_forecast = (_get_or_build_horizon_forecasts(cash_on_hand, monthly_cost, start_date).get(mode)) or {}
        forecaster = current_app.config["forecaster"]
        invoices = forecaster._parse_invoices(
            current_app.config["knowledge_base"].df,
            start_date=start_date,
            end_date=start_date,
        )
        return jsonify(
            {
                "horizon": mode,
                "riskSummary": (active_forecast.get("dashboard_snapshot", {}) or {}).get("risk_summary", {}),
                "concentration": _build_concentration_view(invoices),
                "sync_status": _build_sync_snapshot(),
            }
        )

    return app


def parse_args():
    parser = argparse.ArgumentParser(description="Run the financial reporting app.")
    parser.add_argument(
        "--data-mode",
        choices=("demo", "internal_api"),
        help="Select the internal data acquisition mode for this process.",
    )
    parser.add_argument(
        "--internal-api-base-url",
        help="Optional override for the internal API base URL.",
    )
    parser.add_argument(
        "--internal-api-url",
        help="Optional override for the full internal API endpoint URL.",
    )
    parser.add_argument(
        "--internal-api-method",
        help="Optional override for the internal API HTTP method, for example POST.",
    )
    parser.add_argument(
        "--host",
        help="Bind host for shared access, for example 0.0.0.0.",
    )
    parser.add_argument(
        "--port",
        type=int,
        help="Bind port for the web app.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Run Flask in debug mode.",
    )
    parser.add_argument(
        "--server",
        choices=("flask", "waitress"),
        help="Select the web server runtime.",
    )
    return parser.parse_args()


def apply_runtime_overrides(args):
    if args.data_mode:
        os.environ["DATA_ACQUISITION_MODE"] = args.data_mode
    if args.internal_api_base_url:
        os.environ["INTERNAL_API_BASE_URL"] = args.internal_api_base_url
        os.environ.setdefault("DATA_ACQUISITION_MODE", "internal_api")
    if args.internal_api_url:
        os.environ["INTERNAL_API_ENDPOINT_URL"] = args.internal_api_url
        os.environ.setdefault("DATA_ACQUISITION_MODE", "internal_api")
    if args.internal_api_method:
        os.environ["INTERNAL_API_METHOD"] = args.internal_api_method.upper()
    if args.host:
        os.environ["APP_HOST"] = args.host
    if args.port:
        os.environ["APP_PORT"] = str(args.port)
    if args.debug:
        os.environ["APP_DEBUG"] = "true"
    if args.server:
        os.environ["APP_SERVER"] = args.server


def run_app(app_instance):
    app_server = os.getenv("APP_SERVER", "flask").strip().lower()
    app_host = os.getenv("APP_HOST", "127.0.0.1").strip()
    app_port = int(os.getenv("APP_PORT", "5000"))
    app_debug = os.getenv("APP_DEBUG", "false").strip().lower() in {"1", "true", "yes"}
    waitress_threads = int(os.getenv("WAITRESS_THREADS", "12"))
    waitress_connection_limit = int(os.getenv("WAITRESS_CONNECTION_LIMIT", "100"))
    waitress_channel_timeout = int(os.getenv("WAITRESS_CHANNEL_TIMEOUT", "120"))

    if app_server == "waitress":
        try:
            from waitress import serve
        except ImportError as exc:
            raise RuntimeError(
                "Waitress is not installed. Run `pip install -r requirements.txt` first."
            ) from exc

        logger.info(
            "Starting Waitress on %s:%s with %s threads.",
            app_host,
            app_port,
            waitress_threads,
        )
        serve(
            app_instance,
            host=app_host,
            port=app_port,
            threads=waitress_threads,
            connection_limit=waitress_connection_limit,
            channel_timeout=waitress_channel_timeout,
        )
        return

    logger.info(
        "Starting Flask development server on %s:%s.",
        app_host,
        app_port,
    )
    app_instance.run(host=app_host, port=app_port, debug=app_debug, threaded=True)


app = create_app() if __name__ != "__main__" else None


if __name__ == "__main__":
    runtime_args = parse_args()
    apply_runtime_overrides(runtime_args)
    app = create_app()
    run_app(app)
