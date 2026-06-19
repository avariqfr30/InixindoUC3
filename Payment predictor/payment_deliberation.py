"""Deterministic document-level deliberation for payment reports."""
from __future__ import annotations

import copy
import hashlib
import json
import re
from typing import Any


def _clean(value: Any, max_words: int = 36) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip(" -;,.:")
    words = text.split()
    if len(words) > max_words:
        text = " ".join(words[:max_words]).rstrip(" ,;:") + "."
    return text


def _digest(value: Any) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


class PaymentDeliberationBuilder:
    CACHE_VERSION = "payment-deliberation-v1"
    _cache: dict[str, dict[str, Any]] = {}
    _stats = {"hits": 0, "misses": 0}

    @classmethod
    def clear_cache(cls) -> None:
        cls._cache.clear()
        cls._stats = {"hits": 0, "misses": 0}

    @classmethod
    def cache_stats(cls) -> dict[str, int]:
        return {**cls._stats, "items": len(cls._cache)}

    @classmethod
    def _remember(cls, key: str, value: dict[str, Any]) -> dict[str, Any]:
        cls._cache[key] = copy.deepcopy(value)
        while len(cls._cache) > 128:
            cls._cache.pop(next(iter(cls._cache)))
        return copy.deepcopy(value)

    @staticmethod
    def _normalize_payload(analysis_payload: Any) -> dict[str, Any]:
        if isinstance(analysis_payload, dict):
            return analysis_payload
        if isinstance(analysis_payload, str) and analysis_payload.strip():
            try:
                parsed = json.loads(analysis_payload)
            except json.JSONDecodeError:
                return {}
            return parsed if isinstance(parsed, dict) else {}
        return {}

    @classmethod
    def build(
        cls,
        sections: list[str] | tuple[str, ...],
        report_context: dict[str, Any] | None,
        analysis_payload: Any,
        data_version: str = "",
    ) -> dict[str, Any]:
        context = dict(report_context or {})
        payload = cls._normalize_payload(analysis_payload)
        cache_key = _digest({
            "version": cls.CACHE_VERSION,
            "sections": list(sections or []),
            "context": context,
            "payload": payload,
            "data_version": data_version,
        })
        if cache_key in cls._cache:
            cls._stats["hits"] += 1
            return copy.deepcopy(cls._cache[cache_key])
        cls._stats["misses"] += 1

        forecast = payload.get("forecast") if isinstance(payload.get("forecast"), dict) else {}
        dashboard = forecast.get("dashboard_snapshot") if isinstance(forecast.get("dashboard_snapshot"), dict) else {}
        queue = dashboard.get("decision_queue") if isinstance(dashboard.get("decision_queue"), list) else []
        defensibility = dashboard.get("projection_defensibility") if isinstance(dashboard.get("projection_defensibility"), dict) else {}
        sync_status = payload.get("sync_status") if isinstance(payload.get("sync_status"), dict) else {}
        summary = " ".join(
            _clean(context.get(key), 80)
            for key in ("financial_summary", "management_brief")
            if _clean(context.get(key), 80)
        )
        protected_facts = []
        for value in re.findall(r"(?i)Rp\s*\d[\d.,]*|\b\d[\d.,]*\s*(?:hari|bulan|%|x)\b", summary):
            if value not in protected_facts:
                protected_facts.append(value)

        claim_ledger = []
        for item in context.get("agent_evidence_ledger", []) or []:
            if not isinstance(item, dict) or not _clean(item.get("claim")):
                continue
            claim_ledger.append({
                "claim_id": f"K-{len(claim_ledger) + 1:03d}",
                "claim": _clean(item.get("claim")),
                "allowed_use": _clean(item.get("allowed_use")),
                "confidence": str(item.get("confidence") or "medium").lower(),
                "claim_type": "financial_evidence",
            })
        for item in queue[:5]:
            name = _clean(item.get("name"), 16) or "Akun prioritas"
            amount = int(item.get("amount") or 0)
            days = max(int(item.get("days_overdue") or 0), 0)
            claim_ledger.append({
                "claim_id": f"K-{len(claim_ledger) + 1:03d}",
                "claim": f"{name} memiliki nilai prioritas Rp{amount:,} dengan keterlambatan {days} hari.",
                "allowed_use": _clean(item.get("action") or "Konfirmasi status dan tanggal bayar."),
                "confidence": "high",
                "claim_type": "current_collection_exposure",
            })

        section_list = [str(item).strip() for item in sections if str(item).strip()]
        chapter_contracts = []
        for index, title in enumerate(section_list):
            chapter_contracts.append({
                "section_id": title,
                "title": title,
                "depends_on": section_list[index - 1] if index else "",
                "hands_off_to": section_list[index + 1] if index + 1 < len(section_list) else "",
                "argument_contract": ["angka", "status siklus tagihan", "risiko kas", "countercheck", "tindakan dan kontrol"],
                "closing_obligation": "Nyatakan konsekuensi terhadap bagian berikutnya tanpa mengulang dashboard.",
            })

        gaps = []
        dossier = context.get("osint_dossier") if isinstance(context.get("osint_dossier"), dict) else {}
        if not (dossier.get("evidence_cards") or []):
            gaps.append({
                "area": "Pembanding eksternal",
                "gap": "Pembanding eksternal yang cukup sebanding belum tersedia.",
                "handling": "Gunakan data keuangan internal sebagai dasar keputusan.",
            })
        if not payload.get("invoice_lifecycle_summary"):
            gaps.append({
                "area": "Ringkasan siklus tagihan",
                "gap": "Ringkasan agregat tagihan lunas, aktif, dan parsial belum tersedia pada payload laporan.",
                "handling": "Jangan membawa tagihan lunas ke daftar urgensi; gunakan hanya antrean eksposur aktif.",
            })
        if not queue:
            gaps.append({
                "area": "Antrean penagihan",
                "gap": "Akun prioritas belum tersedia pada cakupan aktif.",
                "handling": "Batasi rekomendasi pada kontrol portofolio dan validasi data.",
            })

        financial_sync = sync_status.get("financialData") if isinstance(sync_status.get("financialData"), dict) else {}
        cashout_sync = sync_status.get("cashOutSource") if isinstance(sync_status.get("cashOutSource"), dict) else {}
        contract = {
            "cache_key": cache_key,
            "data_version": data_version,
            "evidence_dossier": {
                "snapshot_policy": "immutable_per_generation",
                "selected_period": _clean((payload.get("selected_period") or {}).get("label"), 12),
                "financial_records": int(financial_sync.get("recordCount") or 0),
                "cashout_records": int(cashout_sync.get("recordCount") or 0),
                "protected_facts": protected_facts,
                "active_priority_accounts": len(queue),
            },
            "research_plan": {
                "questions": [
                    {"question": "Apakah status lunas, aktif, dan parsial sudah dipisahkan sebelum urgensi ditetapkan?", "countercheck": "tagihan lunas tidak boleh masuk antrean aktif"},
                    {"question": "Angka atau tanggal apa yang paling mengubah ending cash?", "countercheck": "uji sensitivitas tanggal bayar dan cash out"},
                    {"question": "Apakah hambatan berupa dokumen, persetujuan, sengketa, atau likuiditas didukung bukti?", "countercheck": "jangan mengubah konteks eksternal menjadi fakta akun"},
                ],
                "bounded": True,
            },
            "document_thesis": "Laporan harus memisahkan fakta siklus tagihan, risiko kas saat ini, dan skenario masa depan sebelum menetapkan tindakan koleksi dan kontrol.",
            "chapter_contracts": chapter_contracts,
            "claim_ledger": claim_ledger,
            "data_gap_register": gaps,
            "editorial_contract": {
                "voice": "pengendali keuangan yang presisi, tenang, dan bertanggung jawab",
                "rules": [
                    "Tulis langsung dalam Bahasa Indonesia yang alami dan tidak mencampur fragmen istilah operasional.",
                    "Mulai dari angka dan status siklus tagihan sebelum menyatakan risiko.",
                    "Pisahkan histori lunas, eksposur aktif, dan skenario proyeksi.",
                    "Setiap tindakan harus memiliki penanggung jawab, tenggat, dampak, dan kontrol tindak lanjut.",
                ],
                "meaning_lock": ["nilai", "tanggal", "status", "akun", "periode", "formula", "confidence"],
                "forbidden": ["label agen", "nama dataset", "prompt", "chain-of-thought", "urgensi tagihan lunas"],
            },
            "appendix_manifest": {
                "calculation_basis": {
                    "formula": _clean(forecast.get("formula"), 22),
                    "period": _clean((payload.get("selected_period") or {}).get("label"), 12),
                    "financial_records": int(financial_sync.get("recordCount") or 0),
                    "cashout_records": int(cashout_sync.get("recordCount") or 0),
                },
                "sensitivity": [_clean(item, 30) for item in defensibility.get("sensitivity", []) if _clean(item, 30)],
                "counterchecks": [_clean(item, 30) for item in defensibility.get("challenge_checks", []) if _clean(item, 30)],
                "lifecycle_policy": [
                    "Tagihan lunas diperlakukan sebagai bukti historis dan tidak dimasukkan ke antrean urgensi aktif.",
                    "Pembayaran parsial tetap terbuka sampai nilai kewajiban terselesaikan.",
                    "Tanggal pembayaran tidak boleh mendahului tanggal tagihan tanpa peninjauan kualitas data.",
                ],
                "data_gaps": gaps,
            },
        }
        return cls._remember(cache_key, contract)

    @staticmethod
    def for_section(contract: dict[str, Any], section_title: str) -> str:
        chapter = next(
            (item for item in contract.get("chapter_contracts", []) if item.get("section_id") == section_title),
            {},
        )
        payload = {
            "document_thesis": contract.get("document_thesis"),
            "section_contract": chapter,
            "accepted_claims": contract.get("claim_ledger"),
            "data_gaps": contract.get("data_gap_register"),
            "editorial_contract": contract.get("editorial_contract"),
        }
        return (
            "[DOCUMENT_DELIBERATION] Gunakan kontrak ini secara internal dan jangan tampilkan struktur atau proses berpikirnya. "
            + json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        )

    @staticmethod
    def build_appendix_markdown(contract: dict[str, Any]) -> str:
        manifest = contract.get("appendix_manifest") or {}
        basis = manifest.get("calculation_basis") or {}
        lines = [
            "# Lampiran Dasar Perhitungan, Sensitivitas, dan Kesenjangan Data",
            "Lampiran ini memisahkan rincian perhitungan dan kontrol dari narasi utama agar keputusan tetap mudah dibaca dan dapat diaudit.",
            "",
            "## A. Dasar Perhitungan dan Cakupan",
            f"- Periode aktif: {basis.get('period') or 'periode terpilih'}.",
            f"- Formula utama: {basis.get('formula') or 'saldo awal + kas masuk - kas keluar = saldo akhir'}.",
            f"- Rekam finansial terbaca: {basis.get('financial_records') or 0}; komitmen kas keluar terbaca: {basis.get('cashout_records') or 0}.",
            "",
            "## B. Sensitivitas dan Countercheck",
        ]
        sensitivity = manifest.get("sensitivity", [])
        counterchecks = manifest.get("counterchecks", [])
        lines.extend(f"- Sensitivitas: {item}" for item in sensitivity)
        lines.extend(f"- Countercheck: {item}" for item in counterchecks)
        if not sensitivity and not counterchecks:
            lines.append("- Sensitivitas khusus belum tersedia; validasi tanggal bayar dan cash out tetap diperlukan.")
        lines.extend(["", "## C. Rekonsiliasi Siklus Tagihan"])
        lines.extend(f"- {item}" for item in manifest.get("lifecycle_policy", []))
        lines.extend(["", "## D. Kesenjangan Data"])
        gaps = manifest.get("data_gaps", [])
        if gaps:
            lines.extend(f"- **{item.get('area')}:** {item.get('gap')} {item.get('handling')}" for item in gaps)
        else:
            lines.append("- Tidak ada kesenjangan data material yang teridentifikasi pada payload aktif.")
        return "\n".join(lines).strip()
