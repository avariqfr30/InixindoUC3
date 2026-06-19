"""App-owned writing planner for cashflow report sections."""
from __future__ import annotations

import re
import json
import hashlib
import copy
from typing import Any
from report_evidence import PaymentEvidenceBuilder


def _compact(value: Any, limit: int = 360) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if len(text) <= limit:
        return text
    return text[:limit].rsplit(" ", 1)[0].rstrip(" ,.;") + "."


def _digest(value: Any) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


class PaymentSectionPlanner:
    """Build hidden guidance for finance-first cashflow writing."""
    CACHE_VERSION = "payment-section-plan-v2"
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
    def _remember(cls, key: str, plan: dict[str, Any]) -> dict[str, Any]:
        if key in cls._cache:
            cls._cache.pop(key, None)
        cls._cache[key] = copy.deepcopy(plan)
        while len(cls._cache) > 256:
            cls._cache.pop(next(iter(cls._cache)))
        return copy.deepcopy(plan)

    def build_plan(self, sections: list[str] | tuple[str, ...], report_context: dict[str, Any] | None = None) -> dict[str, Any]:
        report_context = report_context or {}
        document_contract = report_context.get("document_contract") if isinstance(report_context.get("document_contract"), dict) else {}
        cache_key = _digest({
            "version": self.CACHE_VERSION,
            "sections": list(sections or []),
            "financial_summary": report_context.get("financial_summary"),
            "management_brief": report_context.get("management_brief"),
            "data_version": report_context.get("data_version"),
            "focus": report_context.get("focus") or report_context.get("notes"),
            "osint_dossier": report_context.get("osint_dossier"),
            "document_contract_key": document_contract.get("cache_key"),
        })
        if cache_key in self._cache:
            self._stats["hits"] += 1
            return copy.deepcopy(self._cache[cache_key])
        self._stats["misses"] += 1
        section_list = ", ".join(str(item).strip() for item in sections if str(item).strip())
        summary = _compact(report_context.get("financial_summary") or report_context.get("management_brief") or "")
        protected = []
        for match in re.findall(r"(?i)\b(?:Rp\s*)?\d[\d.,]*(?:\s*(?:juta|miliar|triliun|%|hari|bulan|tahun|invoice))?", summary):
            if match.strip():
                protected.append(match.strip())
        osint_dossier = report_context.get("osint_dossier") if isinstance(report_context.get("osint_dossier"), dict) else {}
        osint_cards = [
            {
                "claim": str(card.get("claim", "") or "").strip(),
                "why_it_matters": str(card.get("why_it_matters", "") or "").strip(),
                "source_domain": str(card.get("source_domain", "") or "").strip(),
                "allowed_use": str(card.get("allowed_use", "") or "").strip(),
                "matched_internal_fact": str(card.get("matched_internal_fact", "") or "").strip(),
            }
            for card in (osint_dossier.get("evidence_cards") or [])[:5]
            if isinstance(card, dict) and str(card.get("claim", "") or "").strip()
        ]
        internal_insight_cards = PaymentEvidenceBuilder.insight_cards(
            report_context.get("agent_evidence_ledger") or [],
            financial_summary=report_context.get("financial_summary") or "",
            management_brief=report_context.get("management_brief") or "",
            osint_dossier=osint_dossier,
        )
        plan = {
            "cache_key": cache_key,
            "data_version": str(report_context.get("data_version") or ""),
            "use_case": "payment",
            "reader": "finance_and_management_team",
            "section_title": section_list or "bagian laporan yang sedang disusun",
            "section_goal": "mengubah angka cashflow, aging, status invoice, dan risiko koleksi menjadi tindakan operasional",
            "user_flow_context": "cashflow_report generation from dashboard/report focus",
            "evidence_required": ["nilai invoice", "status pembayaran", "aging", "partner/customer", "cash in/out context"],
            "protected_facts": protected[:40],
            "tone_rules": ["financial_controller_precise", "number_led", "restrained_risk_language"],
            "avoid_patterns": [
                "dramatic claims",
                "generic attention phrases",
                "unsupported cashflow conclusions",
                "placeholder dash leakage",
                "repeated dashboard formula blocks",
                "finance commentary that does not start from numbers",
            ],
            "quality_thresholds": {"preserve_numbers": True, "require_action_for_risk": True},
            "narrative_thesis": "laporan cashflow harus membantu manajemen melihat angka, risiko kas, dan tindakan berikutnya dengan cepat",
            "internal_insight_cards": internal_insight_cards,
            "paragraph_roles": [
                "angka utama",
                "risiko kas",
                "penyebab atau pola",
                "batas keyakinan",
                "tindakan operasional",
            ],
            "osint_dossier_quality": osint_dossier.get("quality") or {},
            "osint_evidence_cards": osint_cards,
            "retrieval_intent": {
                "goal": "find invoice, payment status, aging, partner, and cashflow evidence for the selected report section",
                "preferred_datasets": ["InvoiceTraining", "InvoiceConsultant", "ReferenceAccount"],
                "exclude": ["FinanceInvoice", "ProjectStandards"],
                "preferred_terms": [section_list, summary],
            },
            "evidence_ledger": [
                {
                    "claim_role": "cashflow risk or action",
                    "evidence_source": "InvoiceTraining, InvoiceConsultant, or ReferenceAccount",
                    "confidence": "high only when number, status, and aging agree",
                    "allowed_wording": "angka apa yang paling penting, risiko kas apa yang muncul, dan tindakan operasional apa yang perlu diprioritaskan",
                }
            ],
            "rationale_summary": {
                "main_reasoning": "Laporan payment harus bergerak dari angka ke risiko kas lalu ke tindakan, bukan dari narasi umum ke rekomendasi.",
                "evidence_used": ["invoice value", "payment status", "aging", "partner/customer context"],
                "caveats": ["jangan mengubah angka, status, atau tanggal; jangan menyimpulkan risiko tanpa dukungan data"],
            },
            "financial_summary": summary,
            "document_thesis": document_contract.get("document_thesis") or "",
            "chapter_contracts": document_contract.get("chapter_contracts") or [],
            "data_gap_register": document_contract.get("data_gap_register") or [],
            "editorial_contract": document_contract.get("editorial_contract") or {},
            "appendix_manifest": document_contract.get("appendix_manifest") or {},
        }
        return self._remember(cache_key, plan)

    def build_prompt_block_from_plan(self, plan: dict[str, Any]) -> str:
        return (
            "[SECTION_PLANNER] "
            f"[SECTION_PLAN_JSON] {plan} "
            f"Rencanakan bagian laporan cashflow: {plan.get('section_title')}. "
            "Sebelum menulis, tentukan angka apa yang paling penting, risiko kas apa yang muncul dari angka itu, "
            "dan tindakan operasional apa yang perlu diprioritaskan. "
            f"Tesis naratif: {plan.get('narrative_thesis')}. "
            f"Insight internal/dashboard terkurasi: {plan.get('internal_insight_cards')}. "
            f"Rotasi peran paragraf: {plan.get('paragraph_roles')}. "
            "Gunakan suara financial controller: presisi, tenang, tidak dramatis, dan selalu mengikat rekomendasi ke angka. "
            "Pisahkan kondisi cash in, tekanan cash out, aging invoice, risiko koleksi, dan prioritas 30 hari. "
            "Hindari frasa umum seperti 'perlu perhatian lebih' kecuali langsung diikuti tindakan dan dampak. "
            "Jangan menampilkan '-' sebagai layanan, risiko, account, atau penyebab; jika data tidak cukup, katakan batasannya secara natural. "
            f"Retrieval intent: {plan.get('retrieval_intent')}. Evidence ledger: {plan.get('evidence_ledger')}. "
            f"OSINT cards matched to internal cashflow context: {plan.get('osint_evidence_cards')}. "
            f"Rationale ringkas: {plan.get('rationale_summary')}. "
            f"Ringkasan angka yang harus dihormati: {plan.get('financial_summary') or 'gunakan ringkasan finansial dan bukti internal yang tersedia.'}"
            f" Tesis dokumen: {plan.get('document_thesis')}. Kontrak bagian: {plan.get('chapter_contracts')}. "
            f"Kesenjangan data: {plan.get('data_gap_register')}. Kontrak editorial: {plan.get('editorial_contract')}."
        )

    def build_prompt_block(self, sections: list[str] | tuple[str, ...], report_context: dict[str, Any] | None = None) -> str:
        return self.build_prompt_block_from_plan(self.build_plan(sections, report_context))
