"""Selective Indonesian editing that preserves accepted factual tokens."""
from __future__ import annotations

import os
import re
from collections import Counter
from typing import Callable, Optional

from evidence_quality import quality_check
from editorial_intelligence import assess_payment_style

FACT_PATTERNS = (
    r"(?i)\b(?:Rp\s*)?\d[\d.,]*(?:\s*(?:juta|miliar|triliun|%|hari|bulan|tahun))?",
    r"\b\d{4}\b",
    r"\b[A-Z]{2,}[A-Z0-9._/-]*\b",
)
MARKER_RE = re.compile(r"\[\[(?:CHART|PIE|FLOW|DASHBOARD|GANTT):.*?\]\]")


class ProtectedIndonesianEditor:
    QUALITY_GATE_ISSUES = {
        "near_duplicate_paragraph", "near_duplicate_opening", "indonesian_spelling_density",
    }

    def __init__(
        self,
        model_client=None,
        model_name=None,
        quality_fn: Optional[Callable] = None,
        indonesian_quality_gate=None,
    ):
        self.model_client = model_client
        self.model_name = model_name or os.getenv("LLM_MODEL", "gpt-oss:120b-cloud")
        self.quality_fn = quality_fn or (lambda text, protected: quality_check(text, protected))
        if indonesian_quality_gate is None:
            from indonesian_quality import IndonesianQualityGate
            indonesian_quality_gate = IndonesianQualityGate()
        self.indonesian_quality_gate = indonesian_quality_gate

    @staticmethod
    def protected_values(text):
        values = set(MARKER_RE.findall(str(text or "")))
        for pattern in FACT_PATTERNS:
            values.update(match.strip() for match in re.findall(pattern, str(text or "")) if match.strip())
        return sorted(value for value in values if len(value) >= 2)[:120]

    @classmethod
    def numeric_tokens(cls, text):
        return [
            re.sub(r"\s+", " ", token.strip())
            for token in re.findall(FACT_PATTERNS[0], str(text or ""))
            if token.strip()
        ]

    @classmethod
    def preserves_numeric_facts(cls, original, candidate):
        return Counter(cls.numeric_tokens(original)) == Counter(cls.numeric_tokens(candidate))

    @staticmethod
    def preserves_structure(original, candidate):
        def structural_lines(value):
            return tuple(
                line.strip()
                for line in str(value or "").splitlines()
                if line.lstrip().startswith(("#", "|", "[["))
            )
        return structural_lines(original) == structural_lines(candidate)

    def _client(self):
        if self.model_client is not None:
            return self.model_client
        try:
            from ollama import Client
            from config import OLLAMA_HOST
            return Client(host=OLLAMA_HOST)
        except Exception:
            return None

    @staticmethod
    def local_template_issues(text):
        source = str(text or "")
        issues = []
        lowered = source.lower()
        repeated_labels = (
            "cash in hand pada",
            "formula dashboard",
            "status sinkronisasi data finansial",
            "eksposur risiko tinggi",
            "sumber arus kas keluar",
        )
        if sum(lowered.count(label) for label in repeated_labels) >= 3:
            issues.append("repeated_dashboard_frame")
        if re.search(r"(?i)\b(?:layanan|risiko|akun|penyebab)\s*[-:]\s*-?(?:\s*[.;,]|$)|:\s+-\s*(?:[.;,]|$)", source):
            issues.append("placeholder_dash_leakage")
        sentences = [
            re.sub(r"\s+", " ", item).strip()
            for item in re.split(r"(?<=[.!?])\s+", source)
            if len(item.split()) >= 4
        ]
        starts = {}
        for sentence in sentences:
            key = " ".join(re.findall(r"\w+", sentence.lower())[:4])
            if key:
                starts[key] = starts.get(key, 0) + 1
        if any(count >= 3 for count in starts.values()):
            issues.append("repeated_opening")
        stock_terms = re.findall(
            r"(?i)\b(perlu|dapat|fokus|risiko utama|perlu diprioritaskan|dapat menjadi|"
            r"perlu perhatian|status sinkronisasi|formula dashboard)\b",
            source,
        )
        if len(stock_terms) >= max(8, len(sentences) // 3):
            issues.append("stock_finance_phrase_density")
        return sorted(set(issues + assess_payment_style(source).get("findings", [])))

    def polish(self, text, guidance=""):
        original = str(text or "")
        if os.getenv("EVIDENCE_QUALITY_WRITING_ENABLED", "true").strip().lower() not in {"1", "true", "yes", "on"}:
            return original
        protected = self.protected_values(original)
        initial = self.quality_fn(original, protected)
        gate_result = self.indonesian_quality_gate.evaluate(original)
        initial["issues"] = sorted(set(
            list(initial.get("issues", []))
            + self.local_template_issues(original)
            + list(gate_result.issues)
        ))
        issues = [
            issue for issue in initial.get("issues", [])
            if issue in {
                "repeated_sentence", "repeated_paragraph", "filler_phrase", "unnecessary_english",
                "long_sentence", "repeated_opening", "stock_transition", "unexplained_technical_term",
                "repeated_dashboard_frame", "placeholder_dash_leakage", "stock_finance_phrase_density",
                "dashboard_restatement", "repeated_openings", *self.QUALITY_GATE_ISSUES,
            }
        ]
        if not issues:
            return original
        client = self._client()
        if client is None:
            return original
        prompt = (
            "Sunting teks berikut dalam Bahasa Indonesia profesional yang ringkas, alami, dan mudah dipakai manajemen. "
            "Hilangkan repetisi, kalimat pengisi, campuran bahasa Inggris yang tidak perlu, dan kalimat terlalu panjang. "
            "Pertahankan seluruh heading, tabel Markdown, penanda visual, nama, angka, tanggal, persentase, nilai uang, "
            "kode, makna, serta klaim faktual. Jangan menambah fakta baru. Keluarkan teks final saja.\n\n"
            f"PANDUAN GAYA KHUSUS USE CASE:\n{str(guidance or '').strip() or 'Gunakan suara financial controller yang presisi dan berbasis angka.'}\n\n"
            f"MASALAH: {', '.join(issues)}\n\nTEKS:\n{original}"
        )
        try:
            response = client.chat(
                model=self.model_name,
                messages=[
                    {"role": "system", "content": "Anda adalah editor Bahasa Indonesia yang konservatif dan terikat fakta."},
                    {"role": "user", "content": prompt},
                ],
                options={"temperature": 0.1},
            )
            improved = str(response.get("message", {}).get("content", "") or "").strip()
        except Exception:
            return original
        if not improved:
            return original
        if not self.preserves_numeric_facts(original, improved):
            return original
        if not self.preserves_structure(original, improved):
            return original
        final = self.quality_fn(improved, protected)
        final_gate_result = self.indonesian_quality_gate.evaluate(improved)
        final["issues"] = sorted(set(
            list(final.get("issues", []))
            + self.local_template_issues(improved)
            + list(final_gate_result.issues)
        ))
        if final.get("protected_missing"):
            return original
        remaining = {
            issue for issue in final.get("issues", [])
            if issue in {
                "repeated_sentence", "repeated_paragraph", "filler_phrase", "unnecessary_english",
                "long_sentence", "repeated_opening", "stock_transition", "unexplained_technical_term",
                "repeated_dashboard_frame", "placeholder_dash_leakage", "stock_finance_phrase_density",
                "dashboard_restatement", "repeated_openings", *self.QUALITY_GATE_ISSUES,
            }
        }
        if remaining:
            return original
        return improved
