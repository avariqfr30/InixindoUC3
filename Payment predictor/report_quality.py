import re

from config import REPORT_MIN_COMPLETENESS_SCORE
from reasoning_policy import CashflowHotsReasoningPolicy
from report_structure import REPORT_STRUCTURE


class ReportQualityScorer:
    def __init__(self, structure=REPORT_STRUCTURE, min_score=REPORT_MIN_COMPLETENESS_SCORE):
        self.structure = structure
        self.min_score = min_score

    def score(self, raw_text, analysis_payload=None):
        report_text = str(raw_text or "")
        if not report_text.strip():
            return {
                "score": 0.0,
                "passed": False,
                "components": {},
                "missing": ["Dokumen kosong."],
            }

        components = {}
        missing = []

        required_headings = [f"# {section}" for section in self.structure.section_sequence]
        present_headings = [heading for heading in required_headings if heading in report_text]
        heading_score = (len(present_headings) / len(required_headings)) * 30
        components["top_level_sections"] = round(heading_score, 1)
        if len(present_headings) < len(required_headings):
            missing_sections = [heading.replace("# ", "") for heading in required_headings if heading not in present_headings]
            missing.append(f"Bagian utama belum lengkap: {', '.join(missing_sections)}.")

        present_subheadings = [
            subheading for subheading in self.structure.required_subheadings if subheading in report_text
        ]
        subheading_score = (len(present_subheadings) / len(self.structure.required_subheadings)) * 20
        components["required_subsections"] = round(subheading_score, 1)
        if len(present_subheadings) < len(self.structure.required_subheadings):
            missing_subsections = [
                subheading.replace("### ", "")
                for subheading in self.structure.required_subheadings
                if subheading not in present_subheadings
            ]
            missing.append(f"Subbagian wajib belum lengkap: {', '.join(missing_subsections)}.")

        table_score = 0
        if re.search(self.structure.required_tables["scenario_cash_in"], report_text, re.IGNORECASE):
            table_score += 8
        else:
            missing.append("Tabel skenario arus kas masuk belum ditemukan.")

        if re.search(self.structure.required_tables["priority_30_day"], report_text):
            table_score += 12
        else:
            missing.append("Tabel prioritas 30 hari belum lengkap.")
        components["tables_and_owners"] = round(table_score, 1)

        enrichment_score = 0
        if "### Konteks OSINT Pendukung" in report_text:
            enrichment_score += 4
        if "[[CHART:" in report_text:
            enrichment_score += 3
        else:
            missing.append("Visual distribusi pembayaran belum masuk.")
        if "[[FLOW:" in report_text:
            enrichment_score += 3
        else:
            missing.append("Visual alur mitigasi belum masuk.")
        components["context_and_visuals"] = round(enrichment_score, 1)

        consistency_score = 10
        contradiction_patterns = [
            r"turun[^.\n]{0,120}memburuk",
            r"menurun[^.\n]{0,120}memburuk",
            r"naik[^.\n]{0,120}membaik",
            r"meningkat[^.\n]{0,120}membaik",
        ]
        if any(re.search(pattern, report_text, re.IGNORECASE) for pattern in contradiction_patterns):
            consistency_score = 0
            missing.append("Narasi tren risiko terdeteksi kontradiktif terhadap arah metrik.")
        components["numeric_consistency"] = round(consistency_score, 1)

        density_score = 0
        if len(report_text.strip()) >= 4500:
            density_score += 4
        elif len(report_text.strip()) >= 3200:
            density_score += 2
        else:
            missing.append("Narasi laporan masih terlalu tipis untuk bahan rapat internal.")

        if report_text.count("\n- ") >= 12:
            density_score += 3
        else:
            missing.append("Rincian bullet operasional masih kurang kaya.")

        if report_text.count("\n1.") >= 1 or report_text.count("\n1. ") >= 1:
            density_score += 1
        else:
            missing.append("Daftar tindakan bernomor belum kuat.")

        if "catatan" in report_text.lower() or "bukti" in report_text.lower():
            density_score += 2
        else:
            missing.append("Rujukan bukti internal belum cukup terlihat.")
        components["narrative_density"] = round(density_score, 1)

        numeric_accuracy_score = self._score_numeric_accuracy(report_text, analysis_payload)
        components["numeric_accuracy"] = round(numeric_accuracy_score, 1)

        total_score = round(sum(components.values()), 1)
        return {
            "score": total_score,
            "passed": total_score >= self.min_score,
            "components": components,
            "missing": missing,
        }

    @staticmethod
    def _score_numeric_accuracy(report_text, analysis_payload):
        """Verify key financial figures from source data appear in the report."""
        if not analysis_payload or not isinstance(analysis_payload, dict):
            return 10  # No payload to check against

        key_figures = []
        for key in ("total_outstanding", "ending_cash", "total_cash_in",
                     "coverage_ratio", "total_nominal"):
            val = analysis_payload.get(key)
            if val and isinstance(val, (int, float)) and val > 0:
                key_figures.append(val)

        if not key_figures:
            return 10

        matched = 0
        for figure in key_figures:
            formatted_variants = [
                f"{figure:,.0f}",
                f"{figure/1e6:,.0f}",
                f"{figure/1e9:,.1f}",
                f"{figure/1e9:,.2f}",
                str(int(figure)),
            ]
            if any(v in report_text for v in formatted_variants):
                matched += 1

        accuracy_ratio = matched / len(key_figures)
        return round(accuracy_ratio * 10)

    def final_qa(self, raw_text, rejected_claims=None):
        report_text = str(raw_text or "")
        categories = set()
        findings = []
        if re.search(
            r"\b(Internal API|APIDog|endpoint|source\s*=|Invoice Evidence Analyst|Context Analyst|Collection Risk Analyst|Forecast Analyst|Control Reviewer|Executive Editor|agent workflow)\b",
            report_text,
            flags=re.IGNORECASE,
        ):
            categories.add("raw_source_label")
            findings.append("Laporan masih memuat label sumber atau peran internal.")
        visible_reasoning = CashflowHotsReasoningPolicy.find_visible_reasoning(report_text)
        if visible_reasoning:
            categories.add("visible_reasoning")
            findings.append("Laporan masih memuat label atau proses penalaran internal.")
        if CashflowHotsReasoningPolicy.has_uncalibrated_horizon_claim(report_text):
            categories.add("uncalibrated_horizon_claim")
            findings.append("Klaim jangka panjang terlalu keras tanpa catatan batasan atau tingkat keyakinan.")
        rejected_findings = self._find_rejected_claim_violations(report_text, rejected_claims)
        if rejected_findings:
            categories.add("rejected_claim")
            findings.extend(rejected_findings)
        for section in self.structure.section_sequence:
            match = re.search(rf"(?ms)^# {re.escape(section)}\s*(.*?)(?=^# |\Z)", report_text)
            body = match.group(1).strip() if match else ""
            plain_body = re.sub(r"\[\[(?:CHART|FLOW|DASHBOARD):.*?\]\]", " ", body)
            plain_body = re.sub(r"[#*`>|_]", " ", plain_body)
            if len(re.sub(r"\s+", " ", plain_body).strip()) < 40:
                categories.add("empty_section")
                findings.append(f"{section} kosong atau terlalu tipis.")
        if "### Ringkasan Isi Laporan" not in report_text:
            categories.add("missing_section_synthesis")
            findings.append("Ringkasan eksekutif belum merangkum isi laporan akhir.")
        return {"passes": not categories, "categories": sorted(categories), "findings": findings}

    @staticmethod
    def _find_rejected_claim_violations(report_text, rejected_claims):
        text = str(report_text or "")
        lowered = text.lower()
        findings = []
        for claim in rejected_claims or []:
            claim_text = str(claim or "").strip()
            if not claim_text:
                continue
            unsupported_priority = re.search(
                r"Jangan\s+menyatakan\s+(.+?)\s+sebagai\s+prioritas",
                claim_text,
                flags=re.IGNORECASE,
            )
            if unsupported_priority:
                term = unsupported_priority.group(1).strip(" .,:;")
                if term and re.search(rf"\b{re.escape(term)}\b", text, flags=re.IGNORECASE):
                    findings.append(f"Laporan melanggar rejected-claim gate untuk {term}.")
                continue

            blocked_terms = []
            if re.search(r"Jangan\s+menampilkan\s+istilah", claim_text, flags=re.IGNORECASE):
                blocked_terms.extend(re.findall(r"\b(agent|desk|workflow)\b", claim_text, flags=re.IGNORECASE))
            if "penyebab eksternal" in claim_text.lower() and "penyebab eksternal" in lowered and "sebagai fakta" in lowered:
                findings.append("Laporan menjadikan penyebab eksternal sebagai fakta internal.")
            for term in blocked_terms:
                if re.search(rf"\b{re.escape(term)}\b", text, flags=re.IGNORECASE):
                    findings.append(f"Laporan masih memuat istilah internal {term}.")
        return findings

    def is_acceptable(self, raw_text):
        return self.score(raw_text)["passed"]
