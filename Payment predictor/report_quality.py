import re

from config import REPORT_MIN_COMPLETENESS_SCORE
from report_structure import REPORT_STRUCTURE


class ReportQualityScorer:
    def __init__(self, structure=REPORT_STRUCTURE, min_score=REPORT_MIN_COMPLETENESS_SCORE):
        self.structure = structure
        self.min_score = min_score

    def score(self, raw_text):
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

        total_score = round(sum(components.values()), 1)
        return {
            "score": total_score,
            "passed": total_score >= self.min_score,
            "components": components,
            "missing": missing,
        }

    def final_qa(self, raw_text):
        report_text = str(raw_text or "")
        categories = set()
        findings = []
        if re.search(r"\b(Internal API|APIDog|endpoint|source\s*=|Invoice Evidence Analyst|Control Reviewer|agent workflow)\b", report_text, flags=re.IGNORECASE):
            categories.add("raw_source_label")
            findings.append("Laporan masih memuat label sumber atau peran internal.")
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

    def is_acceptable(self, raw_text):
        return self.score(raw_text)["passed"]
