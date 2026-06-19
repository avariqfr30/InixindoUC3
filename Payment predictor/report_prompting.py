from config import FINANCE_SYSTEM_PROMPT, PERSONAS
from reasoning_policy import CashflowHotsReasoningPolicy
from report_planning import PaymentSectionPlanner
from report_structure import REPORT_STRUCTURE


class ReportPromptBuilder:
    def __init__(self, structure=REPORT_STRUCTURE):
        self.structure = structure
        self.planner = PaymentSectionPlanner()

    def build_user_instruction(self, notes, active_sections):
        notes = (notes or "").strip()
        report_sections = "\n".join(f"- {section}" for section in active_sections)
        focus_block = notes if notes else "Tidak ada fokus tambahan dari pengguna."
        return (
            "Susun laporan internal yang detail, profesional, dan siap dipakai sebagai bahan diskusi rapat manajemen.\n"
            "Jangan menulis seperti jawaban AI generik; tulis seperti memo manajemen yang berbasis data.\n"
            "Baca dashboard sebagai pusat keputusan: angka utama, perubahan risiko, gap cash-in/cash-out, lalu tindakan. "
            "Jangan membuat paragraf yang hanya mengulang formula; jelaskan apa arti angka itu bagi keputusan minggu ini.\n"
            "Kerjakan hanya section yang diminta pada pass ini dan hentikan output setelah section terakhir selesai.\n"
            "Pastikan setiap bagian di bawah terisi secara jelas:\n"
            f"{report_sections}\n\n"
            "Setiap rekomendasi harus menyebutkan fokus tindakan dan dampak yang diharapkan.\n"
            f"Fokus pengguna:\n{focus_block}"
        )

    def build_section_scope(self, active_sections):
        section_list = "\n".join(f"- {section}" for section in active_sections)
        section_headings = "\n".join(f"   # {section}" for section in active_sections)
        section_scope = (
            "Generate only the sections listed below for this pass.\n"
            "Start with the first heading listed and stop after the last heading listed.\n"
            "Do not repeat prior sections and do not preview later sections.\n"
            f"{section_list}"
        )
        return section_scope, section_headings

    @staticmethod
    def format_structured_context_block(raw_text):
        lines = []
        for raw_line in str(raw_text or "").splitlines():
            cleaned_line = raw_line.strip()
            if not cleaned_line:
                if lines and lines[-1] != "":
                    lines.append("")
                continue
            if cleaned_line.startswith("===") and cleaned_line.endswith("==="):
                cleaned_line = cleaned_line.strip("= ").strip()
            if cleaned_line.endswith(":") and not cleaned_line.startswith("-"):
                lines.append(f"#### {cleaned_line[:-1].strip()}")
            else:
                lines.append(cleaned_line)
        return "\n".join(lines).strip()

    def build_report_prompt(self, report_context, notes, analysis_context, macro_osint, active_sections, include_visuals):
        persona = PERSONAS.get("default", "Chief Financial Officer")
        section_scope, section_headings = self.build_section_scope(active_sections)
        planning_block = self.planner.build_prompt_block(active_sections, report_context)
        section_scope = f"{section_scope}\n\n{planning_block}"
        structured_context = self.format_structured_context_block(analysis_context)
        agent_evidence_brief = report_context.get(
            "agent_evidence_brief",
            "Tidak ada brief quality-control tambahan.",
        )
        final_editor_context = str(report_context.get("final_editor_context") or "").strip()
        if final_editor_context:
            agent_evidence_brief = f"{agent_evidence_brief}\n\n{final_editor_context}"
        agent_evidence_brief = f"{agent_evidence_brief}\n\n{CashflowHotsReasoningPolicy.prompt_block()}"
        return FINANCE_SYSTEM_PROMPT.format(
            persona=persona,
            financial_summary=report_context.get("financial_summary") or "Ringkasan finansial belum tersedia.",
            management_brief=report_context.get("management_brief") or "Belum ada brief manajemen tambahan.",
            internal_evidence=report_context.get("evidence") or "Bukti operasional tambahan belum tersedia.",
            agent_evidence_brief=agent_evidence_brief,
            industry_trends=macro_osint,
            user_focus=(notes or "Tidak ada fokus tambahan."),
            cashflow_context=(structured_context or "Tidak ada konteks forecast terstruktur tambahan."),
            readiness_signals=report_context.get("readiness_signals") or "Sinyal kesiapan kas belum tersedia.",
            section_scope=section_scope,
            section_headings=section_headings,
            visual_prompt=(report_context.get("visual_prompt") or "") if include_visuals else "",
        )
