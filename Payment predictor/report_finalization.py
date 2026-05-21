import json
import re

from cashflow_analysis import FinancialAnalyzer
from executive_summary_builder import ExecutiveSummaryBuilder
from reader_safe_text import reader_safe_text
from report_evidence import PaymentEvidenceBuilder
from report_structure import REPORT_STRUCTURE


class ReportFinalizer:
    def __init__(self, structure=REPORT_STRUCTURE):
        self.structure = structure

    @staticmethod
    def normalize_analysis_payload(analysis_payload):
        if isinstance(analysis_payload, dict):
            return analysis_payload
        if not analysis_payload:
            return {}
        if isinstance(analysis_payload, str):
            try:
                parsed = json.loads(analysis_payload)
            except json.JSONDecodeError:
                return {}
            return parsed if isinstance(parsed, dict) else {}
        return {}

    def build_dashboard_visual_markers(self, analysis_payload):
        payload = self.normalize_analysis_payload(analysis_payload)
        horizon_snapshot = payload.get("horizon_snapshot") or {}
        forecasts = horizon_snapshot.get("forecasts") if isinstance(horizon_snapshot, dict) else None
        if not isinstance(forecasts, dict):
            return []

        markers = []
        for horizon_key in ("short_term", "mid_term", "long_term"):
            forecast = forecasts.get(horizon_key)
            if not isinstance(forecast, dict):
                continue
            dashboard = forecast.get("dashboard_snapshot")
            if not isinstance(dashboard, dict):
                continue
            coverage_bars = (((dashboard.get("coverage_chart") or {}).get("bars")) or [])[:4]
            compact_payload = {
                "horizon_key": dashboard.get("horizon_key") or horizon_key,
                "horizon_label": dashboard.get("horizon_label"),
                "horizon_focus": dashboard.get("horizon_focus"),
                "status": dashboard.get("status"),
                "current_cash": dashboard.get("current_cash"),
                "runway_months": dashboard.get("runway_months"),
                "coverage_ratio": dashboard.get("coverage_ratio"),
                "average_delay_days": dashboard.get("average_delay_days"),
                "balance_projection": dashboard.get("balance_projection_30d") or [],
                "coverage_bars": coverage_bars,
            }
            markers.append(f"[[DASHBOARD:{json.dumps(compact_payload, ensure_ascii=False, separators=(',', ':'))}]]")
        return markers

    def build_operational_snapshot_block(self, analysis_payload):
        payload = self.normalize_analysis_payload(analysis_payload)
        if not payload:
            return ""

        selected_period = payload.get("selected_period") or {}
        sync_status = payload.get("sync_status") or {}
        financial_sync = sync_status.get("financialData") or {}
        cash_out_sync = sync_status.get("cashOutSource") or {}

        lines = []
        period_label = selected_period.get("label")
        if period_label:
            lines.append(f"- Periode dashboard yang diekspor ke laporan: {period_label}.")

        cash_on_hand = payload.get("cash_on_hand")
        if cash_on_hand is not None:
            lines.append(f"- Cash in hand pada saat review: Rp{int(cash_on_hand):,}.")

        if financial_sync:
            freshness = financial_sync.get("sourceAgeMinutes")
            freshness_label = "belum tersedia"
            if freshness is not None:
                freshness_label = f"{float(freshness):.1f} menit"
            lines.append(
                "- Status sinkronisasi data finansial: "
                f"{financial_sync.get('syncStatus') or '-'}; usia data {freshness_label}; "
                f"record aktif {int(financial_sync.get('recordCount') or 0)}."
            )

        if cash_out_sync:
            if cash_out_sync.get("configured"):
                lines.append(
                    "- Sumber cash out memakai komitmen aktual dengan status "
                    f"{cash_out_sync.get('syncStatus') or '-'} dan "
                    f"{int(cash_out_sync.get('recordCount') or 0)} item aktif."
                )
            else:
                lines.append("- Sumber cash out masih memakai model operating cost bulanan karena feed kewajiban aktual belum dikonfigurasi.")

        horizons = ((payload.get("horizon_snapshot") or {}).get("forecasts")) or {}
        for key in ("short_term", "mid_term", "long_term"):
            horizon_payload = horizons.get(key) or {}
            forecast = horizon_payload.get("forecast") or {}
            health = horizon_payload.get("cashflow_health") or {}
            horizon = horizon_payload.get("time_horizon") or {}
            if not forecast:
                continue
            lines.append(
                "- "
                f"{horizon.get('label') or key}: cash masuk {FinancialAnalyzer._format_currency(int((forecast.get('cash_in') or {}).get('total_predicted_cash_in') or 0))}, "
                f"cash keluar {FinancialAnalyzer._format_currency(int((forecast.get('cash_out') or {}).get('total_cash_out') or 0))}, "
                f"ending cash {FinancialAnalyzer._format_currency(int(forecast.get('ending_cash') or 0))}, "
                f"runway {float(health.get('runway_months') or 0):.1f} bulan, "
                f"coverage {float(health.get('coverage_ratio') or 0):.2f}x, "
                f"fokus {horizon.get('focus') or '-'}."
            )

        return "\n".join(lines).strip()

    @staticmethod
    def extract_visual_markers(visual_prompt):
        chart_marker = ""
        flow_marker = ""
        for line in str(visual_prompt or "").splitlines():
            stripped_line = line.strip()
            if stripped_line.startswith("[[CHART:"):
                chart_marker = stripped_line
            elif stripped_line.startswith("[[FLOW:"):
                flow_marker = stripped_line
        return chart_marker, flow_marker

    @staticmethod
    def split_top_level_sections(raw_text):
        matches = list(re.finditer(r"(?m)^# ([^\n]+?)\s*$", raw_text or ""))
        if not matches:
            return []

        sections = []
        for index, match in enumerate(matches):
            section_title = match.group(1).strip()
            section_end = matches[index + 1].start() if index + 1 < len(matches) else len(raw_text)
            section_body = raw_text[match.end():section_end].strip()
            sections.append({"title": section_title, "body": section_body})

        return sections

    @staticmethod
    def join_top_level_sections(sections):
        blocks = []
        for section in sections:
            section_title = section["title"].strip()
            section_body = section["body"].strip()
            if section_body:
                blocks.append(f"# {section_title}\n{section_body}")
            else:
                blocks.append(f"# {section_title}")
        return "\n\n".join(blocks).strip()

    @staticmethod
    def inject_subheading_block(section_body, subheading, content, before_subheading=None):
        if not content or not str(content).strip():
            return section_body

        subheading_marker = f"### {subheading}"
        if subheading_marker in section_body:
            return section_body

        new_block = f"{subheading_marker}\n{str(content).strip()}"
        if before_subheading:
            before_match = re.search(rf"(?m)^### {re.escape(before_subheading)}\s*$", section_body)
            if before_match:
                section_prefix = section_body[:before_match.start()].rstrip()
                section_suffix = section_body[before_match.start():].lstrip()
                return f"{section_prefix}\n\n{new_block}\n\n{section_suffix}".strip()

        return f"{section_body.rstrip()}\n\n{new_block}".strip()

    @staticmethod
    def append_marker_block(section_body, marker):
        marker = str(marker or "").strip()
        if not marker or marker in section_body:
            return section_body
        return f"{section_body.rstrip()}\n\n{marker}".strip()

    def inject_executive_headlines(self, section_body, report_context):
        headline_block = str((report_context or {}).get("executive_headlines") or "").strip()
        if not headline_block:
            headline_block = str((report_context or {}).get("executive_facts") or "").strip()
        return self.inject_subheading_block(
            section_body,
            "Sorotan Utama untuk Manajemen",
            headline_block,
            before_subheading="Dampak Bisnis" if "### Dampak Bisnis" in section_body else "Tingkat Keyakinan dan Catatan Batasan",
        )

    def build_executive_summary(self, section_body, report_context, peer_sections=None):
        return ExecutiveSummaryBuilder.build(report_context, existing_body=section_body, peer_sections=peer_sections)

    @classmethod
    def sanitize_generated_report_text(cls, raw_text):
        sanitized = str(raw_text or "")
        sanitized = re.sub(r"(?<!\n)(===\s*[^\n=]+?\s*===)", r"\n\n\1", sanitized)
        sanitized = re.sub(r"\n?===\s*([^\n=]+?)\s*===\n?", lambda match: f"\n\n### {match.group(1).strip()}\n", sanitized)
        sanitized = re.sub(r"[ \t]+\n", "\n", sanitized)
        sanitized = re.sub(r"\n{3,}", "\n\n", sanitized)
        return sanitized.strip()

    def finalize(self, raw_text, report_context, macro_osint, analysis_payload=None):
        raw_text = self.sanitize_generated_report_text(reader_safe_text(raw_text))
        sections = self.split_top_level_sections(raw_text)
        if not sections:
            return raw_text

        chart_marker, flow_marker = self.extract_visual_markers(report_context.get("visual_prompt", ""))
        dashboard_markers = self.build_dashboard_visual_markers(analysis_payload)
        operational_snapshot = self.build_operational_snapshot_block(analysis_payload)
        evidence_block = PaymentEvidenceBuilder.to_markdown(report_context.get("agent_evidence_ledger") or [])
        peer_sections = [section for section in sections if section["title"] != "Ringkasan Eksekutif"]
        finalized_sections = []

        for section in sections:
            section_title = section["title"]
            section_body = section["body"]

            if section_title == "Ringkasan Eksekutif":
                section_body = self.build_executive_summary(section_body, report_context, peer_sections=peer_sections)
                if evidence_block:
                    section_body = f"{section_body.rstrip()}\n\n{evidence_block}".strip()
            elif section_title == "Analisis Deskriptif Cashflow":
                section_body = self.append_marker_block(section_body, chart_marker)
            elif section_title == "Analisis Diagnostik Cashflow":
                section_body = self.inject_subheading_block(
                    section_body,
                    "Konteks OSINT Pendukung",
                    macro_osint or "OSINT tidak dipakai karena tidak ada konteks eksternal yang cukup sebanding dengan profil perusahaan.",
                    before_subheading="Risiko dan Kontrol",
                )
            elif section_title == "Analisis Prediktif Cashflow":
                section_body = self.inject_subheading_block(
                    section_body,
                    "Snapshot Dashboard Operasional",
                    operational_snapshot,
                    before_subheading="Skenario 1-2 Kuartal",
                )
                if dashboard_markers:
                    section_body = self.inject_subheading_block(
                        section_body,
                        "Visual Dashboard Snapshot",
                        "\n".join(dashboard_markers),
                        before_subheading="Implikasi terhadap Arus Kas Masuk dan Keluar",
                    )
            elif section_title == "Rekomendasi Preskriptif":
                section_body = self.append_marker_block(section_body, flow_marker)

            finalized_sections.append({"title": section_title, "body": section_body})

        return reader_safe_text(self.join_top_level_sections(finalized_sections))

    def build_fallback_report(self, report_context, notes, analysis_context, macro_osint, analysis_payload=None, structured_context_block=""):
        chart_marker, flow_marker = self.extract_visual_markers(report_context.get("visual_prompt", ""))
        dashboard_markers = self.build_dashboard_visual_markers(analysis_payload)
        operational_snapshot = self.build_operational_snapshot_block(analysis_payload)
        focus_block = notes.strip() if notes and notes.strip() else "Tidak ada fokus tambahan dari pengguna."
        executive_summary = self.build_executive_summary("", report_context)

        lines = [
            "# Ringkasan Eksekutif",
            executive_summary,
            "",
            "# Analisis Deskriptif Cashflow",
            "### Snapshot Portofolio dan Konsentrasi Risiko",
            report_context["financial_summary"],
            "",
            "### Batasan Data dan Asumsi",
            report_context["assumptions"],
            "",
            "# Analisis Diagnostik Cashflow",
            "### Pola Hambatan Utama",
            report_context["diagnostic_breakdown"],
            "",
            "### Bukti Internal yang Mewakili",
            report_context["evidence"],
            "",
            "### Konteks OSINT Pendukung",
            macro_osint or "OSINT tidak dipakai karena tidak ada konteks eksternal yang cukup sebanding dengan profil perusahaan.",
            "",
            "### Risiko dan Kontrol",
            report_context["controls"],
            "",
            "### Fokus Pengguna",
            f"- {focus_block}",
        ]

        if structured_context_block:
            lines.extend(
                [
                    "",
                    "### Parameter Proyeksi dan Ruang Lingkup",
                    structured_context_block,
                ]
            )

        if chart_marker:
            lines.extend(["", chart_marker])

        lines.extend(
            [
                "",
                "# Analisis Prediktif Cashflow",
                "### Dasar Proyeksi",
                "- Proyeksi menggunakan pendekatan risk-adjusted berdasarkan campuran kelas pembayaran historis, sehingga hasil harus dibaca sebagai skenario manajemen, bukan kepastian arus kas masuk.",
                "- Base case mewakili perilaku penagihan yang paling mungkin terjadi bila pola historis bertahan, sedangkan upside dan downside menunjukkan ruang perbaikan atau penurunan.",
                "",
                "### Snapshot Dashboard Operasional",
                operational_snapshot or "- Snapshot dashboard operasional belum tersedia pada saat laporan dibentuk.",
                "",
                "### Skenario 1-2 Kuartal",
                report_context["scenario_table"],
                "",
                "### Implikasi terhadap Arus Kas Masuk dan Keluar",
                report_context["cash_plan_implications"],
                "",
                "# Rekomendasi Preskriptif",
                "### Prinsip Tindakan",
                "1. Dahulukan tagihan bernilai besar dengan skor risiko tinggi dan penyebab yang masih bisa dipulihkan dalam 30 hari.",
                "2. Pisahkan perlakuan untuk isu anggaran, persetujuan, administrasi, likuiditas, sengketa, dan kewajiban jatuh tempo agar tindakan arus masuk dan arus keluar tidak tercampur.",
                "3. Gunakan bukti internal dan jadwal tindak lanjut yang terdokumentasi agar eskalasi ke manajemen klien lebih kuat.",
                "",
                "### Prasyarat Implementasi",
                report_context["implementation_prerequisites"],
                "",
                "### Kesiapan Pelaksanaan",
                report_context["organizational_readiness"],
                "",
                "# Prioritas Tindakan 30 Hari",
                "### Tabel Prioritas",
                report_context["priority_table"],
                "",
                "### Catatan Pelaksanaan",
                "- Tetapkan owner utama per akun prioritas dan review statusnya minimal mingguan.",
                "- Gunakan rapat internal untuk memastikan hambatan administratif dan eskalasi ke klien ditutup dengan tenggat yang jelas.",
                "- Konsolidasikan hasil follow-up ke finance collection, account owner, dan sponsor bisnis agar keputusan rapat langsung dapat dieksekusi.",
            ]
        )

        if dashboard_markers:
            dashboard_insert_index = lines.index("### Implikasi terhadap Arus Kas Masuk dan Keluar") + 2
            dashboard_block = ["", "### Visual Dashboard Snapshot", *dashboard_markers, ""]
            lines[dashboard_insert_index:dashboard_insert_index] = dashboard_block

        if flow_marker:
            lines.extend(["", flow_marker])

        return reader_safe_text("\n".join(lines))
