import sys
import unittest
from pathlib import Path


WORKSPACE = Path("/Users/avariqfr30/Documents/InixindoUC3/Payment predictor")
sys.path.insert(0, str(WORKSPACE))


class ReportSanitizationTest(unittest.TestCase):
    def test_internal_note_trimming_removes_ellipsis_artifacts(self):
        from core import FinancialAnalyzer

        note = (
            "Keputusan... masih menunggu dokumen final dan approval direksi untuk pencairan termin "
            "berikutnya agar invoice bisa diproses."
        )

        cleaned = FinancialAnalyzer._trim_note_for_report(note, max_length=120)

        self.assertNotIn("...", cleaned)
        self.assertIn("Keputusan", cleaned)
        self.assertLessEqual(len(cleaned), 120)

    def test_osint_relevance_gate_rejects_non_comparable_signal(self):
        from core import Researcher

        context = "partner pemerintah, bumn, layanan pelatihan dan sertifikasi"
        comparable_entry = {
            "title": "Vendor pelatihan BUMN hadapi siklus approval pembayaran",
            "snippet": "Pembayaran invoice vendor jasa pelatihan di BUMN tertunda karena approval dan dokumen kontrak.",
            "domain": "kontan.co.id",
        }
        unrelated_entry = {
            "title": "Harga minyak global naik tajam",
            "snippet": "Pasar komoditas bergerak karena sentimen Timur Tengah.",
            "domain": "example.com",
        }

        self.assertTrue(Researcher._is_company_comparable_entry(comparable_entry, context))
        self.assertFalse(Researcher._is_company_comparable_entry(unrelated_entry, context))

    def test_osint_filter_ranks_authoritative_comparable_sources(self):
        from core import Researcher

        context = "partner pemerintah, bumn, layanan pelatihan dan invoice termin"
        entries = [
            {
                "title": "Vendor pelatihan menunggu approval pembayaran invoice",
                "snippet": "Pembayaran invoice vendor jasa pelatihan BUMN tertunda karena approval dokumen.",
                "domain": "blog-random.example",
            },
            {
                "title": "Aturan pengadaan dan termin pembayaran penyedia pemerintah",
                "snippet": "Penyedia pelatihan perlu melengkapi BAST agar pembayaran invoice termin dapat diproses.",
                "domain": "lkpp.go.id",
            },
        ]

        filtered = Researcher._filter_company_comparable_entries(entries, context)

        self.assertEqual(filtered[0]["domain"], "lkpp.go.id")
        self.assertGreater(filtered[0]["relevance_score"], filtered[1]["relevance_score"])

    def test_operational_snapshot_includes_cash_in_and_cash_out(self):
        from core import ReportGenerator

        payload = {
            "selected_period": {"label": "1 Januari 2026 - 31 Maret 2026"},
            "cash_on_hand": 500_000_000,
            "sync_status": {
                "financialData": {"syncStatus": "ready", "sourceAgeMinutes": 12.5, "recordCount": 49},
                "cashOutSource": {"configured": False},
            },
            "horizon_snapshot": {
                "forecasts": {
                    "short_term": {
                        "time_horizon": {"label": "Short Term (0-30 hari)", "focus": "Likuiditas"},
                        "forecast": {
                            "cash_in": {"total_predicted_cash_in": 1_200_000_000},
                            "cash_out": {"total_cash_out": 300_000_000},
                            "ending_cash": 1_400_000_000,
                        },
                        "cashflow_health": {"runway_months": 4.2, "coverage_ratio": 4.0},
                    }
                }
            },
        }

        snapshot = ReportGenerator._build_operational_snapshot_block(payload)

        self.assertIn("cash masuk", snapshot)
        self.assertIn("cash keluar", snapshot)
        self.assertIn("ending cash", snapshot)

    def test_finalized_report_keeps_visual_dashboard_snapshot_subheading(self):
        from core import ReportGenerator

        generator = ReportGenerator(None)
        raw_text = "\n\n".join(
            [
                "# Ringkasan Eksekutif\nRingkas.",
                "# Analisis Deskriptif Cashflow\nDeskriptif.",
                "# Analisis Diagnostik Cashflow\nDiagnostik.",
                "# Analisis Prediktif Cashflow\n### Dasar Proyeksi\nPrediksi inti.",
                "# Rekomendasi Preskriptif\nRekomendasi.",
                "# Prioritas Tindakan 30 Hari\nPrioritas.",
            ]
        )
        analysis_payload = {
            "horizon_snapshot": {
                "forecasts": {
                    "short_term": {
                        "dashboard_snapshot": {
                            "horizon_key": "short_term",
                            "horizon_label": "Short Term (0-30 hari)",
                            "horizon_focus": "Likuiditas",
                            "status": "AMAN",
                            "current_cash": 500000000,
                            "runway_months": 2.5,
                            "coverage_ratio": 1.8,
                            "average_delay_days": 35,
                            "balance_projection_30d": [],
                            "coverage_chart": {"bars": []},
                        }
                    }
                }
            }
        }
        finalized = generator._finalize_report_content(
            raw_text=raw_text,
            report_context={"visual_prompt": ""},
            macro_osint="-",
            analysis_payload=analysis_payload,
        )

        self.assertIn("### Visual Dashboard Snapshot", finalized)
        self.assertIn("[[DASHBOARD:", finalized)

    def test_docx_table_generation_uses_compact_formatted_tables(self):
        from docx import Document
        from core import DocumentBuilder

        document = Document()
        markdown_text = (
            "| Prioritas | Fokus | Dampak |\n"
            "| --- | --- | --- |\n"
            "| 1 | Invoice BUMN | Cash in lebih cepat |\n"
        )

        DocumentBuilder.process_content(document, markdown_text)

        self.assertEqual(len(document.tables), 1)
        table = document.tables[0]
        self.assertEqual(table.alignment, 1)
        self.assertTrue(table.rows[0].cells[0].paragraphs[0].runs[0].bold)
        self.assertLessEqual(table.rows[1].cells[0].paragraphs[0].paragraph_format.space_after.pt, 2)

    def test_docx_body_text_is_left_aligned_and_ordered_lists_restart(self):
        from docx import Document
        from docx.enum.text import WD_ALIGN_PARAGRAPH
        from core import DocumentBuilder

        document = Document()
        DocumentBuilder.parse_html_to_docx(
            document,
            (
                "<p>Paragraf ringkas untuk laporan.</p>"
                "<ol><li>Prioritas pertama</li><li>Prioritas kedua</li></ol>"
                "<ol><li>Restart prioritas baru</li><li>Lanjutannya</li></ol>"
            ),
            (204, 0, 0),
        )

        paragraph_texts = [paragraph.text for paragraph in document.paragraphs if paragraph.text.strip()]

        self.assertEqual(document.paragraphs[0].alignment, WD_ALIGN_PARAGRAPH.LEFT)
        self.assertIn("1. Prioritas pertama", paragraph_texts)
        self.assertIn("2. Prioritas kedua", paragraph_texts)
        self.assertIn("1. Restart prioritas baru", paragraph_texts)
        self.assertIn("2. Lanjutannya", paragraph_texts)


if __name__ == "__main__":
    unittest.main()
