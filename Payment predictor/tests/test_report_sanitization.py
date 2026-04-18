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


if __name__ == "__main__":
    unittest.main()
