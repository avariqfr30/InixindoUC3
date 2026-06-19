import unittest
import json
from datetime import datetime
from pathlib import Path


class PaymentEditorialQualityTests(unittest.TestCase):
    def test_golden_fixture_contains_human_and_deterministic_closure_gates(self):
        fixture = json.loads((Path(__file__).parent / "fixtures" / "golden_payment_quality.json").read_text())

        self.assertTrue(all(score == 4 for score in fixture["expected"]["human_rubric_minimums"].values()))
        self.assertEqual(fixture["expected"]["max_repeated_nontrivial_cell_count"], 2)
        self.assertLessEqual(fixture["expected"]["max_table_cell_words"], 18)

    def test_policy_excludes_irrelevant_datasets_and_is_dashboard_first(self):
        from editorial_intelligence import EXCLUDED_DATASETS, payment_voice_rules

        self.assertEqual(EXCLUDED_DATASETS, {"FinanceInvoice", "ProjectStandards"})
        rules = " ".join(payment_voice_rules()).lower()
        self.assertIn("dashboard", rules)
        self.assertIn("keputusan", rules)

    def test_repeated_owner_or_category_is_grouped_without_filler(self):
        from editorial_intelligence import compact_repeated_finance_cells

        rows = [["A", "Project Admin + Finance Collection", "Dokumen & administrasi"] for _ in range(3)]
        compacted = compact_repeated_finance_cells(rows)

        self.assertEqual(compacted[2][1], "")
        self.assertEqual(compacted[2][2], "")
        self.assertNotIn("sama dengan", " ".join(cell for row in compacted for cell in row).lower())

    def test_style_assessment_flags_formula_narration_and_repeated_openings(self):
        from editorial_intelligence import assess_payment_style

        result = assess_payment_style(
            "Berdasarkan dashboard, total invoice dihitung dari seluruh invoice.\n\n"
            "Berdasarkan dashboard, risiko dihitung dari invoice terlambat.\n\n"
            "Berdasarkan dashboard, prioritas dihitung dari nilai invoice."
        )

        self.assertFalse(result["passed"])
        self.assertIn("dashboard_restatement", result["findings"])
        self.assertIn("repeated_openings", result["findings"])

    def test_date_bounds_keep_invoice_and_bank_disbursement_coverage_separate(self):
        from forecast_routes import _format_date_bounds, _select_forecast_anchor

        bounds = {
            "source": "cached_apidog_dataset",
            "invoice": {
                "min_date": datetime(2021, 1, 4),
                "max_date": datetime(2026, 6, 12),
                "date_columns": ["Periode Laporan"],
                "record_count": 4030,
            },
            "bank_disbursement": {
                "min_date": datetime(2023, 1, 1),
                "max_date": datetime(2026, 6, 29),
                "date_columns": ["BankDisbursement.due_date"],
                "record_count": 20919,
            },
            "min_date": datetime(2021, 1, 4),
            "max_date": datetime(2026, 6, 29),
            "date_columns": ["Periode Laporan", "BankDisbursement.due_date"],
            "record_count": 24949,
            "freshness_gap_days": 17,
        }

        formatted = _format_date_bounds(bounds)

        self.assertEqual(formatted["invoice"]["end"], "2026-06-12")
        self.assertEqual(formatted["bank_disbursement"]["end"], "2026-06-29")
        self.assertEqual(formatted["combined"]["end"], "2026-06-29")
        self.assertEqual(formatted["freshness_gap_days"], 17)
        self.assertEqual(_select_forecast_anchor(bounds, "receivables"), datetime(2026, 6, 12))
        self.assertEqual(_select_forecast_anchor(bounds, "cash_out"), datetime(2026, 6, 29))
        self.assertEqual(_select_forecast_anchor(bounds, "integrated"), datetime(2026, 6, 29))

    def test_protected_editor_uses_dashboard_specific_style_findings(self):
        from writing_quality import ProtectedIndonesianEditor

        issues = ProtectedIndonesianEditor.local_template_issues(
            "Berdasarkan dashboard, total invoice dihitung dari seluruh invoice.\n\n"
            "Berdasarkan dashboard, risiko dihitung dari invoice terlambat.\n\n"
            "Berdasarkan dashboard, prioritas dihitung dari nilai invoice."
        )

        self.assertIn("dashboard_restatement", issues)
        self.assertIn("repeated_openings", issues)


if __name__ == "__main__":
    unittest.main()
