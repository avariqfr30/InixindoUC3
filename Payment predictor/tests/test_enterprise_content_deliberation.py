import unittest

from payment_deliberation import PaymentDeliberationBuilder
from report_planning import PaymentSectionPlanner
from report_quality import ReportQualityScorer


class PaymentDeliberationTests(unittest.TestCase):
    def setUp(self):
        PaymentDeliberationBuilder.clear_cache()
        PaymentSectionPlanner.clear_cache()
        self.sections = [
            "Ringkasan Eksekutif",
            "Analisis Deskriptif Cashflow",
            "Analisis Diagnostik Cashflow",
            "Analisis Prediktif Cashflow",
            "Rekomendasi Preskriptif",
            "Prioritas Tindakan 30 Hari",
        ]
        self.report_context = {
            "financial_summary": "Outstanding Rp533.862.500 dengan cash tersedia Rp500.000.000.",
            "management_brief": "Likuiditas dan kepastian tanggal bayar perlu dijaga.",
            "agent_evidence_ledger": [
                {
                    "claim": "Outstanding aktif bernilai Rp533.862.500.",
                    "allowed_use": "Gunakan sebagai dasar risiko koleksi.",
                    "confidence": "high",
                }
            ],
            "osint_dossier": {"quality": {"usable": False}, "evidence_cards": []},
        }
        self.analysis_payload = {
            "selected_period": {"label": "1-10 Juni 2026"},
            "forecast": {
                "dashboard_snapshot": {
                    "decision_queue": [
                        {
                            "priority": 1,
                            "name": "Husky-CNOOC Madura Limited - Consulting",
                            "amount": 221_445_000,
                            "days_overdue": 45,
                            "action": "Konfirmasi dokumen dan tanggal bayar.",
                        }
                    ],
                    "projection_defensibility": {
                        "sensitivity": ["Tanggal bayar akun prioritas memengaruhi ending cash."],
                        "challenge_checks": ["Validasi komitmen bayar tertulis."],
                    },
                },
                "formula": "Opening Cash + Cash In - Cash Out = Ending Cash",
            },
            "sync_status": {
                "financialData": {"recordCount": 4035, "syncStatus": "ready"},
                "cashOutSource": {"recordCount": 20937, "syncStatus": "ready", "configured": True},
            },
        }

    def test_builds_lifecycle_scenario_and_editorial_contracts(self):
        builder = PaymentDeliberationBuilder()
        contract = builder.build(
            self.sections,
            self.report_context,
            self.analysis_payload,
            data_version="finance-v1",
        )

        self.assertEqual(
            {
                "cache_key", "data_version", "evidence_dossier", "research_plan",
                "document_thesis", "chapter_contracts", "claim_ledger",
                "data_gap_register", "editorial_contract", "appendix_manifest",
            },
            set(contract),
        )
        self.assertEqual("Ringkasan Eksekutif", contract["chapter_contracts"][1]["depends_on"])
        self.assertTrue(any("lunas" in item["question"].lower() for item in contract["research_plan"]["questions"]))
        self.assertIn("Rp533.862.500", contract["evidence_dossier"]["protected_facts"])
        self.assertIn("meaning_lock", contract["editorial_contract"])
        self.assertEqual(contract, builder.build(self.sections, self.report_context, self.analysis_payload, data_version="finance-v1"))
        self.assertEqual(1, builder.cache_stats()["hits"])

    def test_builds_calculation_sensitivity_lifecycle_and_gap_appendices(self):
        builder = PaymentDeliberationBuilder()
        contract = builder.build(self.sections, self.report_context, self.analysis_payload, data_version="finance-v1")
        appendix = builder.build_appendix_markdown(contract)

        self.assertIn("# Lampiran Dasar Perhitungan, Sensitivitas, dan Kesenjangan Data", appendix)
        self.assertIn("## A. Dasar Perhitungan dan Cakupan", appendix)
        self.assertIn("## B. Sensitivitas dan Countercheck", appendix)
        self.assertIn("## C. Rekonsiliasi Siklus Tagihan", appendix)
        self.assertIn("## D. Kesenjangan Data", appendix)
        self.assertNotIn("InvoiceTraining", appendix)
        self.assertNotIn("DOCUMENT_DELIBERATION", appendix)

    def test_planner_and_quality_gate_share_the_contract(self):
        builder = PaymentDeliberationBuilder()
        contract = builder.build(self.sections, self.report_context, self.analysis_payload, data_version="finance-v1")
        plan = PaymentSectionPlanner().build_plan(
            self.sections,
            {**self.report_context, "document_contract": contract},
        )
        appendix = builder.build_appendix_markdown(contract)
        accepted = ReportQualityScorer().final_qa(
            appendix,
            deliberation_contract=contract,
        )
        rejected = ReportQualityScorer().final_qa(
            "# Lampiran Dasar Perhitungan, Sensitivitas, dan Kesenjangan Data\nBelum lengkap.",
            deliberation_contract=contract,
        )

        self.assertEqual(contract["document_thesis"], plan["document_thesis"])
        self.assertNotIn("missing_tiered_appendix", accepted["categories"])
        self.assertIn("missing_tiered_appendix", rejected["categories"])


if __name__ == "__main__":
    unittest.main()
