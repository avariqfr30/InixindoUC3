import unittest

from report_finalization import ReportFinalizer
from report_quality import ReportQualityScorer


def analysis_payload():
    return {
        "forecast": {
            "dashboard_snapshot": {
                "decision_queue": [
                    {
                        "priority": 1,
                        "name": "BUMN - Pelatihan",
                        "amount": 850_000_000,
                        "days_overdue": 130,
                        "action": "Eskalasi senior dan minta komitmen pembayaran tertulis.",
                    }
                    ,
                    {
                        "priority": 2,
                        "name": "Instansi - Konsultasi",
                        "amount": 250_000_000,
                        "days_overdue": 45,
                        "action": "Konfirmasi dokumen, owner approval, dan tanggal bayar.",
                    },
                ]
            }
        }
    }


class FinanceAccountabilityTests(unittest.TestCase):
    def test_builds_reader_visible_action_contract_from_dashboard_queue(self):
        table = ReportFinalizer().build_finance_action_table(analysis_payload())

        self.assertIn("Tagihan/Akun", table)
        self.assertIn("Terlambat 130 hari", table)
        self.assertIn("Koordinator Penagihan Keuangan", table)
        self.assertIn("Penanggung Jawab Akun", table)
        self.assertIn("penanggung jawab persetujuan", table)
        self.assertIn("2 hari kerja", table)
        self.assertIn("komitmen pembayaran tertulis", table)
        self.assertIn("Kontrol Tindak Lanjut", table)
        self.assertIn("Belum terlambat", ReportFinalizer().build_finance_action_table({
            "forecast": {"dashboard_snapshot": {"decision_queue": [{"name": "Akun lancar", "days_overdue": 0}]}}
        }))
        self.assertNotIn("Account Owner", table)
        self.assertNotIn("owner approval", table)

    def test_finalizer_replaces_weak_priority_content_with_action_contract(self):
        raw = """# Ringkasan Eksekutif
Isi ringkas yang cukup panjang untuk pengujian finalisasi dokumen.

# Prioritas Tindakan 30 Hari
### Tabel Prioritas
Prioritas mengikuti akun terbesar.
"""

        result = ReportFinalizer().finalize(raw, {}, "", analysis_payload())

        self.assertEqual(1, result.count("| Prioritas | Tagihan/Akun |"))
        self.assertNotIn("Prioritas mengikuti akun terbesar.", result)

    def test_final_qa_rejects_missing_finance_action_contract(self):
        result = ReportQualityScorer().final_qa(
            "# Prioritas Tindakan 30 Hari\n### Tabel Prioritas\nPrioritas mengikuti akun terbesar."
        )

        self.assertIn("missing_finance_action_contract", result["categories"])


if __name__ == "__main__":
    unittest.main()
