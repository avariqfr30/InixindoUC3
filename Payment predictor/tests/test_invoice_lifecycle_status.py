import unittest
from datetime import datetime

import pandas as pd

from data_contract import invoice_lifecycle_status, normalize_financial_dataframe
from financial_analyzer import FinancialAnalyzer
from forecast_engine import CashflowForecaster, _is_settled_status, _is_unsettled_status


class InvoiceLifecycleStatusTest(unittest.TestCase):
    def test_invoice_lunas_phrase_is_settled_status(self):
        self.assertTrue(_is_settled_status("Invoice Lunas"))
        self.assertFalse(_is_unsettled_status("Invoice Lunas"))

    def test_paid_date_wins_over_non_partial_unsettled_status(self):
        frame = pd.DataFrame([
            {
                "Periode Laporan": "2023-11-08",
                "Tipe Partner": "Institut Teknologi Nasional Yogyakarta",
                "Layanan": "InvoiceTraining",
                "Kelas Pembayaran": "Kelas A",
                "Nilai Invoice": "9990000",
                "Tanggal Invoice": "2023-11-08",
                "Tanggal Jatuh Tempo Invoice": "2023-11-18",
                "Tanggal Bayar Invoice": "2023-11-13",
                "Status Pembayaran Invoice": "no",
            }
        ])

        forecaster = CashflowForecaster(monthly_operating_cost_idr=200_000_000)
        invoices = forecaster._parse_invoices(
            frame,
            start_date=datetime(2026, 6, 9),
            end_date=datetime(2026, 6, 9),
        )

        self.assertEqual(invoices, [])

    def test_normalization_reconciles_unsettled_status_when_paid_date_is_valid(self):
        frame = pd.DataFrame([
            {
                "invoice_number": "INV-PAID-001",
                "invoice_company_name": "PT Contoh",
                "invoice_date": "2026-06-04",
                "invoice_due_date": "2026-06-14",
                "invoice_paid_date": "2026-06-08",
                "invoice_is_settled": "no",
                "invoice_amount": "1000000",
                "payment_class": "Kelas A",
                "source_dataset_label": "Training",
            }
        ])

        normalized, _ = normalize_financial_dataframe(frame)

        self.assertEqual(normalized.loc[0, "Status Pembayaran Invoice"], "Invoice Lunas")

    def test_paid_date_before_invoice_does_not_override_unsettled_status(self):
        lifecycle = invoice_lifecycle_status(
            raw_status="no",
            paid_date="2026-05-31",
            invoice_date="2026-06-04",
            due_date="2026-06-14",
        )

        self.assertFalse(lifecycle["is_settled"])
        self.assertTrue(lifecycle["is_unsettled"])
        self.assertEqual(lifecycle["conflict_reason"], "paid_date_before_invoice_date")

    def test_partial_status_remains_open_even_with_paid_date(self):
        lifecycle = invoice_lifecycle_status(
            raw_status="Invoice Terbayar Sebagian",
            paid_date="2026-06-08",
            invoice_date="2026-06-04",
            due_date="2026-06-14",
        )

        self.assertTrue(lifecycle["is_partial"])
        self.assertTrue(lifecycle["is_unsettled"])
        self.assertFalse(lifecycle["is_settled"])

    def test_outstanding_excludes_invoice_lunas_even_with_late_payment_class(self):
        frame = pd.DataFrame([
            {
                "Periode Laporan": "2026-05-22",
                "Tipe Partner": "Dinas Kominfo Kota Semarang",
                "Layanan": "InvoiceConsultant",
                "Kelas Pembayaran": "Kelas B (Telat 1-14 hari)",
                "Nilai Invoice": "282273000",
                "Tanggal Invoice": "2026-05-22",
                "Tanggal Jatuh Tempo Invoice": "2026-06-01",
                "Tanggal Bayar Invoice": "2026-06-08",
                "Status Pembayaran Invoice": "Invoice Lunas",
            },
            {
                "Periode Laporan": "2026-06-02",
                "Tipe Partner": "Husky-CNOOC Madura Limited",
                "Layanan": "InvoiceConsultant",
                "Kelas Pembayaran": "Kelas C",
                "Nilai Invoice": "221445000",
                "Tanggal Invoice": "2026-06-02",
                "Tanggal Jatuh Tempo Invoice": "2026-06-02",
                "Tanggal Bayar Invoice": "",
                "Status Pembayaran Invoice": "Invoice Dibuat",
            },
        ])

        forecaster = CashflowForecaster(monthly_operating_cost_idr=200_000_000)
        invoices = forecaster._parse_invoices(
            frame,
            start_date=datetime(2026, 6, 9),
            end_date=datetime(2026, 6, 9),
        )
        outstanding = forecaster._analyze_outstanding(invoices)

        self.assertEqual(len(invoices), 1)
        self.assertEqual(invoices[0]["partner_type"], "Husky-CNOOC Madura Limited")
        self.assertEqual(outstanding["total_outstanding"], 221445000)

    def test_report_context_separates_settled_late_history_from_current_risk(self):
        frame = pd.DataFrame([
            {
                "Periode Laporan": "2026-05-22",
                "Tipe Partner": "Dinas Kominfo Kota Semarang",
                "Layanan": "InvoiceConsultant",
                "Kelas Pembayaran": "Kelas B (Telat 1-14 hari)",
                "Nilai Invoice": "282273000",
                "Catatan Historis Keterlambatan": "Invoice dibayar 7 hari setelah jatuh tempo.",
                "Tanggal Invoice": "2026-05-22",
                "Tanggal Jatuh Tempo Invoice": "2026-06-01",
                "Tanggal Bayar Invoice": "2026-06-08",
                "Status Pembayaran Invoice": "Invoice Lunas",
            },
            {
                "Periode Laporan": "2026-06-02",
                "Tipe Partner": "Husky-CNOOC Madura Limited",
                "Layanan": "InvoiceConsultant",
                "Kelas Pembayaran": "Kelas C",
                "Nilai Invoice": "221445000",
                "Catatan Historis Keterlambatan": "Invoice belum tercatat lunas pada data internal.",
                "Tanggal Invoice": "2026-06-02",
                "Tanggal Jatuh Tempo Invoice": "2026-06-02",
                "Tanggal Bayar Invoice": "",
                "Status Pembayaran Invoice": "Invoice Dibuat",
            },
        ])

        context = FinancialAnalyzer.build_report_context(frame, data_mode="production")
        base = context["base_profile"]

        self.assertEqual(base["total_invoices"], 2)
        self.assertEqual(base["settled_invoices"], 1)
        self.assertEqual(base["open_invoices"], 1)
        self.assertEqual(base["delayed_invoices"], 1)
        self.assertEqual(base["delayed_invoice_value"], 221445000)
        self.assertEqual(base["settled_late_invoices"], 1)
        self.assertEqual(base["settled_late_invoice_value"], 282273000)
        self.assertNotIn("Dinas Kominfo Kota Semarang", base["top_risk_partners"])


if __name__ == "__main__":
    unittest.main()
