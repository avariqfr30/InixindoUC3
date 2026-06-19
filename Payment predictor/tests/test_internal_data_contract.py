import io
import json
import os
import shutil
import sys
import tempfile
import unittest
from unittest import mock
from pathlib import Path

import pandas as pd


WORKSPACE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WORKSPACE))

from data_contract import (
    enrich_invoice_records_with_payment_behavior,
    enrich_records_with_account_reference,
    extract_records_from_payload,
    get_internal_api_contract,
    normalize_records,
    normalize_financial_dataframe,
    parse_internal_api_field_map,
)
from dataset_catalog import invoice_dataset_codes, source_dataset_label
from internal_api_doctor import main as internal_api_doctor_main, run_production_source_doctor
from finance_api_clients import CashOutAPIClient, InternalAPIClient
from financial_analyzer import FinancialAnalyzer


class InternalDataContractUnitTest(unittest.TestCase):
    def test_placeholder_dataset_codes_do_not_change_invoice_dataset_union(self):
        self.assertEqual(invoice_dataset_codes(""), ("InvoiceTraining", "InvoiceConsultant"))
        self.assertEqual(invoice_dataset_codes("FinanceInvoice"), ("InvoiceTraining", "InvoiceConsultant"))
        self.assertEqual(invoice_dataset_codes("InvoiceKonsultan"), ("InvoiceTraining", "InvoiceConsultant"))

    def test_source_dataset_labels_are_reader_friendly(self):
        self.assertEqual(source_dataset_label("InvoiceTraining"), "Training")
        self.assertEqual(source_dataset_label("InvoiceConsultant"), "Consulting")
        self.assertEqual(source_dataset_label("FinanceInvoice"), "")

    def test_internal_api_doctor_json_cli_serializes_result(self):
        doctor_result = {
            "ok": True,
            "checks": [],
            "activation": {"activationReady": True, "handoverReady": True},
            "records": {"recordCount": 1},
            "nextSteps": [],
        }

        with mock.patch(
            "internal_api_doctor.run_production_source_doctor",
            return_value=doctor_result,
        ), mock.patch("sys.stdout", new_callable=io.StringIO) as stdout:
            exit_code = internal_api_doctor_main(["--json", "--preview-rows", "1"])

        self.assertEqual(exit_code, 0)
        self.assertEqual(json.loads(stdout.getvalue()), doctor_result)

    def test_normalize_records_flattens_nested_values_consistently(self):
        records = [
            {
                " period ": "Q1 2026",
                "partner": {"type": "BUMN", "region": "DIY"},
                "tags": ["BAST", "approval"],
            }
        ]

        data_frame = normalize_records(records)

        self.assertEqual(set(data_frame.columns), {"period", "partner_type", "partner_region", "tags"})
        self.assertEqual(data_frame.loc[0, "tags"], '["BAST", "approval"]')

    def test_reference_account_enrichment_fills_payment_class_for_matching_company(self):
        invoice_records = [
            {
                "reporting_period": "Mei 2026",
                "company_id": "8592",
                "company_name": "Astra Credit Companies",
                "produk_utama": "Pelatihan AI",
                "nominal_tagihan": "Rp 150.000.000",
            }
        ]
        account_records = [
            {
                "company_id": "8592",
                "company_name": "Astra Credit Companies",
                "company_category_name": "Tipe B",
                "company_category_desc": "Pelanggan tipe B cenderung melakukan pembayaran dalam jangka waktu sedang, yaitu 15-30 hari.",
            }
        ]

        enriched_records, enrichment_summary = enrich_records_with_account_reference(
            invoice_records,
            account_records,
        )
        raw_df = normalize_records(enriched_records)
        field_map = {
            "period": "reporting_period",
            "partner_type": "company_name",
            "service": "produk_utama",
            "invoice_value": "nominal_tagihan",
            "payment_class": "payment_class",
            "delay_note": "delay_note",
        }
        normalized_df, contract_summary = normalize_financial_dataframe(raw_df, explicit_field_map=field_map)

        self.assertEqual(enrichment_summary["matchedRecords"], 1)
        self.assertEqual(enrichment_summary["referenceDataset"], "ReferenceAccount")
        self.assertEqual(normalized_df.loc[0, "Kelas Pembayaran"], "Kelas B")
        self.assertIn("15-30 hari", normalized_df.loc[0, "Catatan Historis Keterlambatan"])
        self.assertTrue(contract_summary["isReady"])

    def test_invoice_training_rows_are_self_sufficient_financial_records(self):
        invoice_records = [
            {
                "invoice_number": "INV/OTI/IWIN/III/21/04041",
                "invoice_company_name": "PT Jasa Raharja Cabang Jambi",
                "invoice_date": "2021-03-30",
                "invoice_due_date": "2021-04-09",
                "invoice_amount": "2475000",
                "invoice_is_settled": "yes",
                "invoice_paid_date": "2021-04-20",
            }
        ]

        enriched_records, behavior_summary = enrich_invoice_records_with_payment_behavior(invoice_records)
        raw_df = normalize_records(enriched_records)
        normalized_df, contract_summary = normalize_financial_dataframe(raw_df)

        self.assertEqual(behavior_summary["invoiceBehaviorFilled"], 1)
        self.assertTrue(contract_summary["isReady"])
        self.assertEqual(normalized_df.loc[0, "Periode Laporan"], "2021-03-30")
        self.assertEqual(normalized_df.loc[0, "Tipe Partner"], "PT Jasa Raharja Cabang Jambi")
        self.assertEqual(normalized_df.loc[0, "Layanan"], "Training")
        self.assertEqual(normalized_df.loc[0, "Kelas Pembayaran"], "Kelas B (Telat 1-14 hari)")
        self.assertIn("dibayar 11 hari setelah jatuh tempo", normalized_df.loc[0, "Catatan Historis Keterlambatan"])

    def test_missing_paid_date_without_settlement_status_does_not_become_high_risk(self):
        invoice_records = [
            {
                "invoice_number": "INV-DRAFT-001",
                "invoice_company_name": "PT Contoh",
                "invoice_date": "2026-05-01",
                "invoice_due_date": "2026-05-30",
                "invoice_amount": "Rp 200.000.000",
            }
        ]

        enriched_records, behavior_summary = enrich_invoice_records_with_payment_behavior(invoice_records)
        raw_df = normalize_records(enriched_records)
        normalized_df, contract_summary = normalize_financial_dataframe(raw_df)
        context = FinancialAnalyzer.build_report_context(normalized_df)

        self.assertEqual(behavior_summary["invoiceBehaviorFilled"], 0)
        self.assertFalse(contract_summary["isReady"])
        self.assertIn("payment_class", contract_summary["missingRequiredFields"])
        self.assertNotIn("Kelas Pembayaran", normalized_df.columns)
        self.assertEqual(context["base_profile"]["delayed_invoices"], 0)
        self.assertEqual(context["base_profile"]["high_risk_invoices"], 0)

    def test_explicit_unsettled_invoice_uses_age_aware_risk_class(self):
        invoice_records = [
            {
                "invoice_number": "INV-OPEN-001",
                "invoice_company_name": "PT Contoh",
                "invoice_date": "2026-05-01",
                "invoice_due_date": "2026-05-30",
                "invoice_is_settled": "no",
                "invoice_amount": "Rp 200.000.000",
            }
        ]

        enriched_records, behavior_summary = enrich_invoice_records_with_payment_behavior(invoice_records)
        raw_df = normalize_records(enriched_records)
        normalized_df, _ = normalize_financial_dataframe(raw_df)
        context = FinancialAnalyzer.build_report_context(normalized_df)

        self.assertEqual(behavior_summary["invoiceBehaviorFilled"], 1)
        self.assertEqual(normalized_df.loc[0, "Kelas Pembayaran"], "Kelas C (belum lunas)")
        self.assertEqual(context["base_profile"]["delayed_invoices"], 1)
        self.assertEqual(context["base_profile"]["high_risk_invoices"], 0)

    def test_internal_client_unions_real_invoice_datasets_and_ignores_placeholder_body(self):
        profile = {
            "type": "json_api",
            "endpoint": {
                "url": "https://example.com/api/Resource/dataset",
                "method": "POST",
                "timeout": 20,
                "verify_ssl": True,
                "records_key": "data.dataset_result",
            },
            "request": {"body": {"dataset": "FinanceInvoice"}, "body_format": "form"},
        }
        calls = []

        def fake_request(method, url, **kwargs):
            calls.append(dict(kwargs.get("data") or {}))
            dataset = (kwargs.get("data") or {}).get("dataset")
            response = mock.Mock()
            response.status_code = 200
            response.headers = {}
            if dataset == "InvoiceTraining":
                response.json.return_value = {
                    "success": True,
                    "data": {
                        "dataset_result": [
                            {
                                "invoice_number": "INV-1",
                                "invoice_company_name": "Klien A",
                                "invoice_date": "2026-01-03",
                                "invoice_due_date": "2026-01-10",
                                "invoice_amount": "1000000",
                                "invoice_is_settled": "yes",
                                "invoice_paid_date": "2026-01-09",
                            }
                        ]
                    },
                }
                return response
            if dataset == "InvoiceConsultant":
                response.json.return_value = {"success": False, "code": 500, "message": "Dataset tidak ditemukan"}
                return response
            raise AssertionError(f"unexpected dataset {dataset}")

        client = InternalAPIClient(source_profile=profile)
        with mock.patch("requests.request", side_effect=fake_request):
            records, summary = client.fetch_invoice_records()

        self.assertEqual([call["dataset"] for call in calls], ["InvoiceTraining", "InvoiceConsultant"])
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["_source_dataset_code"], "InvoiceTraining")
        self.assertIn("InvoiceTraining", summary["loadedDatasets"])
        self.assertEqual(summary["unavailableDatasets"][0]["dataset"], "InvoiceConsultant")

    def test_cash_out_client_defaults_to_bank_disbursement_when_internal_api_is_configured(self):
        source_profile = {
            "type": "json_api",
            "endpoint": {
                "url": "https://example.com/api/Resource/dataset",
                "method": "POST",
                "timeout": 20,
                "verify_ssl": True,
                "records_key": "data.dataset_result",
            },
            "auth": {},
            "request": {"body": {"dataset": "FinanceInvoice"}, "body_format": "form"},
        }

        client = CashOutAPIClient(source_profile=source_profile)

        self.assertTrue(client.is_configured())
        self.assertEqual(client.body, {"dataset": "BankDisbursement", "dataset_cache": "enabled"})
        self.assertEqual(client.body_format, "form")

    def test_preview_source_profile_returns_normalized_contract_summary(self):
        from source_preview_service import preview_source_profile

        profile = {
            "type": "json_api",
            "endpoint": {
                "url": "https://example.com/api/Resource/dataset",
                "method": "POST",
                "timeout": 20,
                "verify_ssl": True,
                "records_key": "data.dataset_result",
            },
            "request": {"body": {"dataset": "FinanceInvoice"}, "body_format": "form"},
            "field_map": {
                "period": "reporting_period",
                "partner_type": "segmentasi_customer",
                "service": "produk_utama",
                "payment_class": "bucket_pembayaran",
                "invoice_value": "nominal_tagihan",
                "delay_note": "hambatan_penagihan",
            },
        }
        fake_response = mock.Mock()
        fake_response.status_code = 200
        fake_response.json.return_value = {
            "success": True,
            "data": {
                "dataset_result": [
                    {
                        "reporting_period": "Januari 2026",
                        "segmentasi_customer": "Instansi Pemerintah",
                        "produk_utama": "Audit SPBE",
                        "bucket_pembayaran": "Kelas B",
                        "nominal_tagihan": 275000000,
                        "hambatan_penagihan": "Dokumen termin menunggu approval internal.",
                    }
                ]
            },
        }

        with mock.patch("requests.request", return_value=fake_response):
            preview = preview_source_profile(profile, preview_rows=5)

        self.assertTrue(preview["contractSummary"]["isReady"])
        self.assertEqual(preview["recordCount"], 1)
        self.assertEqual(preview["sampleRecords"][0]["reporting_period"], "Januari 2026")
        self.assertEqual(preview["extractionSummary"]["resolvedRecordsPath"], "data.dataset_result")

    def test_normalize_dataframe_with_explicit_field_map(self):
        raw_df = pd.DataFrame(
            [
                {
                    "report_period": "Q1 2026",
                    "customer_segment": "Instansi Pemerintah",
                    "service_name": "Audit SPBE",
                    "collection_bucket": "Kelas C (Telat 1-2 Bulan)",
                    "amount_idr": "Rp 180.000.000",
                    "delay_reason": "Dokumen termin belum lengkap.",
                }
            ]
        )
        field_map = parse_internal_api_field_map(
            json.dumps(
                {
                    "period": "report_period",
                    "partner_type": "customer_segment",
                    "service": "service_name",
                    "payment_class": "collection_bucket",
                    "invoice_value": "amount_idr",
                    "delay_note": "delay_reason",
                }
            )
        )

        normalized_df, summary = normalize_financial_dataframe(raw_df, explicit_field_map=field_map)

        self.assertIn("Periode Laporan", normalized_df.columns)
        self.assertIn("Tipe Partner", normalized_df.columns)
        self.assertIn("Layanan", normalized_df.columns)
        self.assertIn("Kelas Pembayaran", normalized_df.columns)
        self.assertIn("Nilai Invoice", normalized_df.columns)
        self.assertIn("Catatan Historis Keterlambatan", normalized_df.columns)
        self.assertTrue(summary["isReady"])
        self.assertEqual(summary["missingRequiredFields"], [])

    def test_contract_payload_contains_mapping_template(self):
        contract = get_internal_api_contract()
        self.assertEqual(contract["fieldMapEnvVar"], "INTERNAL_API_FIELD_MAP_JSON")
        self.assertEqual(contract["endpointUrlEnvVar"], "INTERNAL_API_ENDPOINT_URL")
        self.assertEqual(contract["profileConfigFileEnvVar"], "INTERNAL_API_CONFIG_FILE")
        self.assertIn("period", contract["fieldMapTemplate"])
        self.assertIn("recommendedProductionProfile", contract)
        self.assertIn("handoverChecklist", contract)
        self.assertIn("records", contract["exampleResponse"])

    def test_extract_records_and_infer_fields_from_nested_json(self):
        payload = {
            "meta": {"source": "internal finance"},
            "payload": {
                "data": [
                    {
                        "reporting_period": "Januari 2026",
                        "segmentasi_customer": "Instansi Pemerintah",
                        "produk_utama": "Audit SPBE",
                        "bucket_pembayaran": "Kelas B (Telat 1-2 Minggu)",
                        "nominal_tagihan": 275000000,
                        "hambatan_penagihan": "Dokumen termin menunggu approval internal.",
                    },
                    {
                        "reporting_period": "Februari 2026",
                        "segmentasi_customer": "Swasta (Tech Startup)",
                        "produk_utama": "Pelatihan AI",
                        "bucket_pembayaran": "Kelas C (Telat 1-2 Bulan)",
                        "nominal_tagihan": 120000000,
                        "hambatan_penagihan": "Customer meminta penjadwalan ulang invoice.",
                    },
                ]
            },
        }

        records, extraction_summary = extract_records_from_payload(payload)
        self.assertEqual(extraction_summary["resolvedRecordsPath"], "$.payload.data")
        raw_df = pd.json_normalize(records, sep="_")
        normalized_df, summary = normalize_financial_dataframe(raw_df)

        self.assertIn("Periode Laporan", normalized_df.columns)
        self.assertIn("Tipe Partner", normalized_df.columns)
        self.assertIn("Layanan", normalized_df.columns)
        self.assertIn("Kelas Pembayaran", normalized_df.columns)
        self.assertIn("Nilai Invoice", normalized_df.columns)
        self.assertTrue(summary["isReady"])
        self.assertIn("fieldMapSuggestionJson", summary)
        self.assertEqual(summary["semanticAdapter"], "finance_invoice_v1")

    def test_normalize_dataframe_naturalizes_internal_api_values_for_reports(self):
        raw_df = pd.DataFrame(
            [
                {
                    "report_period": "Q1 2026",
                    "customer_segment": "INSTANSI PEMERINTAH",
                    "service_name": "AUDIT SPBE",
                    "collection_bucket": "KELAS C - TELAT 1-2 BULAN",
                    "amount_idr": "Rp 180.000.000",
                    "delay_reason": "DOKUMEN TERMIN MENUNGGU APPROVAL INTERNAL.",
                }
            ]
        )
        field_map = parse_internal_api_field_map(
            {
                "period": "report_period",
                "partner_type": "customer_segment",
                "service": "service_name",
                "payment_class": "collection_bucket",
                "invoice_value": "amount_idr",
                "delay_note": "delay_reason",
            }
        )

        normalized_df, summary = normalize_financial_dataframe(raw_df, explicit_field_map=field_map)

        self.assertEqual(normalized_df.loc[0, "Tipe Partner"], "Instansi Pemerintah")
        self.assertEqual(normalized_df.loc[0, "Layanan"], "Audit SPBE")
        self.assertEqual(normalized_df.loc[0, "Kelas Pembayaran"], "Kelas C (Telat 1-2 Bulan)")
        self.assertEqual(normalized_df.loc[0, "Catatan Historis Keterlambatan"], "Dokumen Termin Menunggu Approval Internal")
        self.assertTrue(summary["isReady"])

    def test_post_basic_auth_client_supports_body_json(self):
        old_env = {
            "INTERNAL_API_ENDPOINT_URL": os.environ.get("INTERNAL_API_ENDPOINT_URL"),
            "INTERNAL_API_METHOD": os.environ.get("INTERNAL_API_METHOD"),
            "INTERNAL_API_BASIC_USERNAME": os.environ.get("INTERNAL_API_BASIC_USERNAME"),
            "INTERNAL_API_BASIC_PASSWORD": os.environ.get("INTERNAL_API_BASIC_PASSWORD"),
            "INTERNAL_API_BODY_JSON": os.environ.get("INTERNAL_API_BODY_JSON"),
        }
        try:
            os.environ["INTERNAL_API_ENDPOINT_URL"] = "https://example.com/api/Resource/dataset"
            os.environ["INTERNAL_API_METHOD"] = "POST"
            os.environ["INTERNAL_API_BASIC_USERNAME"] = "demo-user"
            os.environ["INTERNAL_API_BASIC_PASSWORD"] = "demo-pass"
            os.environ["INTERNAL_API_BODY_JSON"] = json.dumps({"tag": "cashin"})

            for module_name in ("core", "config"):
                if module_name in sys.modules:
                    del sys.modules[module_name]

            import core as core_module

            fake_response = mock.Mock()
            fake_response.status_code = 200
            fake_response.raise_for_status.return_value = None
            fake_response.json.return_value = {
                "success": True,
                "code": 200,
                "message": "OK",
                "data": [
                    {
                        "period": "Q1 2026",
                        "partner_type": "Instansi Pemerintah",
                        "service": "Audit SPBE",
                        "payment_class": "Kelas B (Telat 1-2 Minggu)",
                        "invoice_value": "Rp 200.000.000",
                    }
                ],
            }

            with mock.patch.object(core_module.requests, "request", return_value=fake_response) as request_mock:
                client = core_module.InternalAPIClient()
                records, extraction_summary = client.fetch_records()

            self.assertEqual(records[0]["period"], "Q1 2026")
            self.assertEqual(extraction_summary["requestMethod"], "POST")
            self.assertEqual(extraction_summary["authMode"], "basic")
            request_mock.assert_called_once()
            _, kwargs = request_mock.call_args
            self.assertEqual(kwargs["auth"], ("demo-user", "demo-pass"))
            self.assertEqual(kwargs["json"], {"tag": "cashin"})
        finally:
            for key, value in old_env.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value
            for module_name in ("core", "config"):
                if module_name in sys.modules:
                    del sys.modules[module_name]

    def test_profile_client_supports_form_body_and_env_bearer_token(self):
        old_env = {"INTERNAL_API_AUTH_TOKEN": os.environ.get("INTERNAL_API_AUTH_TOKEN")}
        try:
            os.environ["INTERNAL_API_AUTH_TOKEN"] = "env-token"
            for module_name in ("core", "config", "finance_api_clients"):
                if module_name in sys.modules:
                    del sys.modules[module_name]

            import core as core_module

            fake_response = mock.Mock()
            fake_response.status_code = 200
            fake_response.raise_for_status.return_value = None
            fake_response.json.return_value = {
                "success": True,
                "data": [
                    {
                        "period": "Q1 2026",
                        "partner_type": "Instansi Pemerintah",
                        "service": "Audit SPBE",
                        "payment_class": "Kelas B",
                        "invoice_value": "Rp 200.000.000",
                    }
                ],
            }
            profile = {
                "type": "json_api",
                "endpoint": {
                    "url": "https://example.com/api/Resource/dataset",
                    "method": "POST",
                    "timeout": 20,
                    "verify_ssl": True,
                    "records_key": "",
                },
                "auth": {"bearer_token": "__ENV__"},
                "request": {
                    "body_format": "form",
                    "body": {"dataset": "FinanceInvoice"},
                },
            }

            with mock.patch.object(core_module.requests, "request", return_value=fake_response) as request_mock:
                client = core_module.InternalAPIClient(source_profile=profile)
                records, _ = client.fetch_records()

            self.assertEqual(records[0]["period"], "Q1 2026")
            _, kwargs = request_mock.call_args
            self.assertEqual(kwargs["headers"]["Authorization"], "Bearer env-token")
            self.assertEqual(kwargs["data"], {"dataset": "FinanceInvoice"})
            self.assertNotIn("json", kwargs)
        finally:
            if old_env["INTERNAL_API_AUTH_TOKEN"] is None:
                os.environ.pop("INTERNAL_API_AUTH_TOKEN", None)
            else:
                os.environ["INTERNAL_API_AUTH_TOKEN"] = old_env["INTERNAL_API_AUTH_TOKEN"]
            for module_name in ("core", "config", "finance_api_clients"):
                if module_name in sys.modules:
                    del sys.modules[module_name]

    def test_profile_client_supports_apidog_multipart_body(self):
        for module_name in ("core", "config", "finance_api_clients"):
            if module_name in sys.modules:
                del sys.modules[module_name]

        import core as core_module

        fake_response = mock.Mock()
        fake_response.status_code = 200
        fake_response.raise_for_status.return_value = None
        fake_response.json.return_value = {
            "success": True,
            "data": [
                {
                    "period": "Q1 2026",
                    "partner_type": "Instansi Pemerintah",
                    "service": "Audit SPBE",
                    "payment_class": "Kelas B",
                    "invoice_value": "Rp 200.000.000",
                }
            ],
        }
        profile = {
            "type": "json_api",
            "endpoint": {
                "url": "https://example.com/api/Resource/dataset",
                "method": "POST",
                "timeout": 20,
                "verify_ssl": True,
                "records_key": "",
            },
            "request": {
                "body_format": "multipart",
                "body": {"dataset": "FinanceInvoice", "dataset_cache": "enabled"},
            },
        }

        with mock.patch.object(core_module.requests, "request", return_value=fake_response) as request_mock:
            client = core_module.InternalAPIClient(source_profile=profile)
            records, _ = client.fetch_records()

        self.assertEqual(records[0]["period"], "Q1 2026")
        _, kwargs = request_mock.call_args
        self.assertEqual(kwargs["files"]["dataset"], (None, "FinanceInvoice"))
        self.assertEqual(kwargs["files"]["dataset_cache"], (None, "enabled"))
        self.assertNotIn("data", kwargs)
        self.assertNotIn("json", kwargs)

    def test_cash_out_client_fetch_records_uses_parent_transport_contract(self):
        for module_name in ("core", "config", "finance_api_clients"):
            if module_name in sys.modules:
                del sys.modules[module_name]

        import finance_api_clients

        fake_response = mock.Mock()
        fake_response.status_code = 200
        fake_response.raise_for_status.return_value = None
        fake_response.json.return_value = {
            "success": True,
            "data": [
                {
                    "amount": "Rp 45.000.000",
                    "due_date": "2026-04-20",
                    "category": "Vendor",
                    "status": "open",
                }
            ],
        }

        with mock.patch.object(finance_api_clients, "CASH_OUT_API_ENDPOINT_URL", "https://example.com/cash-out"), mock.patch.object(
            finance_api_clients, "CASH_OUT_API_METHOD", "POST"
        ), mock.patch.object(finance_api_clients, "CASH_OUT_API_BODY_JSON", json.dumps({"dataset": "CashOut"})), mock.patch(
            "requests.request", return_value=fake_response
        ) as request_mock:
            client = finance_api_clients.CashOutAPIClient()
            records, summary = client.fetch_records()

        self.assertEqual(records[0]["amount"], "Rp 45.000.000")
        self.assertEqual(summary["requestMethod"], "POST")
        _, kwargs = request_mock.call_args
        self.assertEqual(kwargs["json"], {"dataset": "CashOut"})

    def test_internal_client_fetches_reference_account_with_same_transport_shape(self):
        for module_name in ("core", "config", "finance_api_clients"):
            if module_name in sys.modules:
                del sys.modules[module_name]

        import finance_api_clients

        fake_response = mock.Mock()
        fake_response.status_code = 200
        fake_response.raise_for_status.return_value = None
        fake_response.json.return_value = {
            "success": True,
            "data": {
                "dataset_result": [
                    {
                        "company_id": "8592",
                        "company_name": "Astra Credit Companies",
                        "company_category_name": "Tipe B",
                    }
                ]
            },
        }
        profile = {
            "type": "json_api",
            "endpoint": {
                "url": "https://example.com/api/Resource/dataset",
                "method": "POST",
                "timeout": 20,
                "verify_ssl": True,
                "records_key": "data.dataset_result",
            },
            "request": {
                "body_format": "form",
                "body": {"dataset": "FinanceInvoice"},
            },
        }

        with mock.patch("requests.request", return_value=fake_response) as request_mock:
            client = finance_api_clients.InternalAPIClient(source_profile=profile)
            records, summary = client.fetch_reference_account_records(preview_limit=2)

        self.assertEqual(records[0]["company_category_name"], "Tipe B")
        self.assertEqual(summary["referenceDataset"], "ReferenceAccount")
        _, kwargs = request_mock.call_args
        self.assertEqual(kwargs["data"], {"dataset": "ReferenceAccount", "dataset_cache": "enabled"})
        self.assertEqual(client.body, {"dataset": "FinanceInvoice"})

    def test_internal_client_enables_apidog_cache_for_date_sensitive_dataset_clones(self):
        profile = {
            "type": "json_api",
            "endpoint": {
                "url": "https://example.com/api/Resource/dataset",
                "method": "POST",
                "timeout": 20,
                "verify_ssl": True,
                "records_key": "data.dataset_result",
            },
            "request": {
                "body_format": "form",
                "body": {"dataset": "FinanceInvoice", "dataset_cache": "enabled"},
            },
        }

        client = InternalAPIClient(source_profile=profile)
        invoice_client = client._clone_for_dataset("InvoiceTraining")

        self.assertEqual(invoice_client.body["dataset"], "InvoiceTraining")
        self.assertEqual(invoice_client.body["dataset_cache"], "enabled")
        self.assertEqual(client.body["dataset_cache"], "enabled")

    def test_internal_client_fetches_reference_account_with_dataset_code_replaced(self):
        for module_name in ("core", "config", "finance_api_clients"):
            if module_name in sys.modules:
                del sys.modules[module_name]

        import finance_api_clients

        fake_response = mock.Mock()
        fake_response.status_code = 200
        fake_response.raise_for_status.return_value = None
        fake_response.json.return_value = {
            "success": True,
            "data": {"dataset_result": [{"company_id": "8592"}]},
        }
        profile = {
            "type": "json_api",
            "endpoint": {
                "url": "https://example.com/api/Resource/dataset",
                "method": "POST",
                "timeout": 20,
                "verify_ssl": True,
                "records_key": "data.dataset_result",
            },
            "request": {
                "body_format": "form",
                "body": {
                    "dataset": "FinanceInvoice",
                    "dataset_code": "ClassReport",
                },
            },
        }

        with mock.patch("requests.request", return_value=fake_response) as request_mock:
            client = finance_api_clients.InternalAPIClient(source_profile=profile)
            client.fetch_reference_account_records(preview_limit=2)

        _, kwargs = request_mock.call_args
        self.assertEqual(
            kwargs["data"],
            {
                "dataset": "ReferenceAccount",
                "dataset_code": "ReferenceAccount",
                "dataset_cache": "enabled",
            },
        )
        self.assertEqual(
            client.body,
            {"dataset": "FinanceInvoice", "dataset_code": "ClassReport"},
        )

    def test_internal_loader_uses_reference_account_to_fill_missing_payment_class(self):
        from cashflow_analysis import KnowledgeBase

        class FakeInternalAPIClient:
            field_map = {
                "period": "reporting_period",
                "partner_type": "company_name",
                "service": "produk_utama",
                "payment_class": "payment_class",
                "invoice_value": "nominal_tagihan",
                "delay_note": "delay_note",
            }

            def __init__(self, source_profile=None):
                self.source_profile = source_profile

            def is_configured(self):
                return True

            def fetch_records(self):
                return [
                    {
                        "reporting_period": "Mei 2026",
                        "company_id": "8592",
                        "company_name": "Astra Credit Companies",
                        "produk_utama": "Pelatihan AI",
                        "nominal_tagihan": "Rp 150.000.000",
                    }
                ], {"resolvedRecordsPath": "data.dataset_result", "recordCount": 1}

            def fetch_reference_account_records(self):
                return [
                    {
                        "company_id": "8592",
                        "company_name": "Astra Credit Companies",
                        "company_category_name": "Tipe B",
                        "company_category_desc": "Pelanggan tipe B cenderung melakukan pembayaran dalam jangka waktu sedang, yaitu 15-30 hari.",
                    }
                ], {"referenceDataset": "ReferenceAccount", "recordCount": 1}

        knowledge_base = KnowledgeBase.__new__(KnowledgeBase)

        with mock.patch("cashflow_analysis.InternalAPIClient", FakeInternalAPIClient):
            normalized_df, summary = knowledge_base._load_internal_api_data(
                profile={"type": "json_api"}
            )

        self.assertEqual(normalized_df.loc[0, "Kelas Pembayaran"], "Kelas B")
        self.assertTrue(summary["isReady"])
        self.assertEqual(summary["referenceAccountEnrichment"]["matchedRecords"], 1)

    def test_production_source_doctor_reports_activation_readiness(self):
        profile = {
            "key": "production",
            "name": "Produksi API Internal",
            "mode": "production",
            "type": "json_api",
            "endpoint": {
                "url": "https://example.com/api/Resource/dataset",
                "method": "POST",
                "timeout": 20,
                "verify_ssl": True,
                "records_key": "data.dataset_result",
            },
            "auth": {"basic_username": "demo-user", "basic_password": "demo-pass"},
            "request": {"body": {"dataset": "FinanceInvoice"}, "body_format": "form"},
            "field_map": {
                "period": "reporting_period",
                "partner_type": "segmentasi_customer",
                "service": "produk_utama",
                "payment_class": "bucket_pembayaran",
                "invoice_value": "nominal_tagihan",
                "delay_note": "hambatan_penagihan",
            },
        }
        fake_response = mock.Mock()
        fake_response.status_code = 200
        fake_response.json.return_value = {
            "success": True,
            "data": {
                "dataset_result": [
                    {
                        "reporting_period": "Januari 2026",
                        "segmentasi_customer": "Instansi Pemerintah",
                        "produk_utama": "Audit SPBE",
                        "bucket_pembayaran": "Kelas B",
                        "nominal_tagihan": 275000000,
                        "hambatan_penagihan": "Dokumen termin menunggu approval internal.",
                    }
                ]
            },
        }

        with mock.patch("requests.head", return_value=mock.Mock(status_code=200)), mock.patch(
            "requests.request", return_value=fake_response
        ):
            result = run_production_source_doctor(source_profile=profile, preview_rows=5)

        self.assertTrue(result["ok"])
        self.assertTrue(result["activation"]["activationReady"])
        self.assertTrue(result["activation"]["handoverReady"])
        self.assertEqual(result["authShape"]["mode"], "basic")
        self.assertEqual(result["records"]["extractionSummary"]["resolvedRecordsPath"], "data.dataset_result")
        self.assertEqual(
            {check["name"]: check["status"] for check in result["checks"]}["field_mapping_readiness"],
            "pass",
        )

    def test_production_source_doctor_blocks_activation_when_required_fields_missing(self):
        profile = {
            "key": "production",
            "name": "Produksi API Internal",
            "mode": "production",
            "type": "json_api",
            "endpoint": {
                "url": "https://example.com/api/Resource/dataset",
                "method": "POST",
                "timeout": 20,
                "verify_ssl": True,
                "records_key": "data.dataset_result",
            },
            "auth": {"bearer_token": "test-token"},
            "request": {"body": {"dataset": "FinanceInvoice"}, "body_format": "form"},
        }
        fake_response = mock.Mock()
        fake_response.status_code = 200
        fake_response.json.return_value = {
            "success": True,
            "data": {
                "dataset_result": [
                    {
                        "reporting_period": "Januari 2026",
                        "segmentasi_customer": "Instansi Pemerintah",
                    }
                ]
            },
        }

        with mock.patch("requests.head", return_value=mock.Mock(status_code=200)), mock.patch(
            "requests.request", return_value=fake_response
        ):
            result = run_production_source_doctor(source_profile=profile, preview_rows=5)

        self.assertFalse(result["ok"])
        self.assertFalse(result["activation"]["activationReady"])
        self.assertIn("payment_class", result["fieldMapping"]["missingRequiredFields"])
        self.assertEqual(
            {check["name"]: check["status"] for check in result["checks"]}["field_mapping_readiness"],
            "fail",
        )


class InternalDataContractRouteTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._tmpdir = tempfile.mkdtemp(prefix="cashin-internal-contract-")
        os.environ["JOB_STATE_DB_PATH"] = os.path.join(cls._tmpdir, "jobs.db")
        os.environ["REPORT_ARTIFACTS_DIR"] = os.path.join(cls._tmpdir, "artifacts")
        os.environ["DATA_SOURCE_ACTIVE_STATE_PATH"] = os.path.join(cls._tmpdir, "active-source.json")
        os.environ["INTERNAL_API_CONFIG_FILE"] = os.path.join(cls._tmpdir, "production-source.json")
        os.environ["APP_SECRET_KEY"] = "test-secret-key"
        os.environ["SESSION_COOKIE_SECURE"] = "false"
        os.environ["DISABLE_CSRF_FOR_TESTING"] = "1"
        os.environ["TEMP_FULL_ACCESS_USERNAME"] = "contract_user@inixindojogja.co.id"
        os.environ["TEMP_FULL_ACCESS_PASSWORD"] = "password123"
        os.environ["REFERENCE_INTERNAL_ACCOUNT_LOOKUP_MODE"] = "test_double"
        os.environ["REFERENCE_INTERNAL_ACCOUNT_TEST_EMAILS"] = "contract_user@inixindojogja.co.id"
        os.environ["AUTH_SIGNUP_VERIFICATION_DELIVERY_MODE"] = "capture"

        for module_name in (
            "app",
            "config",
            "core",
            "finance_api_clients",
            "cashflow_analysis",
            "osint_research",
            "docx_rendering",
            "report_generation",
        ):
            if module_name in sys.modules:
                del sys.modules[module_name]

        import app as app_module

        cls.flask_app = app_module.create_app()
        cls.flask_app.testing = True

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls._tmpdir, ignore_errors=True)

    def setUp(self):
        self.client = self.flask_app.test_client()
        signup = self.client.post(
            "/signup",
            data={
                "username": "contract_user@inixindojogja.co.id",
                "password": "password123",
                "confirm_password": "password123",
            },
            follow_redirects=False,
        )
        if signup.status_code not in (302, 200, 400, 403):
            raise AssertionError(f"Unexpected signup status: {signup.status_code}")

        if signup.status_code in (200, 400, 403):
            login = self.client.post(
                "/login",
                data={"username": "contract_user@inixindojogja.co.id", "password": "password123"},
                follow_redirects=False,
            )
            if login.status_code != 302:
                raise AssertionError(f"Unexpected login status: {login.status_code}")

    def test_contract_endpoint_returns_summary(self):
        response = self.client.get("/api/internal-data/contract")
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertIn("currentSummary", payload)
        self.assertIn("fields", payload)
        self.assertTrue(payload["currentSummary"]["isReady"])

    def test_internal_api_connector_is_isolated_to_settings_page(self):
        template = (WORKSPACE / "templates" / "index.html").read_text(encoding="utf-8")
        self.assertNotIn("Sambungkan API Internal", template)
        self.assertNotIn("btn-connect-api", template)
        self.assertNotIn("api-connect", template)
        settings_template = (WORKSPACE / "templates" / "data_settings.html").read_text(encoding="utf-8")
        self.assertIn("Internal API / APIDog", settings_template)
        self.assertIn("Internal API Sudah Aktif", settings_template)
        self.assertIn("Muat Ulang Data Sekarang", settings_template)
        self.assertIn("btn-refresh-internal-api", settings_template)
        self.assertIn("/api/internal-api/refresh", settings_template)
        self.assertIn("setInternalApiConnectionState", settings_template)
        self.assertNotIn("Project data source", settings_template)
        self.assertNotIn("active.type", settings_template)

    def test_internal_api_settings_page_is_status_only_for_employees(self):
        settings_template = (WORKSPACE / "templates" / "data_settings.html").read_text(encoding="utf-8")

        self.assertIn("Konfigurasi API dikelola dari environment VPS", settings_template)
        self.assertIn("Internal API Sudah Aktif", settings_template)
        self.assertIn("Muat Ulang Data Sekarang", settings_template)
        self.assertNotIn("endpoint-url", settings_template)
        self.assertNotIn("auth-mode", settings_template)
        self.assertNotIn("body-format", settings_template)
        self.assertNotIn("records-key", settings_template)
        self.assertNotIn("field-map-json", settings_template)
        self.assertNotIn("btn-preview", settings_template)
        self.assertNotIn("/api/internal-data/connect", settings_template)

    def test_internal_api_refresh_endpoint_refreshes_active_api_source(self):
        with self.flask_app.app_context():
            original_refresh_coordinator = self.flask_app.config["refresh_coordinator"]
            original_knowledge_base = self.flask_app.config["knowledge_base"]

            fake_refresh_coordinator = mock.Mock()
            fake_refresh_coordinator.refresh_all.return_value = {
                "knowledgeBase": True,
                "cashOutSource": None,
            }
            fake_knowledge_base = mock.Mock()
            fake_knowledge_base.get_sync_status.return_value = {
                "dataMode": "production",
                "activeSource": {"type": "json_api", "name": "Internal API"},
                "activeSourceKey": "production",
                "availableSources": [{"type": "json_api", "configured": True}],
                "contractReady": True,
                "recordCount": 12,
                "syncStatus": "ready",
                "sourceRegistryIssues": [],
            }
            fake_knowledge_base.get_internal_data_contract.return_value = {
                "currentSummary": {"isReady": True}
            }
            self.flask_app.config["refresh_coordinator"] = fake_refresh_coordinator
            self.flask_app.config["knowledge_base"] = fake_knowledge_base

            try:
                response = self.client.post("/api/internal-api/refresh")
            finally:
                self.flask_app.config["refresh_coordinator"] = original_refresh_coordinator
                self.flask_app.config["knowledge_base"] = original_knowledge_base

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["status"], "refreshed")
        self.assertTrue(payload["apiConnectionActive"])
        fake_refresh_coordinator.refresh_all.assert_called_once()

    def test_internal_api_refresh_endpoint_activates_configured_api_profile(self):
        with self.flask_app.app_context():
            original_refresh_coordinator = self.flask_app.config["refresh_coordinator"]
            original_knowledge_base = self.flask_app.config["knowledge_base"]
            original_forecast_cache = self.flask_app.config["forecast_cache"]
            original_cash_out_store = self.flask_app.config["cash_out_store"]

            demo_snapshot = {
                "dataMode": "demo",
                "activeSource": {"type": "csv", "name": "Demo Lokal"},
                "activeSourceKey": "demo",
                "availableSources": [{"key": "production", "type": "json_api", "configured": True}],
                "contractReady": True,
                "recordCount": 12,
                "syncStatus": "ready",
                "sourceRegistryIssues": [],
            }
            api_snapshot = {
                **demo_snapshot,
                "dataMode": "production",
                "activeSource": {"type": "json_api", "name": "Internal API"},
                "activeSourceKey": "production",
            }
            fake_refresh_coordinator = mock.Mock()
            fake_refresh_coordinator.refresh_all.return_value = {
                "knowledgeBase": True,
                "cashOutSource": None,
            }
            fake_knowledge_base = mock.Mock()
            fake_knowledge_base.get_sync_status.side_effect = [demo_snapshot, api_snapshot]
            fake_knowledge_base.activate_source.return_value = {"activated": True}
            fake_knowledge_base.get_internal_data_contract.return_value = {
                "currentSummary": {"isReady": True}
            }
            fake_forecast_cache = mock.Mock()
            fake_cash_out_store = mock.Mock()
            fake_cash_out_store.get_status.return_value = {"syncStatus": "not_configured"}
            self.flask_app.config["refresh_coordinator"] = fake_refresh_coordinator
            self.flask_app.config["knowledge_base"] = fake_knowledge_base
            self.flask_app.config["forecast_cache"] = fake_forecast_cache
            self.flask_app.config["cash_out_store"] = fake_cash_out_store

            try:
                response = self.client.post("/api/internal-api/refresh")
            finally:
                self.flask_app.config["refresh_coordinator"] = original_refresh_coordinator
                self.flask_app.config["knowledge_base"] = original_knowledge_base
                self.flask_app.config["forecast_cache"] = original_forecast_cache
                self.flask_app.config["cash_out_store"] = original_cash_out_store

        self.assertEqual(response.status_code, 200)
        fake_knowledge_base.activate_source.assert_called_once_with("production")
        fake_forecast_cache.clear.assert_called_once()
        fake_cash_out_store.rebind_source_profile.assert_called_once_with(
            fake_knowledge_base.source_profile,
            refresh=True,
        )

    def test_connect_endpoint_saves_and_activates_ready_api_profile(self):
        import core as core_module

        fake_response = mock.Mock()
        fake_response.status_code = 200
        fake_response.raise_for_status.return_value = None
        fake_response.json.return_value = {
            "success": True,
            "code": 200,
            "message": "OK",
            "data": {
                "dataset_result": [
                    {
                        "reporting_period": "Januari 2026",
                        "segmentasi_customer": "Instansi Pemerintah",
                        "produk_utama": "Audit SPBE",
                        "bucket_pembayaran": "Kelas B (Telat 1-2 Minggu)",
                        "nominal_tagihan": 275000000,
                        "hambatan_penagihan": "Dokumen termin menunggu approval internal.",
                    },
                    {
                        "reporting_period": "Februari 2026",
                        "segmentasi_customer": "Swasta",
                        "produk_utama": "Pelatihan AI",
                        "bucket_pembayaran": "Kelas C (Telat 1-2 Bulan)",
                        "nominal_tagihan": 120000000,
                        "hambatan_penagihan": "Customer meminta jadwal ulang invoice.",
                    },
                ]
            },
        }

        with mock.patch.object(core_module.requests, "request", return_value=fake_response):
            response = self.client.post(
                "/api/internal-data/connect",
                json={
                    "endpointUrl": "https://example.com/api/Resource/dataset",
                    "method": "POST",
                    "basicUsername": "demo-user",
                    "basicPassword": "demo-pass",
                    "bodyJson": {"dataset": "FinanceInvoice"},
                    "recordsKey": "data.dataset_result",
                },
            )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ready"])
        self.assertTrue(payload["activated"])
        self.assertTrue(payload["profileSaved"])
        self.assertEqual(payload["syncStatus"]["financialData"]["activeSourceKey"], "production")
        self.assertTrue(Path(os.environ["INTERNAL_API_CONFIG_FILE"]).exists())

    def test_connect_preview_does_not_persist_profile(self):
        import core as core_module

        config_path = Path(os.environ["INTERNAL_API_CONFIG_FILE"])
        if config_path.exists():
            config_path.unlink()

        fake_response = mock.Mock()
        fake_response.status_code = 200
        fake_response.raise_for_status.return_value = None
        fake_response.json.return_value = {
            "success": True,
            "code": 200,
            "message": "OK",
            "data": {
                "dataset_result": [
                    {
                        "reporting_period": "Januari 2026",
                        "segmentasi_customer": "Instansi Pemerintah",
                        "produk_utama": "Audit SPBE",
                        "bucket_pembayaran": "Kelas B",
                        "nominal_tagihan": 275000000,
                    }
                ]
            },
        }

        with mock.patch.object(core_module.requests, "request", return_value=fake_response):
            response = self.client.post(
                "/api/internal-data/connect",
                json={
                    "endpointUrl": "https://example.com/api/Resource/dataset",
                    "method": "POST",
                    "bodyFormat": "form",
                    "bodyJson": {"dataset": "FinanceInvoice"},
                    "recordsKey": "data.dataset_result",
                    "activate": False,
                },
            )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ready"])
        self.assertFalse(payload["activated"])
        self.assertFalse(payload["profileSaved"])
        self.assertFalse(config_path.exists())


if __name__ == "__main__":
    unittest.main()
