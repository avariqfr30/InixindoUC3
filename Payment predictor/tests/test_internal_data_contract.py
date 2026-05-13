import json
import os
import shutil
import sys
import tempfile
import unittest
from unittest import mock
from pathlib import Path

import pandas as pd


WORKSPACE = Path("/Users/avariqfr30/Documents/InixindoUC3/Payment predictor")
sys.path.insert(0, str(WORKSPACE))

from data_contract import (
    extract_records_from_payload,
    get_internal_api_contract,
    normalize_financial_dataframe,
    parse_internal_api_field_map,
)
from internal_api_doctor import run_production_source_doctor


class InternalDataContractUnitTest(unittest.TestCase):
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
        self.assertIn("Refresh Dataset Sekarang", settings_template)
        self.assertIn("btn-refresh-internal-api", settings_template)
        self.assertIn("/api/internal-api/refresh", settings_template)
        self.assertIn("setInternalApiConnectionState", settings_template)
        self.assertNotIn("Project data source", settings_template)
        self.assertNotIn("active.type", settings_template)

    def test_internal_api_settings_page_is_status_only_for_employees(self):
        settings_template = (WORKSPACE / "templates" / "data_settings.html").read_text(encoding="utf-8")

        self.assertIn("Konfigurasi API dikelola dari environment VPS", settings_template)
        self.assertIn("Internal API Sudah Aktif", settings_template)
        self.assertIn("Refresh Dataset Sekarang", settings_template)
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
        fake_cash_out_store.refresh_data.assert_called_once()

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
