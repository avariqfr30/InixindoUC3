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
                    "body": {"dataset_code": "ClassReport"},
                },
            }

            with mock.patch.object(core_module.requests, "request", return_value=fake_response) as request_mock:
                client = core_module.InternalAPIClient(source_profile=profile)
                records, _ = client.fetch_records()

            self.assertEqual(records[0]["period"], "Q1 2026")
            _, kwargs = request_mock.call_args
            self.assertEqual(kwargs["headers"]["Authorization"], "Bearer env-token")
            self.assertEqual(kwargs["data"], {"dataset_code": "ClassReport"})
            self.assertNotIn("json", kwargs)
        finally:
            if old_env["INTERNAL_API_AUTH_TOKEN"] is None:
                os.environ.pop("INTERNAL_API_AUTH_TOKEN", None)
            else:
                os.environ["INTERNAL_API_AUTH_TOKEN"] = old_env["INTERNAL_API_AUTH_TOKEN"]
            for module_name in ("core", "config", "finance_api_clients"):
                if module_name in sys.modules:
                    del sys.modules[module_name]


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
                "username": "contract_user",
                "password": "password123",
                "confirm_password": "password123",
            },
            follow_redirects=False,
        )
        if signup.status_code not in (302, 400):
            raise AssertionError(f"Unexpected signup status: {signup.status_code}")

        if signup.status_code == 400:
            login = self.client.post(
                "/login",
                data={"username": "contract_user", "password": "password123"},
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
        self.assertIn("Simpan & Aktifkan Internal API", settings_template)

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
                    "bodyJson": {"dataset_code": "ClassReport"},
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
                    "bodyJson": {"dataset_code": "ClassReport"},
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
