import json
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path


WORKSPACE = Path("/Users/avariqfr30/Documents/InixindoUC3/Payment predictor")
sys.path.insert(0, str(WORKSPACE))

from data_sources import (
    build_internal_api_profile_template,
    load_available_source_profiles,
    read_active_source_key,
    resolve_active_source_profile,
    summarize_source_profile,
    write_active_source_key,
)


class DataSourceProfilesTest(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="cashin-source-profiles-")
        self.state_path = os.path.join(self.tmpdir, "active-source.json")
        self.demo_csv = str(WORKSPACE / "data" / "db.csv")
        self.old_env = {
            "INTERNAL_API_ENDPOINT_URL": os.environ.get("INTERNAL_API_ENDPOINT_URL"),
            "INTERNAL_API_METHOD": os.environ.get("INTERNAL_API_METHOD"),
            "INTERNAL_API_BODY_JSON": os.environ.get("INTERNAL_API_BODY_JSON"),
            "INTERNAL_API_HEADERS_JSON": os.environ.get("INTERNAL_API_HEADERS_JSON"),
            "INTERNAL_API_QUERY_PARAMS_JSON": os.environ.get("INTERNAL_API_QUERY_PARAMS_JSON"),
            "INTERNAL_API_FIELD_MAP_JSON": os.environ.get("INTERNAL_API_FIELD_MAP_JSON"),
            "INTERNAL_API_BASIC_USERNAME": os.environ.get("INTERNAL_API_BASIC_USERNAME"),
            "INTERNAL_API_BASIC_PASSWORD": os.environ.get("INTERNAL_API_BASIC_PASSWORD"),
            "INTERNAL_API_CONFIG_FILE": os.environ.get("INTERNAL_API_CONFIG_FILE"),
        }

    def tearDown(self):
        for key, value in self.old_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_profile_registry_loads_demo_and_env_production(self):
        os.environ["INTERNAL_API_ENDPOINT_URL"] = "https://example.com/api/Resource/dataset"
        os.environ["INTERNAL_API_METHOD"] = "POST"
        os.environ["INTERNAL_API_BODY_JSON"] = json.dumps({"dataset_code": "ClassReport"})
        os.environ["INTERNAL_API_BASIC_USERNAME"] = "demo-user"
        os.environ["INTERNAL_API_BASIC_PASSWORD"] = "demo-pass"

        profiles, issues, default_key = load_available_source_profiles(
            demo_csv_path=self.demo_csv,
            legacy_data_mode="internal_api",
            internal_api_endpoint_url=os.environ["INTERNAL_API_ENDPOINT_URL"],
            internal_api_base_url="",
            internal_api_dataset_path="/api/finance/invoices",
        )

        self.assertEqual(default_key, "production")
        self.assertEqual(issues, [])
        self.assertIn("demo", profiles)
        self.assertIn("production", profiles)
        self.assertEqual(profiles["production"]["endpoint"]["method"], "POST")
        self.assertEqual(profiles["production"]["request"]["body"]["dataset_code"], "ClassReport")

    def test_single_file_internal_api_profile_is_supported(self):
        profile = build_internal_api_profile_template()
        profile["endpoint"]["url"] = "https://example.com/api/Resource/dataset"
        profile["request"]["body"] = {"dataset_code": "ClassReport"}
        profile["field_map"] = {}
        profile_path = os.path.join(self.tmpdir, "production-profile.json")
        Path(profile_path).write_text(json.dumps(profile), encoding="utf-8")

        profiles, issues, default_key = load_available_source_profiles(
            demo_csv_path=self.demo_csv,
            legacy_data_mode="internal_api",
            internal_api_endpoint_url="",
            internal_api_base_url="",
            internal_api_dataset_path="/api/finance/invoices",
            config_file_path=profile_path,
        )

        self.assertEqual(default_key, "production")
        self.assertEqual(issues, [])
        self.assertEqual(profiles["production"]["endpoint"]["url"], "https://example.com/api/Resource/dataset")
        self.assertEqual(profiles["production"]["request"]["body"]["dataset_code"], "ClassReport")

    def test_active_source_state_roundtrip(self):
        profiles, _, _ = load_available_source_profiles(
            demo_csv_path=self.demo_csv,
            legacy_data_mode="demo",
            internal_api_endpoint_url="",
            internal_api_base_url="",
            internal_api_dataset_path="/api/finance/invoices",
        )

        write_active_source_key(self.state_path, "demo")
        self.assertEqual(read_active_source_key(self.state_path), "demo")

        active_key, active_profile = resolve_active_source_profile(
            profiles=profiles,
            state_path=self.state_path,
            legacy_default_key="demo",
        )
        self.assertEqual(active_key, "demo")
        self.assertEqual(active_profile["type"], "demo_csv")

    def test_public_summary_hides_secrets(self):
        profile = {
            "key": "production",
            "name": "Produksi API Internal",
            "mode": "production",
            "type": "json_api",
            "endpoint": {
                "url": "https://example.com/api/Resource/dataset",
                "method": "POST",
                "records_key": "data.items",
            },
            "auth": {
                "basic_username": "demo-user",
                "basic_password": "demo-pass",
            },
            "request": {
                "headers": {"X-Test": "1"},
                "query_params": {"tag": "cashflow"},
                "body": {"dataset_code": "ClassReport"},
            },
        }

        summary = summarize_source_profile(profile)
        self.assertTrue(summary["configured"])
        self.assertTrue(summary["basicAuthConfigured"])
        self.assertTrue(summary["bodyConfigured"])
        self.assertNotIn("basic_password", json.dumps(summary))


if __name__ == "__main__":
    unittest.main()
