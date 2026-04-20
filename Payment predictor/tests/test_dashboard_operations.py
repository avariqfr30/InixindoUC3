import os
import shutil
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path


WORKSPACE = Path("/Users/avariqfr30/Documents/InixindoUC3/Payment predictor")
sys.path.insert(0, str(WORKSPACE))


class CashOutProjectorTest(unittest.TestCase):
    def test_live_cash_out_records_override_modeled_projection(self):
        from forecast_engine import CashOutProjector

        projector = CashOutProjector(monthly_operating_cost_idr=300_000_000)
        result = projector.project_cash_out(
            start_date=datetime(2026, 4, 1),
            end_date=datetime(2026, 4, 30),
            cash_out_records=[
                {
                    "amount": 90_000_000,
                    "due_date": datetime(2026, 4, 5),
                    "category": "Payroll",
                    "is_open": True,
                },
                {
                    "amount": 45_000_000,
                    "due_date": datetime(2026, 4, 20),
                    "category": "Vendor",
                    "is_open": True,
                },
                {
                    "amount": 50_000_000,
                    "due_date": datetime(2026, 5, 2),
                    "category": "Vendor",
                    "is_open": True,
                },
            ],
        )

        self.assertEqual(result["source"], "live_schedule")
        self.assertEqual(result["total_cash_out"], 135_000_000)
        self.assertEqual(result["event_count"], 2)
        self.assertEqual(result["category_breakdown"][0]["category"], "Payroll")


class DashboardOperationRouteTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._tmpdir = tempfile.mkdtemp(prefix="cashin-dashboard-ops-")
        os.environ["JOB_STATE_DB_PATH"] = os.path.join(cls._tmpdir, "jobs.db")
        os.environ["REPORT_ARTIFACTS_DIR"] = os.path.join(cls._tmpdir, "artifacts")
        os.environ["DATA_SOURCE_ACTIVE_STATE_PATH"] = os.path.join(cls._tmpdir, "active-source.json")
        os.environ["APP_SECRET_KEY"] = "test-secret-key"
        os.environ["SESSION_COOKIE_SECURE"] = "false"
        os.environ["DATA_REFRESH_INTERVAL_SECONDS"] = "0"
        os.environ["FORECAST_CACHE_TTL_SECONDS"] = "60"

        for module_name in ("app", "config"):
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
                "username": "dashboard_ops_user",
                "password": "password123",
                "confirm_password": "password123",
            },
            follow_redirects=False,
        )
        if signup.status_code == 400:
            login = self.client.post(
                "/login",
                data={"username": "dashboard_ops_user", "password": "password123"},
                follow_redirects=False,
            )
            if login.status_code != 302:
                raise AssertionError(f"Unexpected login status: {login.status_code}")

    def test_get_config_and_health_expose_sync_status(self):
        config_response = self.client.get("/get-config")
        self.assertEqual(config_response.status_code, 200)
        config_payload = config_response.get_json()
        self.assertIn("syncStatus", config_payload)
        self.assertIn("financialData", config_payload["syncStatus"])
        self.assertIn("dataSourceContract", config_payload)
        self.assertEqual(config_payload["dataSourceContract"]["activeSourceKey"], "demo")

        health_response = self.client.get("/health")
        self.assertEqual(health_response.status_code, 200)
        health_payload = health_response.get_json()
        self.assertIn("syncStatus", health_payload)
        self.assertIn("cashOutSource", health_payload["syncStatus"])

    def test_data_source_validate_and_activate_demo(self):
        validate_response = self.client.post(
            "/api/data-source/validate",
            json={"sourceKey": "demo"},
        )
        self.assertEqual(validate_response.status_code, 200)
        validate_payload = validate_response.get_json()
        self.assertTrue(validate_payload["ready"])
        self.assertEqual(validate_payload["source"]["key"], "demo")

        activate_response = self.client.post(
            "/api/data-source/activate",
            json={"sourceKey": "demo"},
        )
        self.assertEqual(activate_response.status_code, 200)
        activate_payload = activate_response.get_json()
        self.assertTrue(activate_payload["activated"])
        self.assertEqual(
            activate_payload["syncStatus"]["financialData"]["activeSourceKey"],
            "demo",
        )

    def test_drilldown_endpoints_return_operational_payload(self):
        start_date = "2026-04-01T00:00:00"
        top_overdue = self.client.post(
            "/api/forecast/drilldown/top-overdue",
            json={
                "currency": "IDR",
                "cash_on_hand": 350_000_000,
                "monthly_operating_cost": 200_000_000,
                "start_date": start_date,
                "horizon": "short_term",
            },
        )
        self.assertEqual(top_overdue.status_code, 200)
        top_overdue_payload = top_overdue.get_json()
        self.assertIn("items", top_overdue_payload)
        self.assertIn("sync_status", top_overdue_payload)

        payment_class_trend = self.client.get("/api/forecast/drilldown/payment-class-trend")
        self.assertEqual(payment_class_trend.status_code, 200)
        trend_payload = payment_class_trend.get_json()
        self.assertIn("series", trend_payload)
        self.assertIn("topPeriods", trend_payload)

        concentration = self.client.post(
            "/api/forecast/drilldown/concentration",
            json={
                "currency": "IDR",
                "cash_on_hand": 350_000_000,
                "monthly_operating_cost": 200_000_000,
                "start_date": start_date,
                "horizon": "short_term",
            },
        )
        self.assertEqual(concentration.status_code, 200)
        concentration_payload = concentration.get_json()
        self.assertIn("concentration", concentration_payload)
        self.assertIn("partners", concentration_payload["concentration"])


if __name__ == "__main__":
    unittest.main()
