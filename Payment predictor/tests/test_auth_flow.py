import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path


WORKSPACE = Path("/Users/avariqfr30/Documents/InixindoUC3/Payment predictor")


class AuthFlowTestCase(unittest.TestCase):
    TEST_USERNAME = "bootstrap_owner@inixindojogja.co.id"
    TEST_PASSWORD = "password123"

    @classmethod
    def setUpClass(cls):
        cls._tmpdir = tempfile.mkdtemp(prefix="cashin-auth-flow-")
        os.environ["JOB_STATE_DB_PATH"] = os.path.join(cls._tmpdir, "jobs.db")
        os.environ["REPORT_ARTIFACTS_DIR"] = os.path.join(cls._tmpdir, "artifacts")
        os.environ["DATA_SOURCE_ACTIVE_STATE_PATH"] = os.path.join(cls._tmpdir, "active-source.json")
        os.environ["APP_SECRET_KEY"] = "test-secret-key"
        os.environ["SESSION_COOKIE_SECURE"] = "false"
        os.environ["AUTH_MAX_ACTIVE_SESSIONS"] = "6"
        os.environ["AUTH_MAX_SESSIONS_PER_USER"] = "1"
        os.environ["AUTH_SESSION_IDLE_TIMEOUT_MINUTES"] = "60"
        os.environ["AUTH_SESSION_ABSOLUTE_TIMEOUT_HOURS"] = "12"

        sys.path.insert(0, str(WORKSPACE))
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

    def _signup(self, client, username, password=TEST_PASSWORD):
        return client.post(
            "/signup",
            data={
                "username": username,
                "password": password,
                "confirm_password": password,
            },
            follow_redirects=False,
        )

    def _signup_or_login(self, client, username=TEST_USERNAME, password=TEST_PASSWORD):
        signup = self._signup(client, username, password=password)
        if signup.status_code == 302:
            return signup
        if signup.status_code == 202:
            return signup
        if signup.status_code not in (400, 403):
            raise AssertionError(f"Unexpected signup status: {signup.status_code}")
        login = client.post(
            "/login",
            data={"username": username, "password": password},
            follow_redirects=False,
        )
        if login.status_code != 302:
            raise AssertionError(f"Unexpected login status: {login.status_code}")
        return login

    def test_login_logout_flow_and_cache_headers(self):
        response = self.client.get("/")
        self.assertEqual(response.status_code, 302)
        self.assertIn("/login", response.location)

        login_page = self.client.get("/login")
        self.assertEqual(login_page.status_code, 200)
        self.assertIn("no-store", login_page.headers.get("Cache-Control", ""))

        signup = self._signup_or_login(self.client)
        self.assertEqual(signup.status_code, 302)
        self.assertTrue(signup.location.endswith("/"))

        home = self.client.get("/")
        self.assertEqual(home.status_code, 200)
        self.assertIn("Masuk sebagai", home.get_data(as_text=True))
        self.assertIn("Pengaturan Data", home.get_data(as_text=True))
        self.assertNotIn("Persetujuan Akun", home.get_data(as_text=True))
        self.assertIn("no-store", home.headers.get("Cache-Control", ""))

        settings = self.client.get("/settings")
        self.assertEqual(settings.status_code, 200)
        self.assertIn("Internal API / APIDog", settings.get_data(as_text=True))
        self.assertIn("no-store", settings.headers.get("Cache-Control", ""))

        auth_login_redirect = self.client.get("/login", follow_redirects=False)
        self.assertEqual(auth_login_redirect.status_code, 302)
        self.assertTrue(auth_login_redirect.location.endswith("/"))

        logout = self.client.post("/logout", follow_redirects=False)
        self.assertEqual(logout.status_code, 302)
        self.assertIn("/login", logout.location)
        self.assertIn("no-store", logout.headers.get("Cache-Control", ""))
        self.assertIn("session=", logout.headers.get("Set-Cookie", ""))

        post_logout_home = self.client.get("/", follow_redirects=False)
        self.assertEqual(post_logout_home.status_code, 302)
        self.assertIn("/login", post_logout_home.location)

        post_logout_api = self.client.get("/get-config")
        self.assertEqual(post_logout_api.status_code, 401)
        payload = post_logout_api.get_json()
        self.assertEqual(payload["error"], "Autentikasi diperlukan.")

    def test_new_login_revokes_previous_active_session_for_same_user(self):
        client_one = self.flask_app.test_client()
        signup = self._signup_or_login(client_one)
        self.assertEqual(signup.status_code, 302)
        first_access = client_one.get("/get-config")
        self.assertEqual(first_access.status_code, 200)

        client_two = self.flask_app.test_client()
        login = client_two.post(
            "/login",
            data={"username": self.TEST_USERNAME, "password": self.TEST_PASSWORD},
            follow_redirects=False,
        )
        self.assertEqual(login.status_code, 302)
        second_access = client_two.get("/get-config")
        self.assertEqual(second_access.status_code, 200)

        revoked_access = client_one.get("/get-config")
        self.assertEqual(revoked_access.status_code, 401)
        revoked_payload = revoked_access.get_json()
        self.assertEqual(revoked_payload["error"], "Autentikasi diperlukan.")

    def test_session_without_server_side_id_is_rejected(self):
        signup = self._signup_or_login(self.client)
        self.assertEqual(signup.status_code, 302)
        authorized = self.client.get("/get-config")
        self.assertEqual(authorized.status_code, 200)

        with self.client.session_transaction() as auth_session:
            auth_session.pop("auth_session_id", None)
            auth_session["username"] = self.TEST_USERNAME

        unauthorized = self.client.get("/get-config")
        self.assertEqual(unauthorized.status_code, 401)
        payload = unauthorized.get_json()
        self.assertEqual(payload["error"], "Autentikasi diperlukan.")

    def test_signup_allows_company_email_after_bootstrap_account_exists(self):
        first_signup = self._signup_or_login(self.client)
        self.assertEqual(first_signup.status_code, 302)

        self.client.post("/logout", follow_redirects=False)

        signup_page = self.client.get("/signup")
        self.assertEqual(signup_page.status_code, 200)
        html = signup_page.get_data(as_text=True)
        self.assertIn("@inixindojogja.co.id", html)
        self.assertIn(">Daftar</a>", html)

        second_signup = self._signup(self.client, "second_user@inixindojogja.co.id")
        self.assertEqual(second_signup.status_code, 302)

        self.client.post("/logout", follow_redirects=False)
        approved_login = self.client.post(
            "/login",
            data={"username": "second_user@inixindojogja.co.id", "password": self.TEST_PASSWORD},
            follow_redirects=False,
        )
        self.assertEqual(approved_login.status_code, 302)

    def test_signup_rejects_invalid_email_format_and_non_company_domain(self):
        self._signup_or_login(self.client)
        self.client.post("/logout", follow_redirects=False)

        rejected_signup = self._signup(self.client, "not-an-email")
        self.assertEqual(rejected_signup.status_code, 400)
        self.assertIn(
            "Email harus memakai format alamat email yang valid.",
            rejected_signup.get_data(as_text=True),
        )

        external_signup = self._signup(self.client, "external@example.com")
        self.assertEqual(external_signup.status_code, 400)
        self.assertIn(
            "Hanya email @inixindojogja.co.id yang diizinkan.",
            external_signup.get_data(as_text=True),
        )


if __name__ == "__main__":
    unittest.main()
