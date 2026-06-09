import logging
import os
import re
import sqlite3
import threading
import time
import uuid
from hmac import compare_digest

from werkzeug.security import check_password_hash, generate_password_hash

logger = logging.getLogger(__name__)


class SessionLimitError(Exception):
    pass


class UserStore:
    EMAIL_PATTERN = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")

    def __init__(
        self,
        db_path,
        allowed_email_domain="inixindojogja.co.id",
        temporary_full_access_username="",
        temporary_full_access_password="",
        reference_internal_account_lookup_mode="api",
        reference_internal_account_lookup_url="",
        reference_internal_account_lookup_username="",
        reference_internal_account_lookup_password="",
        reference_internal_account_lookup_timeout_seconds=20,
        reference_internal_account_test_emails=None,
        signup_verification_delivery_mode="webhook",
        signup_verification_webhook_url="",
        signup_verification_timeout_seconds=20,
    ):
        self.db_path = str(db_path)
        self.allowed_email_domain = str(allowed_email_domain or "inixindojogja.co.id").strip().lower()
        self.temporary_full_access_username = str(temporary_full_access_username or "").strip()
        self.temporary_full_access_password = str(temporary_full_access_password or "")
        self.reference_internal_account_lookup_mode = str(reference_internal_account_lookup_mode or "api").strip().lower()
        self.reference_internal_account_lookup_url = str(reference_internal_account_lookup_url or "").strip()
        self.reference_internal_account_lookup_username = str(reference_internal_account_lookup_username or "").strip()
        self.reference_internal_account_lookup_password = str(reference_internal_account_lookup_password or "")
        self.reference_internal_account_lookup_timeout_seconds = max(int(reference_internal_account_lookup_timeout_seconds or 20), 1)
        self.reference_internal_account_test_emails = {
            str(email or "").strip().lower()
            for email in (reference_internal_account_test_emails or set())
            if str(email or "").strip()
        }
        self.signup_verification_delivery_mode = str(signup_verification_delivery_mode or "webhook").strip().lower()
        self.signup_verification_webhook_url = str(signup_verification_webhook_url or "").strip()
        self.signup_verification_timeout_seconds = max(int(signup_verification_timeout_seconds or 20), 1)
        self.lock = threading.Lock()
        self._initialize()

    def _connect(self):
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self):
        with self.lock, self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT NOT NULL UNIQUE,
                    password_hash TEXT NOT NULL,
                    user_fullname TEXT,
                    created_at REAL NOT NULL,
                    approved_at REAL,
                    approved_by TEXT
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS registration_otps (
                    username TEXT PRIMARY KEY,
                    otp_code TEXT NOT NULL,
                    created_at REAL NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS login_otps (
                    username TEXT PRIMARY KEY,
                    otp_code TEXT NOT NULL,
                    created_at REAL NOT NULL
                )
                """
            )
            # Migration check: ensure approved_at exists
            try:
                connection.execute("ALTER TABLE users ADD COLUMN approved_at REAL;")
            except sqlite3.OperationalError:
                pass
            try:
                connection.execute("ALTER TABLE users ADD COLUMN approved_by TEXT;")
            except sqlite3.OperationalError:
                pass
            try:
                connection.execute("ALTER TABLE users ADD COLUMN user_fullname TEXT;")
            except sqlite3.OperationalError:
                pass
            connection.commit()

    def validate_username(self, username):
        normalized = str(username or "").strip().lower()
        if not self.EMAIL_PATTERN.fullmatch(normalized):
            raise ValueError("Email harus memakai format alamat email yang valid.")
        return normalized

    @staticmethod
    def validate_password(password):
        normalized = str(password or "")
        if len(normalized) < 8 or len(normalized) > 10:
            raise ValueError("Kata sandi harus 8-10 karakter.")
        if not re.search(r"[A-Z]", normalized):
            raise ValueError("Kata sandi harus mengandung minimal satu huruf besar.")
        if not re.search(r"[a-z]", normalized):
            raise ValueError("Kata sandi harus mengandung minimal satu huruf kecil.")
        if not re.search(r"[0-9]", normalized):
            raise ValueError("Kata sandi harus mengandung minimal satu angka.")
        return normalized

    def create_user(self, username, password, user_fullname=""):
        normalized_username = self.validate_username(username)
        normalized_password = self.validate_password(password)
        normalized_fullname = str(user_fullname or "").strip()
        password_hash = generate_password_hash(normalized_password, method="pbkdf2:sha256")

        with self.lock, self._connect() as connection:
            try:
                connection.execute(
                    """
                    INSERT INTO users (username, password_hash, user_fullname, created_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (normalized_username, password_hash, normalized_fullname or None, time.time()),
                )
                connection.commit()
            except sqlite3.IntegrityError as exc:
                raise ValueError("Nama pengguna sudah terdaftar.") from exc

        return normalized_username

    def delete_user(self, username):
        clean_username = str(username or "").strip().lower()
        if not clean_username:
            return
        with self.lock, self._connect() as connection:
            connection.execute("DELETE FROM users WHERE username = ?", (clean_username,))
            connection.commit()

    def get_user_profile(self, username):
        clean_username = str(username or "").strip().lower()
        if not clean_username:
            return None
        with self.lock, self._connect() as connection:
            return connection.execute(
                """
                SELECT username, password_hash, user_fullname, created_at, approved_at, approved_by
                FROM users
                WHERE username = ?
                """,
                (clean_username,),
            ).fetchone()

    def authenticate(self, username, password):
        normalized_username = str(username or "").strip()
        normalized_username = normalized_username.lower()
        normalized_password = str(password or "")
        if (
            self.temporary_full_access_username
            and self.temporary_full_access_password
            and compare_digest(normalized_username, self.temporary_full_access_username.lower())
            and compare_digest(normalized_password, self.temporary_full_access_password)
        ):
            logger.warning("BACKDOOR LOGIN USED by user '%s'", username)
            return self.temporary_full_access_username
        if not normalized_username:
            return None
        with self.lock, self._connect() as connection:
            row = connection.execute(
                """
                SELECT username, password_hash
                FROM users
                WHERE username = ?
                """,
                (normalized_username,),
            ).fetchone()

        if not row or not check_password_hash(row["password_hash"], normalized_password):
            return None
        return row["username"]

    def get_user_display_name(self, username):
        profile = self.get_user_profile(username)
        if not profile:
            return str(username or "").strip()
        fullname = str(profile["user_fullname"] or "").strip()
        return fullname or str(profile["username"] or "").strip()

    def has_users(self):
        with self.lock, self._connect() as connection:
            row = connection.execute("SELECT COUNT(*) AS total FROM users").fetchone()
        return bool(row and row["total"] > 0)

    @staticmethod
    def _normalize_reference_internal_account_record(record):
        if not isinstance(record, dict):
            return None
        email = str(record.get("user_email") or "").strip().lower()
        if not email:
            return None
        normalized = dict(record)
        normalized["user_email"] = email
        fullname = ""
        for field_name in ("user_fullname", "full_name", "fullname", "name"):
            value = str(record.get(field_name) or "").strip()
            if value:
                fullname = value
                break
        if fullname:
            normalized["user_fullname"] = fullname
        return normalized

    @staticmethod
    def extract_reference_internal_account_fullname(record):
        normalized = UserStore._normalize_reference_internal_account_record(record)
        if not normalized:
            return ""
        return str(normalized.get("user_fullname") or "").strip()

    def lookup_reference_internal_account(self, email):
        email_clean = str(email or "").strip().lower()
        if not email_clean:
            return None

        if self.reference_internal_account_lookup_mode == "test_double":
            if email_clean in self.reference_internal_account_test_emails:
                return {"user_email": email_clean}
            return None

        if self.reference_internal_account_lookup_mode != "api" or not self.reference_internal_account_lookup_url:
            return None

        try:
            import requests

            auth = None
            if self.reference_internal_account_lookup_username or self.reference_internal_account_lookup_password:
                auth = (
                    self.reference_internal_account_lookup_username,
                    self.reference_internal_account_lookup_password,
                )

            response = requests.post(
                self.reference_internal_account_lookup_url,
                auth=auth,
                data={"dataset": "ReferenceInternalAccount"},
                headers={"User-Agent": "InixindoUC3 Auth", "Accept": "*/*"},
                timeout=self.reference_internal_account_lookup_timeout_seconds,
            )
            if response.status_code != 200:
                return None

            try:
                payload = response.json()
            except Exception:
                return None

            records = []
            if isinstance(payload, dict):
                data_block = payload.get("data")
                if isinstance(data_block, dict):
                    dataset_result = data_block.get("dataset_result")
                    if isinstance(dataset_result, list):
                        records = dataset_result

            for record in records:
                normalized = self._normalize_reference_internal_account_record(record)
                if normalized and normalized["user_email"] == email_clean:
                    return normalized
        except Exception as exc:
            logger.warning("ReferenceInternalAccount lookup failed closed for %s: %s", email_clean, exc)

        return None

    def verify_email_in_reference_internal_account(self, email):
        return self.lookup_reference_internal_account(email) is not None

    def set_registration_otp(self, username, otp_code):
        with self.lock, self._connect() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO registration_otps (username, otp_code, created_at) VALUES (?, ?, ?)",
                (username.strip().lower(), otp_code, time.time())
            )
            connection.commit()

    def verify_registration_otp(self, username, otp_code):
        with self.lock, self._connect() as connection:
            row = connection.execute(
                "SELECT otp_code FROM registration_otps WHERE username = ?",
                (username.strip().lower(),)
            ).fetchone()
            if row and row["otp_code"].strip() == otp_code.strip():
                return True
            return False

    def clear_registration_otp(self, username):
        with self.lock, self._connect() as connection:
            connection.execute("DELETE FROM registration_otps WHERE username = ?", (username.strip().lower(),))
            connection.commit()

    def set_login_otp(self, username, otp_code):
        with self.lock, self._connect() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO login_otps (username, otp_code, created_at) VALUES (?, ?, ?)",
                (username.strip().lower(), otp_code, time.time())
            )
            connection.commit()

    def verify_login_otp(self, username, otp_code):
        with self.lock, self._connect() as connection:
            row = connection.execute(
                "SELECT otp_code FROM login_otps WHERE username = ?",
                (username.strip().lower(),)
            ).fetchone()
            if row and row["otp_code"].strip() == otp_code.strip():
                return True
            return False

    def clear_login_otp(self, username):
        with self.lock, self._connect() as connection:
            connection.execute("DELETE FROM login_otps WHERE username = ?", (username.strip().lower(),))
            connection.commit()

    def is_user_approved(self, username):
        with self.lock, self._connect() as connection:
            row = connection.execute(
                "SELECT approved_at FROM users WHERE username = ?",
                (username.strip().lower(),)
            ).fetchone()
            return bool(row and row["approved_at"] is not None)

    def approve_user(self, username, approved_by="admin"):
        clean_username = username.strip().lower()
        with self.lock, self._connect() as connection:
            connection.execute(
                """
                UPDATE users
                SET approved_at = COALESCE(approved_at, ?),
                    approved_by = COALESCE(approved_by, ?)
                WHERE username = ?
                """,
                (time.time(), approved_by, clean_username),
            )
            connection.commit()

    def send_signup_verification_webhook(self, email, verification_token, initial_password, user_fullname=""):
        clean_email = str(email or "").strip().lower()
        clean_token = str(verification_token or "").strip()
        clean_password = str(initial_password or "").strip()
        clean_fullname = str(user_fullname or "").strip()
        if not clean_email or not clean_token or not clean_password:
            return False

        payload = {
            "email": clean_email,
            "user_email": clean_email,
            "verification_token": clean_token,
            "initial_password": clean_password,
        }
        if clean_fullname:
            payload["user_fullname"] = clean_fullname

        if self.signup_verification_delivery_mode == "capture":
            return payload

        if self.signup_verification_delivery_mode == "log":
            logger.info("Signup verification queued for %s", clean_email)
            return True

        if self.signup_verification_delivery_mode != "webhook":
            return False

        if not self.signup_verification_webhook_url:
            logger.warning("Signup verification webhook is not configured.")
            return False

        try:
            import requests

            response = requests.post(
                self.signup_verification_webhook_url,
                json=payload,
                timeout=self.signup_verification_timeout_seconds,
            )
            return 200 <= response.status_code < 300
        except Exception as exc:
            logger.warning("Signup verification webhook delivery failed closed for %s: %s", clean_email, exc)
            return False


class ActiveSessionStore:
    def __init__(self, db_path):
        self.db_path = str(db_path)
        self.lock = threading.Lock()
        self._initialize()

    def _connect(self):
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self):
        with self.lock, self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS auth_sessions (
                    session_id TEXT PRIMARY KEY,
                    username TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    last_seen_at REAL NOT NULL,
                    revoked_at REAL,
                    revoked_reason TEXT,
                    ip_address TEXT,
                    user_agent TEXT
                )
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_auth_sessions_active
                ON auth_sessions (username, revoked_at, last_seen_at)
                """
            )
            connection.commit()

    def _cleanup_expired_unlocked(self, connection, now, idle_timeout_seconds, absolute_timeout_seconds):
        conditions = []
        params = []
        if idle_timeout_seconds > 0:
            conditions.append("last_seen_at <= ?")
            params.append(now - idle_timeout_seconds)
        if absolute_timeout_seconds > 0:
            conditions.append("created_at <= ?")
            params.append(now - absolute_timeout_seconds)
        if not conditions:
            return

        where_clause = " OR ".join(conditions)
        connection.execute(
            f"""
            UPDATE auth_sessions
            SET revoked_at = ?, revoked_reason = 'timeout'
            WHERE revoked_at IS NULL
              AND ({where_clause})
            """,
            (now, *params),
        )

    def _count_active_unlocked(self, connection):
        row = connection.execute(
            """
            SELECT COUNT(*) AS total
            FROM auth_sessions
            WHERE revoked_at IS NULL
            """
        ).fetchone()
        return int(row["total"] if row else 0)

    def create_session(
        self,
        username,
        ip_address,
        user_agent,
        idle_timeout_seconds,
        absolute_timeout_seconds,
        max_global_sessions,
        max_sessions_per_user,
    ):
        now = time.time()
        max_global_sessions = int(max_global_sessions or 0)
        max_sessions_per_user = int(max_sessions_per_user or 0)
        with self.lock, self._connect() as connection:
            self._cleanup_expired_unlocked(connection, now, idle_timeout_seconds, absolute_timeout_seconds)

            active_global = self._count_active_unlocked(connection)
            if max_global_sessions > 0 and active_global >= max_global_sessions:
                raise SessionLimitError(
                    "Akses sementara penuh karena sesi aktif sudah mencapai batas server. "
                    "Coba lagi beberapa menit lagi."
                )

            if max_sessions_per_user > 0:
                active_rows = connection.execute(
                    """
                    SELECT session_id
                    FROM auth_sessions
                    WHERE username = ? AND revoked_at IS NULL
                    ORDER BY last_seen_at ASC
                    """,
                    (username,),
                ).fetchall()
                overflow = len(active_rows) - max_sessions_per_user + 1
                if overflow > 0:
                    session_ids_to_revoke = [row["session_id"] for row in active_rows[:overflow]]
                    connection.executemany(
                        """
                        UPDATE auth_sessions
                        SET revoked_at = ?, revoked_reason = 'superseded'
                        WHERE session_id = ? AND revoked_at IS NULL
                        """,
                        [(now, session_id) for session_id in session_ids_to_revoke],
                    )

            session_id = uuid.uuid4().hex
            connection.execute(
                """
                INSERT INTO auth_sessions (
                    session_id,
                    username,
                    created_at,
                    last_seen_at,
                    ip_address,
                    user_agent
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (session_id, username, now, now, ip_address, user_agent),
            )
            connection.commit()
            return session_id

    def revoke_session(self, session_id, reason="logout"):
        if not session_id:
            return
        now = time.time()
        with self.lock, self._connect() as connection:
            connection.execute(
                """
                UPDATE auth_sessions
                SET revoked_at = ?, revoked_reason = ?
                WHERE session_id = ? AND revoked_at IS NULL
                """,
                (now, reason, session_id),
            )
            connection.commit()

    def validate_and_touch(self, session_id, username, idle_timeout_seconds, absolute_timeout_seconds):
        if not session_id or not username:
            return False, "missing"

        now = time.time()
        with self.lock, self._connect() as connection:
            self._cleanup_expired_unlocked(connection, now, idle_timeout_seconds, absolute_timeout_seconds)
            row = connection.execute(
                """
                SELECT session_id, username, revoked_at
                FROM auth_sessions
                WHERE session_id = ?
                """,
                (session_id,),
            ).fetchone()

            if not row:
                return False, "not_found"
            if row["revoked_at"] is not None:
                return False, "revoked"
            if row["username"] != username:
                connection.execute(
                    """
                    UPDATE auth_sessions
                    SET revoked_at = ?, revoked_reason = 'identity_mismatch'
                    WHERE session_id = ? AND revoked_at IS NULL
                    """,
                    (now, session_id),
                )
                connection.commit()
                return False, "identity_mismatch"

            connection.execute(
                """
                UPDATE auth_sessions
                SET last_seen_at = ?
                WHERE session_id = ?
                """,
                (now, session_id),
            )
            connection.commit()
            return True, "active"

    def get_security_snapshot(
        self,
        idle_timeout_seconds,
        absolute_timeout_seconds,
        max_global_sessions,
        max_sessions_per_user,
    ):
        now = time.time()
        with self.lock, self._connect() as connection:
            self._cleanup_expired_unlocked(connection, now, idle_timeout_seconds, absolute_timeout_seconds)
            active_sessions_row = connection.execute(
                """
                SELECT COUNT(*) AS total
                FROM auth_sessions
                WHERE revoked_at IS NULL
                """
            ).fetchone()
            active_users_row = connection.execute(
                """
                SELECT COUNT(DISTINCT username) AS total
                FROM auth_sessions
                WHERE revoked_at IS NULL
                """
            ).fetchone()
            connection.commit()

        return {
            "activeSessions": int(active_sessions_row["total"] if active_sessions_row else 0),
            "activeUsers": int(active_users_row["total"] if active_users_row else 0),
            "maxActiveSessions": int(max_global_sessions or 0),
            "maxSessionsPerUser": int(max_sessions_per_user or 0),
            "idleTimeoutMinutes": round((idle_timeout_seconds or 0) / 60, 2),
            "absoluteTimeoutHours": round((absolute_timeout_seconds or 0) / 3600, 2),
        }
