import re
import sqlite3
import threading
import time
import uuid

from werkzeug.security import check_password_hash, generate_password_hash


class SessionLimitError(Exception):
    pass


class UserStore:
    USERNAME_PATTERN = re.compile(r"^[A-Za-z0-9_]{3,32}$")

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
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT NOT NULL UNIQUE,
                    password_hash TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    status TEXT NOT NULL DEFAULT 'approved',
                    is_admin INTEGER NOT NULL DEFAULT 0,
                    approved_at REAL,
                    approved_by TEXT
                )
                """
            )
            columns = {
                row["name"]
                for row in connection.execute("PRAGMA table_info(users)").fetchall()
            }
            if "status" not in columns:
                connection.execute("ALTER TABLE users ADD COLUMN status TEXT NOT NULL DEFAULT 'approved'")
            if "is_admin" not in columns:
                connection.execute("ALTER TABLE users ADD COLUMN is_admin INTEGER NOT NULL DEFAULT 0")
            if "approved_at" not in columns:
                connection.execute("ALTER TABLE users ADD COLUMN approved_at REAL")
            if "approved_by" not in columns:
                connection.execute("ALTER TABLE users ADD COLUMN approved_by TEXT")
            connection.commit()

    @classmethod
    def validate_username(cls, username):
        normalized = str(username or "").strip()
        if not cls.USERNAME_PATTERN.fullmatch(normalized):
            raise ValueError("Nama pengguna harus 3-32 karakter dan hanya boleh berisi huruf, angka, atau garis bawah.")
        return normalized

    @staticmethod
    def validate_password(password):
        normalized = str(password or "")
        if len(normalized) < 8:
            raise ValueError("Kata sandi harus minimal 8 karakter.")
        return normalized

    def create_user(self, username, password, auto_approve=False, is_admin=False, approved_by=None):
        normalized_username = self.validate_username(username)
        normalized_password = self.validate_password(password)
        password_hash = generate_password_hash(normalized_password, method="pbkdf2:sha256")
        now = time.time()
        status = "approved" if auto_approve else "pending"
        approved_at = now if auto_approve else None
        approver = normalized_username if auto_approve and approved_by is None else approved_by

        with self.lock, self._connect() as connection:
            try:
                connection.execute(
                    """
                    INSERT INTO users (
                        username,
                        password_hash,
                        created_at,
                        status,
                        is_admin,
                        approved_at,
                        approved_by
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        normalized_username,
                        password_hash,
                        now,
                        status,
                        1 if is_admin else 0,
                        approved_at,
                        approver,
                    ),
                )
                connection.commit()
            except sqlite3.IntegrityError as exc:
                raise ValueError("Nama pengguna sudah terdaftar.") from exc

        return normalized_username

    def authenticate(self, username, password):
        normalized_username = str(username or "").strip()
        normalized_password = str(password or "")
        with self.lock, self._connect() as connection:
            row = connection.execute(
                """
                SELECT username, password_hash, status, is_admin
                FROM users
                WHERE username = ?
                """,
                (normalized_username,),
            ).fetchone()

        if not row or not check_password_hash(row["password_hash"], normalized_password):
            return None, "invalid"
        if row["status"] != "approved":
            return None, row["status"]
        return {
            "username": row["username"],
            "is_admin": bool(row["is_admin"]),
            "status": row["status"],
        }, "approved"

    def has_users(self):
        with self.lock, self._connect() as connection:
            row = connection.execute("SELECT COUNT(*) AS total FROM users").fetchone()
        return bool(row and row["total"] > 0)

    def get_user(self, username):
        normalized_username = str(username or "").strip()
        if not normalized_username:
            return None
        with self.lock, self._connect() as connection:
            row = connection.execute(
                """
                SELECT username, status, is_admin, created_at, approved_at, approved_by
                FROM users
                WHERE username = ?
                """,
                (normalized_username,),
            ).fetchone()
        if not row:
            return None
        return {
            "username": row["username"],
            "status": row["status"],
            "is_admin": bool(row["is_admin"]),
            "created_at": row["created_at"],
            "approved_at": row["approved_at"],
            "approved_by": row["approved_by"],
        }

    def list_users(self):
        with self.lock, self._connect() as connection:
            rows = connection.execute(
                """
                SELECT username, status, is_admin, created_at, approved_at, approved_by
                FROM users
                ORDER BY
                    CASE status WHEN 'pending' THEN 0 WHEN 'approved' THEN 1 ELSE 2 END,
                    created_at ASC
                """
            ).fetchall()
        return [
            {
                "username": row["username"],
                "status": row["status"],
                "is_admin": bool(row["is_admin"]),
                "created_at": row["created_at"],
                "approved_at": row["approved_at"],
                "approved_by": row["approved_by"],
            }
            for row in rows
        ]

    def approve_user(self, username, approved_by):
        normalized_username = self.validate_username(username)
        approver_username = self.validate_username(approved_by)
        now = time.time()
        with self.lock, self._connect() as connection:
            row = connection.execute(
                "SELECT status FROM users WHERE username = ?",
                (normalized_username,),
            ).fetchone()
            if not row:
                raise ValueError("Pengguna tidak ditemukan.")
            if row["status"] == "approved":
                return
            connection.execute(
                """
                UPDATE users
                SET status = 'approved',
                    approved_at = ?,
                    approved_by = ?
                WHERE username = ?
                """,
                (now, approver_username, normalized_username),
            )
            connection.commit()

    def reject_user(self, username, approved_by):
        normalized_username = self.validate_username(username)
        approver_username = self.validate_username(approved_by)
        now = time.time()
        with self.lock, self._connect() as connection:
            row = connection.execute(
                "SELECT is_admin FROM users WHERE username = ?",
                (normalized_username,),
            ).fetchone()
            if not row:
                raise ValueError("Pengguna tidak ditemukan.")
            if row["is_admin"]:
                raise ValueError("Akun admin tidak dapat ditolak.")
            connection.execute(
                """
                UPDATE users
                SET status = 'rejected',
                    approved_at = ?,
                    approved_by = ?
                WHERE username = ?
                """,
                (now, approver_username, normalized_username),
            )
            connection.commit()


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
