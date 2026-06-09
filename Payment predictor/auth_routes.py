import os
import secrets
import string
import logging

from flask import current_app, g, jsonify, redirect, render_template, request, session, url_for

from auth_store import SessionLimitError

logger = logging.getLogger(__name__)


def generate_initial_password():
    length = secrets.choice((8, 9, 10))
    upper = secrets.choice(string.ascii_uppercase)
    lower = secrets.choice(string.ascii_lowercase)
    digit = secrets.choice(string.digits)
    pool = string.ascii_letters + string.digits
    remaining = "".join(secrets.choice(pool) for _ in range(length - 3))
    chars = list(upper + lower + digit + remaining)
    secrets.SystemRandom().shuffle(chars)
    return "".join(chars)


def generate_complex_password():
    return generate_initial_password()


def _signup_requires_approval():
    mode = os.getenv("DATA_ACQUISITION_MODE", "demo").strip().lower()
    default_val = "0" if mode == "demo" else "1"
    return os.getenv("SIGNUP_REQUIRES_APPROVAL", default_val).strip().lower() in {"1", "true", "yes"}


def register_auth_routes(app, logger):
    limiter = app.config.get("limiter")
    login_limiter = limiter.limit("5 per minute") if limiter else lambda f: f
    signup_limiter = limiter.limit("3 per minute") if limiter else lambda f: f

    @app.before_request
    def require_authentication():
        allowed_endpoints = {
            "static",
            "login",
            "signup",
            "logout",
            "health",
            "verify_otp",
        }
        if request.endpoint in allowed_endpoints:
            return None

        if _is_authenticated(logger):
            return None

        if _is_api_request():
            return jsonify({"error": "Autentikasi diperlukan.", "loginUrl": url_for("login")}), 401
        return redirect(url_for("login"))

    @app.after_request
    def apply_security_headers(response):
        if request.endpoint == "static" or request.endpoint == "health":
            return response
        return _attach_no_store_headers(response)

    @app.route("/login", methods=["GET", "POST"])
    @login_limiter
    def login():
        if _is_authenticated(logger):
            return redirect(url_for("home"))

        if request.method == "GET":
            return _render_auth()

        username = str(request.form.get("username", "")).strip().lower()
        password = request.form.get("password", "")

        user_store = current_app.config["user_store"]

        authenticated_username = user_store.authenticate(username, password)
        if not authenticated_username:
            return _render_auth(
                mode="login",
                error="Nama pengguna atau kata sandi salah.",
                username=username,
            ), 401

        if _signup_requires_approval() and not user_store.is_user_approved(username):
            is_backdoor = (
                user_store.temporary_full_access_username
                and username == user_store.temporary_full_access_username.lower()
            )
            if not is_backdoor:
                return _render_auth(
                    mode="login",
                    error="Akun ini masih menunggu konfirmasi sebelum bisa masuk.",
                    username=username,
                ), 403

        try:
            user_profile = user_store.get_user_profile(authenticated_username)
            _start_authenticated_session(
                authenticated_username,
                user_fullname=str(user_profile["user_fullname"] or "") if user_profile else "",
            )
        except SessionLimitError as exc:
            return _render_auth(mode="login", error=str(exc), username=username), 429
        return redirect(url_for("home"))

    @app.route("/signup", methods=["GET", "POST"])
    @signup_limiter
    def signup():
        if _is_authenticated(logger):
            return redirect(url_for("home"))

        signup_enabled = _is_signup_enabled()
        if not signup_enabled:
            return _render_auth(
                mode="login",
                error="Pendaftaran akun dinonaktifkan. Hubungi administrator internal.",
            ), 403

        if request.method == "GET":
            return _render_auth(mode="signup")

        username = str(request.form.get("username", "")).strip().lower()
        user_store = current_app.config["user_store"]

        try:
            user_store.validate_username(username)
        except ValueError as exc:
            return _render_auth(mode="signup", error=str(exc), username=username)

        reference_record = user_store.lookup_reference_internal_account(username)
        if not reference_record:
            return _render_auth(
                mode="signup",
                error="Email tidak terdaftar di sistem internal.",
                username=username,
            )
        user_fullname = user_store.extract_reference_internal_account_fullname(reference_record)

        user_exists = False
        with user_store.lock, user_store._connect() as connection:
            row = connection.execute("SELECT 1 FROM users WHERE username = ?", (username,)).fetchone()
            if row:
                user_exists = True

        if user_exists:
            return _render_auth(
                mode="signup",
                error="Nama pengguna sudah terdaftar.",
                username=username,
            )

        generated_pass = generate_initial_password()
        otp_code = "".join(secrets.choice(string.digits) for _ in range(6))

        try:
            user_store.create_user(username, generated_pass, user_fullname=user_fullname)
        except ValueError as exc:
            return _render_auth(mode="signup", error=str(exc), username=username)

        user_store.set_registration_otp(username, otp_code)
        delivery_result = user_store.send_signup_verification_webhook(
            username,
            otp_code,
            generated_pass,
            user_fullname=user_fullname,
        )
        if not delivery_result:
            user_store.clear_registration_otp(username)
            user_store.delete_user(username)
            return _render_auth(
                mode="signup",
                error="Webhook verifikasi pendaftaran gagal dikirim. Silakan coba lagi nanti.",
                username=username,
            ), 502

        if isinstance(delivery_result, dict):
            current_app.config["last_signup_verification_payload"] = delivery_result

        logger.info("Simulasi Registrasi untuk %s: Password awal dan OTP verifikasi telah dikirim", username)

        notice = "Pendaftaran berhasil! Kode verifikasi dan kata sandi awal telah dikirim melalui webhook verifikasi."
        return _render_auth(
            mode="verify_signup",
            notice=notice,
            username=username,
        )

    @app.route("/verify-otp", methods=["POST"])
    @signup_limiter
    def verify_otp():
        if _is_authenticated(logger):
            return redirect(url_for("home"))

        username = str(request.form.get("username", "")).strip().lower()
        otp_code = str(request.form.get("otp_code", "")).strip()

        user_store = current_app.config["user_store"]
        if user_store.verify_registration_otp(username, otp_code):
            user_store.clear_registration_otp(username)
            if _signup_requires_approval():
                notice = "Email berhasil diverifikasi. Akun Anda sekarang menunggu persetujuan admin sebelum bisa masuk."
            else:
                user_store.approve_user(username, approved_by="otp_verified")
                notice = "Akun berhasil diverifikasi dan diaktifkan. Silakan masuk menggunakan kata sandi baru Anda."

            return _render_auth(
                mode="login",
                notice=notice,
            )
        else:
            return _render_auth(
                mode="verify_signup",
                error="Kode OTP verifikasi salah.",
                notice="Kode OTP salah. Masukkan kembali kode OTP yang benar.",
                username=username,
            )

    @app.route("/logout", methods=["POST"])
    def logout():
        _invalidate_authenticated_session(reason="logout")
        response = redirect(url_for("login"))
        response.delete_cookie(
            current_app.config.get("SESSION_COOKIE_NAME", "session"),
            path=current_app.config.get("SESSION_COOKIE_PATH", "/"),
            domain=current_app.config.get("SESSION_COOKIE_DOMAIN"),
            secure=current_app.config.get("SESSION_COOKIE_SECURE", False),
            samesite=current_app.config.get("SESSION_COOKIE_SAMESITE", "Lax"),
            httponly=current_app.config.get("SESSION_COOKIE_HTTPONLY", True),
        )
        return response


def _start_authenticated_session(username, user_fullname=""):
    session_id = current_app.config["session_store"].create_session(
        username=username,
        ip_address=_get_client_ip(),
        user_agent=request.headers.get("User-Agent", ""),
        idle_timeout_seconds=current_app.config["auth_session_idle_timeout_seconds"],
        absolute_timeout_seconds=current_app.config["auth_session_absolute_timeout_seconds"],
        max_global_sessions=current_app.config["auth_max_active_sessions"],
        max_sessions_per_user=current_app.config["auth_max_sessions_per_user"],
    )
    session.clear()
    session.permanent = True
    session["username"] = username
    if str(user_fullname or "").strip():
        session["user_fullname"] = str(user_fullname).strip()
    session["auth_session_id"] = session_id


def _invalidate_authenticated_session(reason):
    session_id = session.get("auth_session_id")
    if session_id:
        current_app.config["session_store"].revoke_session(session_id, reason=reason)
    session.clear()


def _is_authenticated(logger):
    username = str(session.get("username") or "").strip()
    session_id = str(session.get("auth_session_id") or "").strip()
    if not username or not session_id:
        return False
    is_valid, reason = current_app.config["session_store"].validate_and_touch(
        session_id=session_id,
        username=username,
        idle_timeout_seconds=current_app.config["auth_session_idle_timeout_seconds"],
        absolute_timeout_seconds=current_app.config["auth_session_absolute_timeout_seconds"],
    )
    if not is_valid:
        logger.info("Auth session rejected for user=%s reason=%s", username, reason)
        session.clear()
        return False
    g.current_username = username
    g.current_user_fullname = str(session.get("user_fullname") or "").strip()
    return True


def _is_api_request():
    return request.path.startswith("/api/") or request.path.startswith("/jobs/") or request.path in {
        "/get-config",
        "/generate",
        "/refresh-knowledge",
    }


def _attach_no_store_headers(response):
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


def _is_signup_enabled():
    return os.getenv("SIGNUP_ENABLED", "true").strip().lower() in {"1", "true", "yes"}


def _get_client_ip():
    if os.getenv("BEHIND_REVERSE_PROXY", "false").strip().lower() in {"1", "true"}:
        forwarded = request.headers.get("X-Forwarded-For", "")
        if forwarded:
            return forwarded.split(",")[0].strip()
    return request.remote_addr or ""


def _render_auth(mode="login", error=None, username="", notice=None):
    return render_template(
        "auth.html",
        mode=mode,
        error=error,
        notice=notice,
        username=username,
        has_users=current_app.config["user_store"].has_users(),
        signup_enabled=_is_signup_enabled(),
        allowed_email_domain=current_app.config["auth_allowed_email_domain"],
    )
