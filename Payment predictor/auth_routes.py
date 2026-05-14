from flask import current_app, g, jsonify, redirect, render_template, request, session, url_for

from auth_store import SessionLimitError


def register_auth_routes(app, logger):
    @app.before_request
    def require_authentication():
        allowed_endpoints = {
            "static",
            "login",
            "signup",
            "logout",
            "health",
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
    def login():
        if _is_authenticated(logger):
            return redirect(url_for("home"))

        if request.method == "GET":
            return _render_auth()

        username = str(request.form.get("username", "")).strip()
        password = request.form.get("password", "")
        authenticated_username = current_app.config["user_store"].authenticate(username, password)
        if not authenticated_username:
            return _render_auth(
                mode="login",
                error="Nama pengguna atau kata sandi salah.",
                username=username,
            ), 401

        try:
            _start_authenticated_session(authenticated_username)
        except SessionLimitError as exc:
            return _render_auth(mode="login", error=str(exc), username=username), 429
        return redirect(url_for("home"))

    @app.route("/signup", methods=["GET", "POST"])
    def signup():
        if _is_authenticated(logger):
            return redirect(url_for("home"))

        signup_enabled = _is_signup_enabled()
        if request.method == "GET":
            if not signup_enabled:
                return _render_auth(
                    mode="login",
                    error="Pendaftaran akun dinonaktifkan. Hubungi administrator internal.",
                ), 403
            return _render_auth(mode="signup")

        if not signup_enabled:
            return _render_auth(
                mode="login",
                error="Pendaftaran akun dinonaktifkan. Hubungi administrator internal.",
            ), 403

        username = str(request.form.get("username", "")).strip()
        password = request.form.get("password", "")
        confirm_password = request.form.get("confirm_password", "")
        if password != confirm_password:
            return _render_auth(
                mode="signup",
                error="Konfirmasi kata sandi tidak cocok.",
                username=username,
            )

        try:
            created_username = current_app.config["user_store"].create_user(username, password)
        except ValueError as exc:
            return _render_auth(mode="signup", error=str(exc), username=username)

        try:
            _start_authenticated_session(created_username)
        except SessionLimitError as exc:
            return _render_auth(mode="signup", error=str(exc), username=username), 429
        return redirect(url_for("home"))

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


def _start_authenticated_session(username):
    session_id = current_app.config["session_store"].create_session(
        username=username,
        ip_address=request.headers.get("X-Forwarded-For", request.remote_addr or ""),
        user_agent=request.headers.get("User-Agent", ""),
        idle_timeout_seconds=current_app.config["auth_session_idle_timeout_seconds"],
        absolute_timeout_seconds=current_app.config["auth_session_absolute_timeout_seconds"],
        max_global_sessions=current_app.config["auth_max_active_sessions"],
        max_sessions_per_user=current_app.config["auth_max_sessions_per_user"],
    )
    session.clear()
    session.permanent = True
    session["username"] = username
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
    return True


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
