import argparse
import calendar
import logging
import os
from datetime import datetime, timedelta

from flask import Flask, current_app, g, jsonify, redirect, render_template, request, send_file, session, url_for
from flask_cors import CORS

from auth_store import ActiveSessionStore, SessionLimitError, UserStore
from report_jobs import QueueCapacityError, ReportJobManager, ReportJobStore
from runtime_services import BackgroundRefreshCoordinator, ForecastSnapshotCache

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def create_app():
    from config import (
        APP_SECRET_KEY,
        AUTH_ALLOWED_EMAIL_DOMAIN,
        AUTH_MAX_ACTIVE_SESSIONS,
        AUTH_MAX_SESSIONS_PER_USER,
        AUTH_SESSION_ABSOLUTE_TIMEOUT_HOURS,
        AUTH_SESSION_IDLE_TIMEOUT_MINUTES,
        DATA_REFRESH_INTERVAL_SECONDS,
        DB_URI,
        FORECAST_CACHE_TTL_SECONDS,
        JOB_STATE_DB_PATH,
        PERMANENT_SESSION_LIFETIME,
        REPORT_ARTIFACTS_DIR,
        REPORT_JOB_RETENTION_SECONDS,
        REPORT_MAX_CONCURRENT_JOBS,
        REPORT_MAX_PENDING_JOBS,
        REPORT_MIN_COMPLETENESS_SCORE,
        REPORT_METRICS_WINDOW_HOURS,
        REPORT_STATUS_POLL_INTERVAL_MS,
        SESSION_COOKIE_SECURE,
        SMART_SUGGESTIONS,
    )
    from core import CashOutStore, KnowledgeBase, ReportGenerator, Researcher
    from data_sources import summarize_source_profile
    from forecast_engine import CashflowForecaster, parse_idr_amount
    from internal_api_connection import connect_internal_data_source as connect_internal_data_source_service

    app = Flask(__name__)
    app.secret_key = APP_SECRET_KEY
    app.config["SESSION_COOKIE_HTTPONLY"] = True
    app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
    app.config["SESSION_COOKIE_SECURE"] = SESSION_COOKIE_SECURE
    app.config["PERMANENT_SESSION_LIFETIME"] = PERMANENT_SESSION_LIFETIME
    CORS(app)

    knowledge_base = KnowledgeBase(DB_URI)
    cash_out_store = CashOutStore()
    report_generator = ReportGenerator(knowledge_base)
    job_store = ReportJobStore(JOB_STATE_DB_PATH, REPORT_ARTIFACTS_DIR)
    user_store = UserStore(JOB_STATE_DB_PATH, allowed_email_domain=AUTH_ALLOWED_EMAIL_DOMAIN)
    session_store = ActiveSessionStore(JOB_STATE_DB_PATH)
    forecast_cache = ForecastSnapshotCache(FORECAST_CACHE_TTL_SECONDS)
    job_manager = ReportJobManager(
        report_generator=report_generator,
        max_workers=REPORT_MAX_CONCURRENT_JOBS,
        max_pending_jobs=REPORT_MAX_PENDING_JOBS,
        retention_seconds=REPORT_JOB_RETENTION_SECONDS,
        artifacts_dir=REPORT_ARTIFACTS_DIR,
        job_store=job_store,
        metrics_window_hours=REPORT_METRICS_WINDOW_HOURS,
    )
    
    # Initialize forecaster
    forecaster = CashflowForecaster(monthly_operating_cost_idr=200_000_000)
    refresh_coordinator = BackgroundRefreshCoordinator(
        knowledge_base=knowledge_base,
        cash_out_store=cash_out_store,
        forecast_cache=forecast_cache,
        interval_seconds=DATA_REFRESH_INTERVAL_SECONDS,
    )
    refresh_coordinator.start()

    # Boot-time endpoint validation (non-blocking, logs warnings)
    if knowledge_base.internal_api_client and knowledge_base.internal_api_client.is_configured():
        ok, msg = knowledge_base.internal_api_client.validate_endpoint_url()
        if ok:
            logger.info("Boot check: %s", msg)
        else:
            logger.warning("Boot check: %s", msg)
    if cash_out_store.client.is_configured():
        ok, msg = cash_out_store.client.validate_endpoint_url()
        if ok:
            logger.info("Boot check (cash-out): %s", msg)
        else:
            logger.warning("Boot check (cash-out): %s", msg)

    app.config["knowledge_base"] = knowledge_base
    app.config["cash_out_store"] = cash_out_store
    app.config["job_manager"] = job_manager
    app.config["forecaster"] = forecaster
    app.config["user_store"] = user_store
    app.config["session_store"] = session_store
    app.config["min_completeness_score"] = REPORT_MIN_COMPLETENESS_SCORE
    app.config["status_poll_interval_ms"] = REPORT_STATUS_POLL_INTERVAL_MS
    app.config["forecast_cache"] = forecast_cache
    app.config["data_refresh_interval_seconds"] = DATA_REFRESH_INTERVAL_SECONDS
    app.config["refresh_coordinator"] = refresh_coordinator
    app.config["auth_max_active_sessions"] = max(int(AUTH_MAX_ACTIVE_SESSIONS), 1)
    app.config["auth_max_sessions_per_user"] = max(int(AUTH_MAX_SESSIONS_PER_USER), 1)
    app.config["auth_session_idle_timeout_seconds"] = max(int(AUTH_SESSION_IDLE_TIMEOUT_MINUTES), 1) * 60
    app.config["auth_session_absolute_timeout_seconds"] = max(int(AUTH_SESSION_ABSOLUTE_TIMEOUT_HOURS), 1) * 3600
    app.config["auth_allowed_email_domain"] = AUTH_ALLOWED_EMAIL_DOMAIN

    def _start_authenticated_session(username):
        session_id = session_store.create_session(
            username=username,
            ip_address=request.headers.get("X-Forwarded-For", request.remote_addr or ""),
            user_agent=request.headers.get("User-Agent", ""),
            idle_timeout_seconds=app.config["auth_session_idle_timeout_seconds"],
            absolute_timeout_seconds=app.config["auth_session_absolute_timeout_seconds"],
            max_global_sessions=app.config["auth_max_active_sessions"],
            max_sessions_per_user=app.config["auth_max_sessions_per_user"],
        )
        session.clear()
        session.permanent = True
        session["username"] = username
        session["auth_session_id"] = session_id

    def _invalidate_authenticated_session(reason):
        session_id = session.get("auth_session_id")
        if session_id:
            session_store.revoke_session(session_id, reason=reason)
        session.clear()

    def _is_authenticated():
        username = str(session.get("username") or "").strip()
        session_id = str(session.get("auth_session_id") or "").strip()
        if not username or not session_id:
            return False
        is_valid, reason = session_store.validate_and_touch(
            session_id=session_id,
            username=username,
            idle_timeout_seconds=app.config["auth_session_idle_timeout_seconds"],
            absolute_timeout_seconds=app.config["auth_session_absolute_timeout_seconds"],
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

        if _is_authenticated():
            return None

        if _is_api_request():
            return jsonify({"error": "Autentikasi diperlukan.", "loginUrl": url_for("login")}), 401
        return redirect(url_for("login"))

    @app.after_request
    def apply_security_headers(response):
        if request.endpoint == "static" or request.endpoint == "health":
            return response
        return _attach_no_store_headers(response)

    def _render_auth(mode="login", error=None, username="", notice=None):
        return render_template(
            "auth.html",
            mode=mode,
            error=error,
            notice=notice,
            username=username,
            has_users=user_store.has_users(),
            signup_enabled=_is_signup_enabled(),
            allowed_email_domain=app.config["auth_allowed_email_domain"],
        )

    @app.route("/login", methods=["GET", "POST"])
    def login():
        if _is_authenticated():
            return redirect(url_for("home"))

        if request.method == "GET":
            return _render_auth(mode="login")

        username = str(request.form.get("username", "")).strip()
        password = request.form.get("password", "")
        authenticated_username = user_store.authenticate(username, password)
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
        if _is_authenticated():
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
            created_username = user_store.create_user(username, password)
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
            app.config.get("SESSION_COOKIE_NAME", "session"),
            path=app.config.get("SESSION_COOKIE_PATH", "/"),
            domain=app.config.get("SESSION_COOKIE_DOMAIN"),
            secure=app.config.get("SESSION_COOKIE_SECURE", False),
            samesite=app.config.get("SESSION_COOKIE_SAMESITE", "Lax"),
            httponly=app.config.get("SESSION_COOKIE_HTTPONLY", True),
        )
        return response

    def _build_forecast_periods(month_count=3):
        base_date = datetime.now().replace(day=1)
        periods = []
        windows = [
            (1, 10, "1-10"),
            (11, 20, "11-20"),
            (21, None, "21-akhir bulan"),
        ]

        for offset in range(month_count):
            year = base_date.year + ((base_date.month - 1 + offset) // 12)
            month = ((base_date.month - 1 + offset) % 12) + 1
            first_day = datetime(year, month, 1)
            last_day = calendar.monthrange(year, month)[1]

            for start_day, end_day, label in windows:
                start = first_day.replace(day=start_day)
                resolved_end_day = last_day if end_day is None else min(end_day, last_day)
                end = first_day.replace(day=resolved_end_day)
                periods.append(
                    {
                        "id": f"{year}-{month:02d}_{label.replace(' ', '_')}",
                        "label": f"{label} {first_day.strftime('%B %Y')}",
                        "start": start.isoformat(),
                        "end": end.isoformat(),
                    }
                )

        return periods

    def _build_external_context(start_date, end_date):
        dataset = knowledge_base.df
        partner_types = []
        services = []
        if dataset is not None and not dataset.empty:
            partner_column = next(
                (column for column in dataset.columns if str(column).strip().lower() in {"tipe partner", "partner type", "partner_type"}),
                None,
            )
            service_column = next(
                (column for column in dataset.columns if str(column).strip().lower() in {"layanan", "service", "service_name"}),
                None,
            )
            if partner_column:
                partner_types = (
                    dataset[partner_column]
                    .dropna()
                    .astype(str)
                    .value_counts()
                    .head(3)
                    .index
                    .tolist()
                )
            if service_column:
                services = (
                    dataset[service_column]
                    .dropna()
                    .astype(str)
                    .value_counts()
                    .head(3)
                    .index
                    .tolist()
                )
        partner_snippet = ", ".join(partner_types)
        service_snippet = ", ".join(services)
        return (
            f"periode {start_date.strftime('%d %B %Y')} sampai {end_date.strftime('%d %B %Y')} "
            f"partner {partner_snippet} "
            f"layanan {service_snippet}"
        ).strip()

    def _parse_request_idr_amount(raw_value, field_name, default_value):
        value = default_value if raw_value is None else raw_value
        try:
            return parse_idr_amount(value)
        except ValueError as exc:
            raise ValueError(
                f"{field_name} must be provided in Rupiah (IDR) only. Foreign currencies are not supported."
            ) from exc

    def _validate_currency_code(payload):
        currency = str(payload.get("currency", "IDR")).strip().upper()
        if currency not in {"IDR", "RP", "RUPIAH"}:
            raise ValueError("This app only accepts Rupiah (IDR) amounts.")
        return "IDR"

    def _build_sync_snapshot():
        refresh_interval = current_app.config["data_refresh_interval_seconds"]
        knowledge_status = current_app.config["knowledge_base"].get_sync_status(refresh_interval)
        cash_out_status = current_app.config["cash_out_store"].get_status(refresh_interval)
        return {
            "financialData": knowledge_status,
            "cashOutSource": cash_out_status,
            "refreshIntervalSeconds": refresh_interval,
        }

    def _get_cash_out_records():
        return current_app.config["cash_out_store"].get_records()

    def _build_forecast_cache_key(kind, cash_on_hand, monthly_cost, start_date, end_date=None):
        knowledge_state = current_app.config["knowledge_base"].get_sync_status()
        cash_out_state = current_app.config["cash_out_store"].get_status()
        return (
            kind,
            knowledge_state["dataVersion"],
            cash_out_state["version"],
            int(cash_on_hand),
            int(monthly_cost),
            start_date.isoformat(),
            end_date.isoformat() if end_date else None,
        )

    def _get_or_build_single_forecast(cash_on_hand, monthly_cost, start_date, end_date):
        cache_key = _build_forecast_cache_key("single_forecast", cash_on_hand, monthly_cost, start_date, end_date)
        cached_value = current_app.config["forecast_cache"].get(cache_key)
        if cached_value is not None:
            return cached_value

        forecaster = CashflowForecaster(monthly_operating_cost_idr=monthly_cost)
        forecast = forecaster.forecast(
            df=current_app.config["knowledge_base"].df,
            cash_on_hand=cash_on_hand,
            start_date=start_date,
            end_date=end_date,
            cash_out_records=_get_cash_out_records(),
        )
        return current_app.config["forecast_cache"].set(cache_key, forecast)

    def _get_or_build_horizon_forecasts(cash_on_hand, monthly_cost, start_date):
        cache_key = _build_forecast_cache_key("horizon_forecast", cash_on_hand, monthly_cost, start_date)
        cached_value = current_app.config["forecast_cache"].get(cache_key)
        if cached_value is not None:
            return cached_value

        forecaster = CashflowForecaster(monthly_operating_cost_idr=monthly_cost)
        forecasts = forecaster.forecast_by_horizon(
            df=current_app.config["knowledge_base"].df,
            cash_on_hand=cash_on_hand,
            start_date=start_date,
            cash_out_records=_get_cash_out_records(),
        )
        return current_app.config["forecast_cache"].set(cache_key, forecasts)

    def _build_payment_class_trend():
        dataset = current_app.config["knowledge_base"].df
        if dataset is None or dataset.empty:
            return {"series": [], "topPeriods": []}

        resolved_columns = current_app.config["knowledge_base"].data_contract_summary.get("sourceColumns", {})
        period_column = resolved_columns.get("period")
        payment_class_column = resolved_columns.get("payment_class")
        invoice_value_column = resolved_columns.get("invoice_value")
        if not period_column or not payment_class_column or not invoice_value_column:
            return {"series": [], "topPeriods": []}

        working_df = dataset[[period_column, payment_class_column, invoice_value_column]].copy()
        working_df.columns = ["period", "payment_class", "invoice_value"]
        working_df["payment_class"] = working_df["payment_class"].astype(str).str.extract(r"(Kelas [A-E])", expand=False).fillna("Tidak Diketahui")
        working_df["invoice_value"] = working_df["invoice_value"].apply(
            lambda value: parse_idr_amount(value) if value is not None and str(value).strip() else 0
        )
        working_df["period"] = working_df["period"].astype(str).fillna("Tidak Diketahui")

        grouped = (
            working_df.groupby(["period", "payment_class"], as_index=False)
            .agg(amount=("invoice_value", "sum"), invoice_count=("invoice_value", "size"))
        )
        period_totals = (
            grouped.groupby("period", as_index=False)
            .agg(total_amount=("amount", "sum"))
            .sort_values("total_amount", ascending=False)
        )
        return {
            "series": grouped.to_dict(orient="records"),
            "topPeriods": period_totals.head(10).to_dict(orient="records"),
        }

    def _build_concentration_view(invoices):
        if not invoices:
            return {"partners": [], "services": []}

        partner_totals = {}
        service_totals = {}
        total_amount = sum(invoice["amount"] for invoice in invoices) or 1

        for invoice in invoices:
            partner = invoice["partner_type"] or "Tidak Diketahui"
            service = invoice["service"] or "Tidak Diketahui"
            partner_totals[partner] = partner_totals.get(partner, 0) + invoice["amount"]
            service_totals[service] = service_totals.get(service, 0) + invoice["amount"]

        def _rank_items(source_map):
            return [
                {
                    "label": label,
                    "amount": amount,
                    "sharePct": round((amount / total_amount) * 100, 1),
                }
                for label, amount in sorted(source_map.items(), key=lambda item: item[1], reverse=True)[:10]
            ]

        return {
            "partners": _rank_items(partner_totals),
            "services": _rank_items(service_totals),
        }

    @app.route("/")
    def home():
        return render_template(
            "index.html",
            current_username=session.get("username", ""),
        )

    @app.route("/settings")
    def data_settings():
        return render_template(
            "data_settings.html",
            current_username=session.get("username", ""),
        )

    @app.route("/get-config")
    def get_config():
        active_knowledge_base = current_app.config["knowledge_base"]
        if active_knowledge_base.df is None or active_knowledge_base.df.empty:
            return jsonify(
                {
                    "error": "Financial data is currently unavailable.",
                    "syncStatus": _build_sync_snapshot(),
                    "dataSourceContract": active_knowledge_base.get_internal_data_contract(),
                    "authSecurity": session_store.get_security_snapshot(
                        idle_timeout_seconds=app.config["auth_session_idle_timeout_seconds"],
                        absolute_timeout_seconds=app.config["auth_session_absolute_timeout_seconds"],
                        max_global_sessions=app.config["auth_max_active_sessions"],
                        max_sessions_per_user=app.config["auth_max_sessions_per_user"],
                    ),
                }
            )
        review_context = active_knowledge_base.get_review_context()

        return jsonify(
            {
                "suggestions": SMART_SUGGESTIONS,
                "statusPollIntervalMs": current_app.config["status_poll_interval_ms"],
                "reviewContext": review_context,
                "syncStatus": _build_sync_snapshot(),
                "dataSourceContract": active_knowledge_base.get_internal_data_contract(),
                "authSecurity": session_store.get_security_snapshot(
                    idle_timeout_seconds=app.config["auth_session_idle_timeout_seconds"],
                    absolute_timeout_seconds=app.config["auth_session_absolute_timeout_seconds"],
                    max_global_sessions=app.config["auth_max_active_sessions"],
                    max_sessions_per_user=app.config["auth_max_sessions_per_user"],
                ),
            }
        )

    @app.route("/generate", methods=["POST"])
    def generate_doc():
        payload = request.get_json(silent=True) or {}
        notes = payload.get("notes", "")
        analysis_context = (payload.get("analysis_context") or "").strip()
        analysis_payload = payload.get("analysis_payload") if isinstance(payload.get("analysis_payload"), dict) else None
        active_job_manager = current_app.config["job_manager"]
        try:
            job_id = active_job_manager.submit(notes, analysis_context, analysis_payload=analysis_payload)
        except QueueCapacityError as exc:
            return (
                jsonify(
                    {
                        "error": str(exc),
                        "activeJobs": exc.active_jobs,
                        "maxPendingJobs": exc.max_pending_jobs,
                    }
                ),
                429,
            )
        return jsonify({"jobId": job_id}), 202

    @app.route("/jobs/<job_id>")
    def get_job_status(job_id):
        active_job_manager = current_app.config["job_manager"]
        status = active_job_manager.get_status(job_id)
        if status is None:
            return jsonify({"error": "Job not found."}), 404
        return jsonify(status)

    @app.route("/jobs/<job_id>/download")
    def download_job(job_id):
        active_job_manager = current_app.config["job_manager"]
        download_payload = active_job_manager.get_download(job_id)
        if download_payload is None:
            return jsonify({"error": "Job not found."}), 404
        if "artifactPath" not in download_payload:
            return jsonify(download_payload), 409
        return send_file(
            download_payload["artifactPath"],
            as_attachment=True,
            download_name=f"{download_payload['filename']}.docx",
            mimetype="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        )

    @app.route("/refresh-knowledge", methods=["POST"])
    def refresh_knowledge():
        refresh_result = current_app.config["refresh_coordinator"].refresh_all()
        return jsonify(
            {
                "status": "success" if refresh_result["knowledgeBase"] else "error",
                "refreshResult": refresh_result,
                "syncStatus": _build_sync_snapshot(),
            }
        )

    @app.route("/health")
    def health():
        health_snapshot = current_app.config["job_manager"].get_health()
        internal_data_contract = current_app.config["knowledge_base"].get_internal_data_contract()
        health_snapshot["dataReady"] = bool(
            current_app.config["knowledge_base"].df is not None
            and not current_app.config["knowledge_base"].df.empty
        )
        health_snapshot["internalDataContractReady"] = bool(
            internal_data_contract.get("currentSummary", {}).get("isReady")
        )
        health_snapshot["minimumCompletenessScore"] = current_app.config["min_completeness_score"]
        health_snapshot["authSecurity"] = session_store.get_security_snapshot(
            idle_timeout_seconds=app.config["auth_session_idle_timeout_seconds"],
            absolute_timeout_seconds=app.config["auth_session_absolute_timeout_seconds"],
            max_global_sessions=app.config["auth_max_active_sessions"],
            max_sessions_per_user=app.config["auth_max_sessions_per_user"],
        )
        health_snapshot["syncStatus"] = _build_sync_snapshot()
        return jsonify(health_snapshot)

    @app.route("/api/internal-data/contract", methods=["GET"])
    def get_internal_data_contract():
        return jsonify(current_app.config["knowledge_base"].get_internal_data_contract())

    @app.route("/api/internal-data/connect", methods=["POST"])
    def connect_internal_data_source():
        payload = request.get_json(silent=True) or {}
        try:
            response_payload, status_code = connect_internal_data_source_service(
                payload=payload,
                knowledge_base=current_app.config["knowledge_base"],
                forecast_cache=current_app.config["forecast_cache"],
                cash_out_store=current_app.config["cash_out_store"],
                sync_snapshot=_build_sync_snapshot,
            )
            return jsonify(response_payload), status_code
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        except Exception as exc:
            return jsonify(
                {
                    "ready": False,
                    "activated": False,
                    "profileSaved": False,
                    "message": str(exc),
                    "error": str(exc),
                    "syncStatus": _build_sync_snapshot(),
                }
            ), 400

    @app.route("/api/data-source/validate", methods=["POST"])
    def validate_data_source():
        payload = request.get_json(silent=True) or {}
        source_key = str(payload.get("sourceKey") or "").strip().lower()
        preview_mode = bool(payload.get("preview"))
        preview_rows = int(payload.get("previewRows") or 5)
        if not source_key:
            return jsonify({"error": "sourceKey wajib diisi."}), 400

        if preview_mode:
            # Dry-run: fetch limited records to verify mapping without loading full dataset
            try:
                active_kb = current_app.config["knowledge_base"]
                active_kb._reload_source_registry()
                profile = active_kb.source_registry.get(source_key)
                if not profile:
                    return jsonify({"error": f"Sumber data `{source_key}` tidak tersedia."}), 404

                from core import InternalAPIClient
                from data_contract import build_internal_data_summary, normalize_financial_dataframe
                if profile.get("type") == "json_api":
                    client = InternalAPIClient(source_profile=profile)
                    records, extraction_summary = client.fetch_records(preview_limit=preview_rows)
                    raw_df = active_kb._normalize_records(records)
                    if raw_df.empty:
                        return jsonify({
                            "preview": True,
                            "ready": False,
                            "message": "Preview fetch returned no records.",
                            "recordCount": 0,
                            "syncStatus": _build_sync_snapshot(),
                        })
                    _, data_summary = normalize_financial_dataframe(
                        raw_df, explicit_field_map=client.field_map,
                    )
                    sample_records = raw_df.head(preview_rows).to_dict(orient="records")
                    return jsonify({
                        "preview": True,
                        "ready": bool(data_summary.get("isReady")),
                        "message": "Preview berhasil." if data_summary.get("isReady") else "Field wajib belum lengkap.",
                        "recordCount": len(records),
                        "previewRows": len(sample_records),
                        "sampleRecords": sample_records,
                        "contractSummary": data_summary,
                        "extractionSummary": extraction_summary,
                        "syncStatus": _build_sync_snapshot(),
                    })
                else:
                    return jsonify({
                        "preview": True,
                        "message": "Preview hanya tersedia untuk sumber tipe json_api.",
                        "syncStatus": _build_sync_snapshot(),
                    })
            except Exception as exc:
                return jsonify({
                    "preview": True,
                    "ready": False,
                    "message": str(exc),
                    "syncStatus": _build_sync_snapshot(),
                }), 400

        try:
            validation = current_app.config["knowledge_base"].validate_source(source_key)
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 404
        return jsonify(
            {
                **validation,
                "syncStatus": _build_sync_snapshot(),
            }
        )

    @app.route("/api/data-source/activate", methods=["POST"])
    def activate_data_source():
        payload = request.get_json(silent=True) or {}
        source_key = str(payload.get("sourceKey") or "").strip().lower()
        if not source_key:
            return jsonify({"error": "sourceKey wajib diisi."}), 400

        activation = current_app.config["knowledge_base"].activate_source(source_key)
        current_app.config["forecast_cache"].clear()
        current_app.config["cash_out_store"].refresh_data()
        response_payload = {
            **activation,
            "syncStatus": _build_sync_snapshot(),
            "reviewContext": current_app.config["knowledge_base"].get_review_context()
            if activation.get("activated")
            else None,
        }
        if not activation.get("activated"):
            return jsonify(response_payload), 409
        return jsonify(response_payload)

    @app.route("/api/data-source/reload-profiles", methods=["POST"])
    def reload_data_source_profiles():
        """Reload source profiles from disk/env without restarting the app."""
        try:
            active_kb = current_app.config["knowledge_base"]
            active_kb._reload_source_registry()
            return jsonify({
                "reloaded": True,
                "activeSourceKey": active_kb.active_source_key,
                "availableSources": [
                    summarize_source_profile(profile)
                    for _, profile in sorted(active_kb.source_registry.items())
                ],
                "registryIssues": list(active_kb.source_registry_issues),
                "syncStatus": _build_sync_snapshot(),
            })
        except Exception as exc:
            return jsonify({"reloaded": False, "error": str(exc)}), 500

    @app.route("/api/data-source/check-connectivity", methods=["POST"])
    def check_data_source_connectivity():
        """Check if the configured API endpoint is reachable."""
        payload = request.get_json(silent=True) or {}
        source_key = str(payload.get("sourceKey") or "").strip().lower()

        active_kb = current_app.config["knowledge_base"]
        active_kb._reload_source_registry()
        profile = active_kb.source_registry.get(source_key)
        if not profile:
            return jsonify({"error": f"Sumber data `{source_key}` tidak tersedia."}), 404

        if profile.get("type") != "json_api":
            return jsonify({"reachable": True, "message": "Sumber CSV lokal tidak memerlukan koneksi jaringan."})

        from core import InternalAPIClient
        client = InternalAPIClient(source_profile=profile)
        ok, message = client.validate_endpoint_url()
        return jsonify({"reachable": ok, "message": message})

    # ==================== CASHFLOW FORECAST ENDPOINTS ====================
    
    @app.route("/api/forecast/periods", methods=["GET"])
    def get_forecast_periods():
        """Get available date range periods for forecasting"""
        return jsonify({"periods": _build_forecast_periods()})
    
    @app.route("/api/forecast", methods=["POST"])
    def generate_forecast():
        """
        Generate cashflow forecast
        Request body:
        {
            "period_id": "2026-03_week_1",
            "cash_on_hand": 500000000,
            "monthly_operating_cost": 200000000
        }
        """
        payload = request.get_json(silent=True) or {}
        
        # Get data
        knowledge_base = current_app.config["knowledge_base"]
        if knowledge_base.df is None or knowledge_base.df.empty:
            return jsonify({"error": "Financial data not available"}), 400
        
        # Parse inputs
        try:
            currency_code = _validate_currency_code(payload)
            cash_on_hand = _parse_request_idr_amount(payload.get("cash_on_hand"), "cash_on_hand", 500_000_000)
            monthly_cost = _parse_request_idr_amount(payload.get("monthly_operating_cost"), "monthly_operating_cost", 200_000_000)
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        
        # Parse period dates
        try:
            start_iso = payload.get("start_date")
            end_iso = payload.get("end_date")
            
            if not start_iso or not end_iso:
                return jsonify({"error": "start_date and end_date required"}), 400
            
            start_date = datetime.fromisoformat(start_iso)
            end_date = datetime.fromisoformat(end_iso)
        except (ValueError, TypeError):
            return jsonify({"error": "Invalid date format (use ISO format)"}), 400
        
        # Generate forecast
        try:
            forecast = _get_or_build_single_forecast(
                cash_on_hand=cash_on_hand,
                monthly_cost=monthly_cost,
                start_date=start_date,
                end_date=end_date,
            )
            forecast["currency"] = currency_code
            forecast["external_factors"] = Researcher.get_payment_delay_risks(
                _build_external_context(start_date, end_date)
            )
            forecast["sync_status"] = _build_sync_snapshot()
            return jsonify(forecast)
        except Exception as e:
            logger.error(f"Forecast error: {e}", exc_info=True)
            return jsonify({"error": str(e)}), 500
    
    @app.route("/api/forecast/by-horizon", methods=["POST"])
    def generate_forecast_by_horizon():
        """
        Generate cashflow forecasts for all time horizons (0-30d, 1-3m, 3-12m)
        Request body:
        {
            "cash_on_hand": 500000000,
            "monthly_operating_cost": 200000000,
            "start_date": "2026-03-31"
        }
        """
        payload = request.get_json(silent=True) or {}
        
        # Get data
        knowledge_base = current_app.config["knowledge_base"]
        if knowledge_base.df is None or knowledge_base.df.empty:
            return jsonify({"error": "Financial data not available"}), 400
        
        # Parse inputs
        try:
            currency_code = _validate_currency_code(payload)
            cash_on_hand = _parse_request_idr_amount(payload.get("cash_on_hand"), "cash_on_hand", 500_000_000)
            monthly_cost = _parse_request_idr_amount(payload.get("monthly_operating_cost"), "monthly_operating_cost", 200_000_000)
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        start_date_iso = payload.get("start_date")
        
        if not start_date_iso:
            start_date = datetime.now()
        else:
            try:
                start_date = datetime.fromisoformat(start_date_iso)
            except (ValueError, TypeError):
                return jsonify({"error": "Invalid start_date format (use ISO format)"}), 400
        
        # Generate forecasts for all horizons
        try:
            forecasts = _get_or_build_horizon_forecasts(
                cash_on_hand=cash_on_hand,
                monthly_cost=monthly_cost,
                start_date=start_date,
            )
            horizon_end = start_date + timedelta(days=365)
            return jsonify({
                'start_date': start_date.isoformat(),
                'cash_on_hand': cash_on_hand,
                'currency': currency_code,
                'forecasts': forecasts,
                'time_horizons': CashflowForecaster.TIME_HORIZONS,
                'external_factors': Researcher.get_payment_delay_risks(
                    _build_external_context(start_date, horizon_end)
                ),
                'sync_status': _build_sync_snapshot(),
            })
        except Exception as e:
            logger.error(f"Multi-horizon forecast error: {e}", exc_info=True)
            return jsonify({"error": str(e)}), 500
    
    @app.route("/api/forecast/outstanding", methods=["GET"])
    def get_outstanding():
        """Get outstanding invoices analysis"""
        knowledge_base = current_app.config["knowledge_base"]
        if knowledge_base.df is None or knowledge_base.df.empty:
            return jsonify({"error": "Financial data not available"}), 400
        
        try:
            forecaster = current_app.config["forecaster"]
            invoices = forecaster._parse_invoices(
                knowledge_base.df,
                start_date=datetime.now(),
                end_date=datetime.now(),
            )
            result = forecaster._analyze_outstanding(invoices)
            result["invoice_count"] = len(invoices)
            result["sync_status"] = _build_sync_snapshot()
            return jsonify(result)
        except Exception as e:
            logger.error(f"Outstanding analysis error: {e}", exc_info=True)
            return jsonify({"error": str(e)}), 500

    @app.route("/api/forecast/drilldown/top-overdue", methods=["POST"])
    def get_top_overdue_drilldown():
        payload = request.get_json(silent=True) or {}
        try:
            _validate_currency_code(payload)
            cash_on_hand = _parse_request_idr_amount(payload.get("cash_on_hand"), "cash_on_hand", 500_000_000)
            monthly_cost = _parse_request_idr_amount(payload.get("monthly_operating_cost"), "monthly_operating_cost", 200_000_000)
            start_date = datetime.fromisoformat(payload.get("start_date")) if payload.get("start_date") else datetime.now()
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        except TypeError:
            return jsonify({"error": "Invalid start_date format (use ISO format)"}), 400
        mode = str(payload.get("horizon", "short_term")).strip() or "short_term"
        forecasts = _get_or_build_horizon_forecasts(cash_on_hand, monthly_cost, start_date)
        active_forecast = forecasts.get(mode) or forecasts.get("short_term")
        dashboard_snapshot = active_forecast.get("dashboard_snapshot", {}) if active_forecast else {}
        return jsonify(
            {
                "horizon": mode,
                "items": dashboard_snapshot.get("top_overdue_accounts", []),
                "alertLines": dashboard_snapshot.get("alert_recommendation_lines", []),
                "sync_status": _build_sync_snapshot(),
            }
        )

    @app.route("/api/forecast/drilldown/payment-class-trend", methods=["GET"])
    def get_payment_class_trend_drilldown():
        return jsonify(
            {
                **_build_payment_class_trend(),
                "sync_status": _build_sync_snapshot(),
            }
        )

    @app.route("/api/forecast/drilldown/concentration", methods=["POST"])
    def get_concentration_drilldown():
        payload = request.get_json(silent=True) or {}
        try:
            _validate_currency_code(payload)
            cash_on_hand = _parse_request_idr_amount(payload.get("cash_on_hand"), "cash_on_hand", 500_000_000)
            monthly_cost = _parse_request_idr_amount(payload.get("monthly_operating_cost"), "monthly_operating_cost", 200_000_000)
            start_date = datetime.fromisoformat(payload.get("start_date")) if payload.get("start_date") else datetime.now()
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        except TypeError:
            return jsonify({"error": "Invalid start_date format (use ISO format)"}), 400
        mode = str(payload.get("horizon", "short_term")).strip() or "short_term"
        active_forecast = (_get_or_build_horizon_forecasts(cash_on_hand, monthly_cost, start_date).get(mode)) or {}
        forecaster = current_app.config["forecaster"]
        invoices = forecaster._parse_invoices(
            current_app.config["knowledge_base"].df,
            start_date=start_date,
            end_date=start_date,
        )
        return jsonify(
            {
                "horizon": mode,
                "riskSummary": (active_forecast.get("dashboard_snapshot", {}) or {}).get("risk_summary", {}),
                "concentration": _build_concentration_view(invoices),
                "sync_status": _build_sync_snapshot(),
            }
        )

    return app


def parse_args():
    parser = argparse.ArgumentParser(description="Run the financial reporting app.")
    parser.add_argument(
        "--data-mode",
        choices=("demo", "internal_api"),
        help="Select the internal data acquisition mode for this process.",
    )
    parser.add_argument(
        "--internal-api-base-url",
        help="Optional override for the internal API base URL.",
    )
    parser.add_argument(
        "--internal-api-url",
        help="Optional override for the full internal API endpoint URL.",
    )
    parser.add_argument(
        "--internal-api-method",
        help="Optional override for the internal API HTTP method, for example POST.",
    )
    parser.add_argument(
        "--host",
        help="Bind host for shared access, for example 0.0.0.0.",
    )
    parser.add_argument(
        "--port",
        type=int,
        help="Bind port for the web app.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Run Flask in debug mode.",
    )
    parser.add_argument(
        "--server",
        choices=("flask", "waitress"),
        help="Select the web server runtime.",
    )
    return parser.parse_args()


def apply_runtime_overrides(args):
    if args.data_mode:
        os.environ["DATA_ACQUISITION_MODE"] = args.data_mode
    if args.internal_api_base_url:
        os.environ["INTERNAL_API_BASE_URL"] = args.internal_api_base_url
        os.environ.setdefault("DATA_ACQUISITION_MODE", "internal_api")
    if args.internal_api_url:
        os.environ["INTERNAL_API_ENDPOINT_URL"] = args.internal_api_url
        os.environ.setdefault("DATA_ACQUISITION_MODE", "internal_api")
    if args.internal_api_method:
        os.environ["INTERNAL_API_METHOD"] = args.internal_api_method.upper()
    if args.host:
        os.environ["APP_HOST"] = args.host
    if args.port:
        os.environ["APP_PORT"] = str(args.port)
    if args.debug:
        os.environ["APP_DEBUG"] = "true"
    if args.server:
        os.environ["APP_SERVER"] = args.server


def run_app(app_instance):
    app_server = os.getenv("APP_SERVER", "flask").strip().lower()
    app_host = os.getenv("APP_HOST", "127.0.0.1").strip()
    app_port = int(os.getenv("APP_PORT", "5000"))
    app_debug = os.getenv("APP_DEBUG", "false").strip().lower() in {"1", "true", "yes"}
    waitress_threads = int(os.getenv("WAITRESS_THREADS", "12"))
    waitress_connection_limit = int(os.getenv("WAITRESS_CONNECTION_LIMIT", "100"))
    waitress_channel_timeout = int(os.getenv("WAITRESS_CHANNEL_TIMEOUT", "120"))

    if app_server == "waitress":
        try:
            from waitress import serve
        except ImportError as exc:
            raise RuntimeError(
                "Waitress is not installed. Run `pip install -r requirements.txt` first."
            ) from exc

        logger.info(
            "Starting Waitress on %s:%s with %s threads.",
            app_host,
            app_port,
            waitress_threads,
        )
        serve(
            app_instance,
            host=app_host,
            port=app_port,
            threads=waitress_threads,
            connection_limit=waitress_connection_limit,
            channel_timeout=waitress_channel_timeout,
        )
        return

    logger.info(
        "Starting Flask development server on %s:%s.",
        app_host,
        app_port,
    )
    app_instance.run(host=app_host, port=app_port, debug=app_debug, threaded=True)


app = create_app() if __name__ != "__main__" else None


if __name__ == "__main__":
    runtime_args = parse_args()
    apply_runtime_overrides(runtime_args)
    app = create_app()
    run_app(app)
