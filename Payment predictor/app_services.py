from flask import current_app

from auth_store import ActiveSessionStore, UserStore
from report_jobs import ReportJobManager, ReportJobStore
from runtime_services import BackgroundRefreshCoordinator, ForecastSnapshotCache


def configure_runtime_services(app, logger):
    from config import (
        AUTH_SIGNUP_VERIFICATION_DELIVERY_MODE,
        AUTH_SIGNUP_VERIFICATION_TIMEOUT_SECONDS,
        AUTH_SIGNUP_VERIFICATION_WEBHOOK_URL,
        AUTH_ALLOWED_EMAIL_DOMAIN,
        AUTH_MAX_ACTIVE_SESSIONS,
        AUTH_MAX_SESSIONS_PER_USER,
        AUTH_SESSION_ABSOLUTE_TIMEOUT_HOURS,
        AUTH_SESSION_IDLE_TIMEOUT_MINUTES,
        REFERENCE_INTERNAL_ACCOUNT_LOOKUP_MODE,
        REFERENCE_INTERNAL_ACCOUNT_LOOKUP_PASSWORD,
        REFERENCE_INTERNAL_ACCOUNT_LOOKUP_TIMEOUT_SECONDS,
        REFERENCE_INTERNAL_ACCOUNT_LOOKUP_URL,
        REFERENCE_INTERNAL_ACCOUNT_LOOKUP_USERNAME,
        REFERENCE_INTERNAL_ACCOUNT_TEST_EMAILS,
        DATA_REFRESH_INTERVAL_SECONDS,
        DB_URI,
        FORECAST_CACHE_TTL_SECONDS,
        JOB_STATE_DB_PATH,
        REPORT_ARTIFACTS_DIR,
        REPORT_JOB_RETENTION_SECONDS,
        REPORT_MAX_CONCURRENT_JOBS,
        REPORT_MAX_PENDING_JOBS,
        REPORT_MIN_COMPLETENESS_SCORE,
        REPORT_METRICS_WINDOW_HOURS,
        REPORT_STATUS_POLL_INTERVAL_MS,
        SMART_SUGGESTIONS,
        TEMP_FULL_ACCESS_PASSWORD,
        TEMP_FULL_ACCESS_USERNAME,
    )
    from core import CashOutStore, KnowledgeBase, ReportGenerator
    from forecast_engine import CashflowForecaster

    knowledge_base = KnowledgeBase(DB_URI)
    cash_out_store = CashOutStore(source_profile=knowledge_base.source_profile)
    report_generator = ReportGenerator(knowledge_base)
    job_store = ReportJobStore(JOB_STATE_DB_PATH, REPORT_ARTIFACTS_DIR)
    user_store = UserStore(
        JOB_STATE_DB_PATH,
        allowed_email_domain=AUTH_ALLOWED_EMAIL_DOMAIN,
        temporary_full_access_username=TEMP_FULL_ACCESS_USERNAME,
        temporary_full_access_password=TEMP_FULL_ACCESS_PASSWORD,
        reference_internal_account_lookup_mode=REFERENCE_INTERNAL_ACCOUNT_LOOKUP_MODE,
        reference_internal_account_lookup_url=REFERENCE_INTERNAL_ACCOUNT_LOOKUP_URL,
        reference_internal_account_lookup_username=REFERENCE_INTERNAL_ACCOUNT_LOOKUP_USERNAME,
        reference_internal_account_lookup_password=REFERENCE_INTERNAL_ACCOUNT_LOOKUP_PASSWORD,
        reference_internal_account_lookup_timeout_seconds=REFERENCE_INTERNAL_ACCOUNT_LOOKUP_TIMEOUT_SECONDS,
        reference_internal_account_test_emails=REFERENCE_INTERNAL_ACCOUNT_TEST_EMAILS,
        signup_verification_delivery_mode=AUTH_SIGNUP_VERIFICATION_DELIVERY_MODE,
        signup_verification_webhook_url=AUTH_SIGNUP_VERIFICATION_WEBHOOK_URL,
        signup_verification_timeout_seconds=AUTH_SIGNUP_VERIFICATION_TIMEOUT_SECONDS,
    )
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
    forecaster = CashflowForecaster(monthly_operating_cost_idr=200_000_000)
    refresh_coordinator = BackgroundRefreshCoordinator(
        knowledge_base=knowledge_base,
        cash_out_store=cash_out_store,
        forecast_cache=forecast_cache,
        interval_seconds=DATA_REFRESH_INTERVAL_SECONDS,
    )
    refresh_coordinator.start()

    _log_endpoint_validation(knowledge_base, cash_out_store, logger)

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
    app.config["smart_suggestions"] = SMART_SUGGESTIONS


def build_sync_snapshot():
    refresh_interval = current_app.config["data_refresh_interval_seconds"]
    knowledge_status = current_app.config["knowledge_base"].get_sync_status(refresh_interval)
    cash_out_status = current_app.config["cash_out_store"].get_status(refresh_interval)
    return {
        "financialData": knowledge_status,
        "cashOutSource": cash_out_status,
        "refreshIntervalSeconds": refresh_interval,
    }


def get_auth_security_snapshot():
    return current_app.config["session_store"].get_security_snapshot(
        idle_timeout_seconds=current_app.config["auth_session_idle_timeout_seconds"],
        absolute_timeout_seconds=current_app.config["auth_session_absolute_timeout_seconds"],
        max_global_sessions=current_app.config["auth_max_active_sessions"],
        max_sessions_per_user=current_app.config["auth_max_sessions_per_user"],
    )


def is_internal_api_active(sync_snapshot):
    source = sync_snapshot.get("financialData") or {}
    active_source = source.get("activeSource") or {}
    return bool(active_source.get("type") == "json_api" or source.get("dataMode") == "production")


def internal_api_source_key(sync_snapshot):
    source = sync_snapshot.get("financialData") or {}
    available_sources = source.get("availableSources") or []
    for profile in available_sources:
        if profile and profile.get("type") == "json_api" and profile.get("configured"):
            return profile.get("key") or "production"
    return ""


def _log_endpoint_validation(knowledge_base, cash_out_store, logger):
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
