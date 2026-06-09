import argparse
import logging
import os
import secrets

from flask import Flask
from flask_cors import CORS

from app_services import configure_runtime_services
from auth_routes import register_auth_routes
from data_source_routes import register_data_source_routes
from forecast_routes import register_forecast_routes
from health_routes import register_health_routes
from report_routes import register_report_routes

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

_DEFAULT_SECRET_KEY = "change-this-secret-key-in-production"


def create_app():
    from config import (
        APP_SECRET_KEY,
        APP_SERVER,
        PERMANENT_SESSION_LIFETIME,
        SESSION_COOKIE_SECURE,
    )

    app = Flask(__name__)

    # --- Secret key validation (1b) ---
    if APP_SECRET_KEY == _DEFAULT_SECRET_KEY:
        is_production = APP_SERVER == "waitress" or os.getenv("FLASK_ENV") == "production"
        if is_production:
            raise RuntimeError(
                "CRITICAL: APP_SECRET_KEY is the default value. "
                "Set a secure key: export APP_SECRET_KEY=$(python3 -c 'import secrets; print(secrets.token_hex(32))')"
            )
        generated = secrets.token_hex(32)
        app.secret_key = generated
        logger.warning("APP_SECRET_KEY is default. Auto-generated ephemeral key for dev session: %s...", generated[:8])
    else:
        app.secret_key = APP_SECRET_KEY

    app.config["SESSION_COOKIE_HTTPONLY"] = True
    app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
    app.config["SESSION_COOKIE_SECURE"] = SESSION_COOKIE_SECURE
    app.config["PERMANENT_SESSION_LIFETIME"] = PERMANENT_SESSION_LIFETIME

    # --- CORS restriction (1a) ---
    allowed_origins = [
        "http://127.0.0.1:5000",
        "http://localhost:5000",
    ]
    extra_origin = os.getenv("ALLOWED_ORIGIN", "").strip()
    if extra_origin:
        allowed_origins.append(extra_origin)
    CORS(app, origins=allowed_origins, supports_credentials=True)

    # --- CSRF Protection (1c) ---
    from flask_wtf.csrf import CSRFProtect, generate_csrf
    is_testing = (
        app.testing
        or app.config.get("TESTING")
        or os.getenv("WTF_CSRF_ENABLED") == "0"
        or os.getenv("DISABLE_CSRF_FOR_TESTING") == "1"
    )
    app.config["WTF_CSRF_ENABLED"] = not is_testing
    csrf = CSRFProtect(app)

    @app.after_request
    def _set_csrf_cookie(response):
        if not is_testing:
            csrf_token = generate_csrf()
            response.set_cookie("csrf_token", csrf_token, samesite="Lax")
        return response

    # --- Rate Limiting (1e) ---
    from flask_limiter import Limiter
    from flask_limiter.util import get_remote_address
    limiter = Limiter(
        key_func=get_remote_address,
        app=app,
        default_limits=["200 per minute"],
        enabled=not is_testing
    )
    app.config["limiter"] = limiter

    # --- Security headers (1d) ---
    @app.after_request
    def _apply_security_headers(response):
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; "
            "script-src 'self' 'unsafe-inline' https://cdnjs.cloudflare.com; "
            "style-src 'self' 'unsafe-inline' https://cdnjs.cloudflare.com; "
            "img-src 'self' data: blob:; "
            "font-src 'self' data:; "
            "connect-src 'self'; "
            "frame-ancestors 'none'"
        )
        return response

    configure_runtime_services(app, logger)

    # --- Ollama connection & model availability diagnostics ---
    if not is_testing:
        import requests
        from config import OLLAMA_HOST, LLM_MODEL
        try:
            url = f"{OLLAMA_HOST.rstrip('/')}/api/tags"
            response = requests.get(url, timeout=2.0)
            if response.status_code == 200:
                data = response.json()
                available_models = [m.get("name") for m in data.get("models", []) if m.get("name")]
                model_found = False
                for m in available_models:
                    if m == LLM_MODEL or m.split(":")[0] == LLM_MODEL.split(":")[0]:
                        model_found = True
                        break
                if model_found:
                    logger.info("Ollama connection verified. LLM_MODEL '%s' is available.", LLM_MODEL)
                else:
                    logger.warning(
                        "Ollama is running, but LLM_MODEL '%s' was not found in the local models list: %s. "
                        "Please pull the model using command: ollama pull %s",
                        LLM_MODEL, available_models, LLM_MODEL
                    )
            else:
                logger.warning(
                    "Ollama tags endpoint returned status %s. Cannot verify if LLM_MODEL '%s' is pulled.",
                    response.status_code, LLM_MODEL
                )
        except Exception as exc:
            logger.warning(
                "Ollama connection check failed on host '%s': %s. "
                "Ensure Ollama is running and accessible (or run: ollama serve).",
                OLLAMA_HOST, exc
            )

    register_auth_routes(app, logger)
    register_report_routes(app)
    register_data_source_routes(app)
    register_forecast_routes(app, logger)
    register_health_routes(app)
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
    from config import (
        APP_DEBUG,
        APP_HOST,
        APP_PORT,
        APP_SERVER,
        WAITRESS_CHANNEL_TIMEOUT,
        WAITRESS_CONNECTION_LIMIT,
        WAITRESS_THREADS,
    )

    app_server = APP_SERVER
    app_host = APP_HOST
    app_port = APP_PORT
    app_debug = APP_DEBUG
    waitress_threads = WAITRESS_THREADS
    waitress_connection_limit = WAITRESS_CONNECTION_LIMIT
    waitress_channel_timeout = WAITRESS_CHANNEL_TIMEOUT

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
