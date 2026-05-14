import argparse
import logging
import os

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


def create_app():
    from config import (
        APP_SECRET_KEY,
        PERMANENT_SESSION_LIFETIME,
        SESSION_COOKIE_SECURE,
    )

    app = Flask(__name__)
    app.secret_key = APP_SECRET_KEY
    app.config["SESSION_COOKIE_HTTPONLY"] = True
    app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
    app.config["SESSION_COOKIE_SECURE"] = SESSION_COOKIE_SECURE
    app.config["PERMANENT_SESSION_LIFETIME"] = PERMANENT_SESSION_LIFETIME
    CORS(app)

    configure_runtime_services(app, logger)
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
