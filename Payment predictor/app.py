import argparse
import concurrent.futures
import io
import logging
import os
import threading
import time
import uuid

from flask import Flask, current_app, jsonify, render_template, request, send_file
from flask_cors import CORS

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


class ReportJobManager:
    def __init__(self, report_generator, max_workers, retention_seconds):
        self.report_generator = report_generator
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        self.retention_seconds = retention_seconds
        self.jobs = {}
        self.lock = threading.Lock()

    def _cleanup_locked(self):
        now = time.time()
        expired_job_ids = [
            job_id
            for job_id, job in self.jobs.items()
            if job["status"] in {"ready", "error"}
            and (now - job["updated_at"]) > self.retention_seconds
        ]
        for job_id in expired_job_ids:
            self.jobs.pop(job_id, None)

    def _serialize_job_locked(self, job_id, job):
        queued_ahead = sum(
            1
            for other_job_id, other_job in self.jobs.items()
            if other_job_id != job_id
            and other_job["status"] in {"queued", "running"}
            and other_job["created_at"] < job["created_at"]
        )
        return {
            "jobId": job_id,
            "status": job["status"],
            "queuedAhead": queued_ahead,
            "durationSeconds": round(job["duration_seconds"], 2) if job["duration_seconds"] is not None else None,
            "error": job["error"],
        }

    def submit(self, notes):
        job_id = uuid.uuid4().hex
        now = time.time()

        with self.lock:
            self._cleanup_locked()
            self.jobs[job_id] = {
                "status": "queued",
                "created_at": now,
                "updated_at": now,
                "duration_seconds": None,
                "error": None,
                "filename": None,
                "file_bytes": None,
            }

        self.executor.submit(self._run_job, job_id, notes)
        return job_id

    def _run_job(self, job_id, notes):
        with self.lock:
            job = self.jobs.get(job_id)
            if not job:
                return
            job["status"] = "running"
            job["updated_at"] = time.time()

        started_at = time.time()

        try:
            document, file_name = self.report_generator.run(notes)
            output_stream = io.BytesIO()
            document.save(output_stream)
            file_bytes = output_stream.getvalue()
        except Exception as exc:
            logger.exception("Background report generation failed: %s", exc)
            with self.lock:
                job = self.jobs.get(job_id)
                if not job:
                    return
                job["status"] = "error"
                job["updated_at"] = time.time()
                job["duration_seconds"] = time.time() - started_at
                job["error"] = "Document generation failed. Please verify the service configuration."
            return

        with self.lock:
            job = self.jobs.get(job_id)
            if not job:
                return
            job["status"] = "ready"
            job["updated_at"] = time.time()
            job["duration_seconds"] = time.time() - started_at
            job["filename"] = file_name
            job["file_bytes"] = file_bytes

    def get_status(self, job_id):
        with self.lock:
            self._cleanup_locked()
            job = self.jobs.get(job_id)
            if not job:
                return None
            return self._serialize_job_locked(job_id, job)

    def get_download(self, job_id):
        with self.lock:
            self._cleanup_locked()
            job = self.jobs.get(job_id)
            if not job:
                return None
            if job["status"] != "ready":
                return {"status": job["status"], "error": job["error"]}
            return {
                "filename": job["filename"],
                "file_bytes": job["file_bytes"],
            }

    def get_health(self):
        with self.lock:
            self._cleanup_locked()
            queued_jobs = sum(1 for job in self.jobs.values() if job["status"] == "queued")
            running_jobs = sum(1 for job in self.jobs.values() if job["status"] == "running")
            ready_jobs = sum(1 for job in self.jobs.values() if job["status"] == "ready")
        return {
            "queuedJobs": queued_jobs,
            "runningJobs": running_jobs,
            "readyJobs": ready_jobs,
        }


def create_app():
    from config import (
        DB_URI,
        REPORT_JOB_RETENTION_SECONDS,
        REPORT_MAX_CONCURRENT_JOBS,
        REPORT_STATUS_POLL_INTERVAL_MS,
        SMART_SUGGESTIONS,
    )
    from core import KnowledgeBase, ReportGenerator

    app = Flask(__name__)
    CORS(app)

    knowledge_base = KnowledgeBase(DB_URI)
    report_generator = ReportGenerator(knowledge_base)
    job_manager = ReportJobManager(
        report_generator=report_generator,
        max_workers=REPORT_MAX_CONCURRENT_JOBS,
        retention_seconds=REPORT_JOB_RETENTION_SECONDS,
    )

    app.config["knowledge_base"] = knowledge_base
    app.config["job_manager"] = job_manager
    app.config["status_poll_interval_ms"] = REPORT_STATUS_POLL_INTERVAL_MS

    @app.route("/")
    def home():
        return render_template("index.html")

    @app.route("/get-config")
    def get_config():
        active_knowledge_base = current_app.config["knowledge_base"]
        if active_knowledge_base.df is None or active_knowledge_base.df.empty:
            return jsonify({"error": "Financial data is currently unavailable."})
        review_context = active_knowledge_base.get_review_context()

        return jsonify(
            {
                "suggestions": SMART_SUGGESTIONS,
                "statusPollIntervalMs": current_app.config["status_poll_interval_ms"],
                "reviewContext": review_context,
            }
        )

    @app.route("/generate", methods=["POST"])
    def generate_doc():
        payload = request.get_json(silent=True) or {}
        notes = payload.get("notes", "")
        active_job_manager = current_app.config["job_manager"]
        job_id = active_job_manager.submit(notes)
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
        if "file_bytes" not in download_payload:
            return jsonify(download_payload), 409

        output_stream = io.BytesIO(download_payload["file_bytes"])
        output_stream.seek(0)
        return send_file(
            output_stream,
            as_attachment=True,
            download_name=f"{download_payload['filename']}.docx",
            mimetype="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        )

    @app.route("/refresh-knowledge", methods=["POST"])
    def refresh_knowledge():
        active_knowledge_base = current_app.config["knowledge_base"]
        success = active_knowledge_base.refresh_data()
        return jsonify({"status": "success" if success else "error"})

    @app.route("/health")
    def health():
        health_snapshot = current_app.config["job_manager"].get_health()
        health_snapshot["dataReady"] = bool(
            current_app.config["knowledge_base"].df is not None
            and not current_app.config["knowledge_base"].df.empty
        )
        return jsonify(health_snapshot)

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
