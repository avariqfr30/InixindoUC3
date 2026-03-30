import argparse
import concurrent.futures
import logging
import os
import sqlite3
import threading
import time
import uuid
from pathlib import Path

from flask import Flask, current_app, jsonify, render_template, request, send_file
from flask_cors import CORS

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


class QueueCapacityError(Exception):
    def __init__(self, active_jobs, max_pending_jobs):
        self.active_jobs = active_jobs
        self.max_pending_jobs = max_pending_jobs
        super().__init__(
            f"Queue is full ({active_jobs}/{max_pending_jobs} active jobs). Please try again in a few minutes."
        )


class ReportJobStore:
    def __init__(self, db_path, artifacts_dir):
        self.db_path = str(db_path)
        self.artifacts_dir = Path(artifacts_dir)
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)
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
                CREATE TABLE IF NOT EXISTS report_jobs (
                    job_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    started_at REAL,
                    duration_seconds REAL,
                    error TEXT,
                    filename TEXT,
                    artifact_path TEXT,
                    notes_preview TEXT,
                    fallback_used INTEGER DEFAULT 0,
                    osint_available INTEGER DEFAULT 0,
                    visuals_included INTEGER DEFAULT 0,
                    quality_gate_passed INTEGER DEFAULT 0
                )
                """
            )
            connection.commit()

    def recover_incomplete_jobs(self):
        with self.lock, self._connect() as connection:
            connection.execute(
                """
                UPDATE report_jobs
                SET status = 'error',
                    error = 'Service restarted before this job completed.',
                    updated_at = ?
                WHERE status IN ('queued', 'running')
                """,
                (time.time(),),
            )
            connection.commit()

    def cleanup_expired(self, retention_seconds):
        cutoff_time = time.time() - retention_seconds
        with self.lock, self._connect() as connection:
            rows = connection.execute(
                """
                SELECT job_id, artifact_path
                FROM report_jobs
                WHERE status IN ('ready', 'error') AND updated_at < ?
                """,
                (cutoff_time,),
            ).fetchall()

            for row in rows:
                artifact_path = row["artifact_path"]
                if artifact_path:
                    try:
                        Path(artifact_path).unlink(missing_ok=True)
                    except OSError:
                        logger.warning("Unable to remove expired artifact for job %s.", row["job_id"])

            connection.execute(
                """
                DELETE FROM report_jobs
                WHERE status IN ('ready', 'error') AND updated_at < ?
                """,
                (cutoff_time,),
            )
            connection.commit()

    def create_job(self, job_id, notes_preview):
        now = time.time()
        with self.lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO report_jobs (
                    job_id,
                    status,
                    created_at,
                    updated_at,
                    notes_preview
                ) VALUES (?, 'queued', ?, ?, ?)
                """,
                (job_id, now, now, notes_preview),
            )
            connection.commit()

    def update_job(self, job_id, **fields):
        if not fields:
            return

        assignments = []
        values = []
        for field_name, field_value in fields.items():
            assignments.append(f"{field_name} = ?")
            values.append(field_value)
        values.append(job_id)

        with self.lock, self._connect() as connection:
            connection.execute(
                f"UPDATE report_jobs SET {', '.join(assignments)} WHERE job_id = ?",
                values,
            )
            connection.commit()

    def get_job(self, job_id):
        with self.lock, self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM report_jobs WHERE job_id = ?",
                (job_id,),
            ).fetchone()

        if not row:
            return None

        job = dict(row)
        for field_name in ("fallback_used", "osint_available", "visuals_included", "quality_gate_passed"):
            job[field_name] = bool(job.get(field_name))
        return job

    def count_active_jobs(self):
        with self.lock, self._connect() as connection:
            row = connection.execute(
                "SELECT COUNT(*) AS total FROM report_jobs WHERE status IN ('queued', 'running')"
            ).fetchone()
        return int(row["total"] if row else 0)

    def count_queued_ahead(self, job_id, created_at):
        with self.lock, self._connect() as connection:
            row = connection.execute(
                """
                SELECT COUNT(*) AS total
                FROM report_jobs
                WHERE job_id != ?
                  AND status IN ('queued', 'running')
                  AND created_at < ?
                """,
                (job_id, created_at),
            ).fetchone()
        return int(row["total"] if row else 0)

    def get_status_counts(self):
        counts = {"queuedJobs": 0, "runningJobs": 0, "readyJobs": 0, "errorJobs": 0}
        with self.lock, self._connect() as connection:
            rows = connection.execute(
                """
                SELECT status, COUNT(*) AS total
                FROM report_jobs
                GROUP BY status
                """
            ).fetchall()

        for row in rows:
            status_key = f"{row['status']}Jobs"
            if status_key in counts:
                counts[status_key] = int(row["total"])
        return counts

    def get_recent_metrics(self, window_hours):
        since_timestamp = time.time() - (window_hours * 3600)
        with self.lock, self._connect() as connection:
            row = connection.execute(
                """
                SELECT
                    COUNT(*) AS completed_jobs,
                    SUM(CASE WHEN status = 'ready' THEN 1 ELSE 0 END) AS ready_jobs,
                    SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS error_jobs,
                    SUM(CASE WHEN fallback_used = 1 THEN 1 ELSE 0 END) AS fallback_jobs,
                    AVG(duration_seconds) AS avg_duration
                FROM report_jobs
                WHERE status IN ('ready', 'error') AND updated_at >= ?
                """,
                (since_timestamp,),
            ).fetchone()

        completed_jobs = int(row["completed_jobs"] or 0)
        ready_jobs = int(row["ready_jobs"] or 0)
        error_jobs = int(row["error_jobs"] or 0)
        fallback_jobs = int(row["fallback_jobs"] or 0)
        avg_duration = round(float(row["avg_duration"]), 2) if row["avg_duration"] is not None else None
        success_rate = round((ready_jobs / completed_jobs) * 100, 1) if completed_jobs else None
        fallback_rate = round((fallback_jobs / completed_jobs) * 100, 1) if completed_jobs else None

        return {
            "completedJobs": completed_jobs,
            "readyJobs": ready_jobs,
            "errorJobs": error_jobs,
            "fallbackJobs": fallback_jobs,
            "averageDurationSeconds": avg_duration,
            "successRatePct": success_rate,
            "fallbackRatePct": fallback_rate,
        }


class ReportJobManager:
    def __init__(self, report_generator, max_workers, max_pending_jobs, retention_seconds, artifacts_dir, job_store, metrics_window_hours):
        self.report_generator = report_generator
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        self.max_workers = max_workers
        self.max_pending_jobs = max(max_pending_jobs, max_workers)
        self.retention_seconds = retention_seconds
        self.artifacts_dir = Path(artifacts_dir)
        self.job_store = job_store
        self.metrics_window_hours = metrics_window_hours
        self.lock = threading.Lock()
        self.job_store.recover_incomplete_jobs()
        self.job_store.cleanup_expired(self.retention_seconds)

    def _cleanup_locked(self):
        self.job_store.cleanup_expired(self.retention_seconds)

    def _serialize_job(self, job):
        queued_ahead = self.job_store.count_queued_ahead(job["job_id"], job["created_at"])
        return {
            "jobId": job["job_id"],
            "status": job["status"],
            "queuedAhead": queued_ahead,
            "durationSeconds": round(job["duration_seconds"], 2) if job["duration_seconds"] is not None else None,
            "error": job["error"],
            "fallbackUsed": job["fallback_used"],
            "osintAvailable": job["osint_available"],
            "visualsIncluded": job["visuals_included"],
            "qualityGatePassed": job["quality_gate_passed"],
        }

    def submit(self, notes):
        job_id = uuid.uuid4().hex
        notes_preview = (notes or "").strip().replace("\n", " ")[:240]

        with self.lock:
            self._cleanup_locked()
            active_jobs = self.job_store.count_active_jobs()
            if active_jobs >= self.max_pending_jobs:
                raise QueueCapacityError(active_jobs, self.max_pending_jobs)
            self.job_store.create_job(job_id, notes_preview)

        self.executor.submit(self._run_job, job_id, notes)
        return job_id

    def _run_job(self, job_id, notes):
        started_at = time.time()
        self.job_store.update_job(
            job_id,
            status="running",
            started_at=started_at,
            updated_at=started_at,
            error=None,
        )

        try:
            document, file_name, run_metadata = self.report_generator.run(notes)
            artifact_path = self.artifacts_dir / f"{job_id}_{file_name}.docx"
            document.save(str(artifact_path))
        except Exception as exc:
            logger.exception("Background report generation failed: %s", exc)
            self.job_store.update_job(
                job_id,
                status="error",
                updated_at=time.time(),
                duration_seconds=time.time() - started_at,
                error="Document generation failed. Please verify the service configuration.",
            )
            return

        self.job_store.update_job(
            job_id,
            status="ready",
            updated_at=time.time(),
            duration_seconds=time.time() - started_at,
            filename=file_name,
            artifact_path=str(artifact_path),
            fallback_used=1 if run_metadata.get("fallback_used") else 0,
            osint_available=1 if run_metadata.get("osint_available") else 0,
            visuals_included=1 if run_metadata.get("visuals_included") else 0,
            quality_gate_passed=1 if run_metadata.get("quality_gate_passed") else 0,
            error=None,
        )

    def get_status(self, job_id):
        with self.lock:
            self._cleanup_locked()
            job = self.job_store.get_job(job_id)
        if not job:
            return None
        return self._serialize_job(job)

    def get_download(self, job_id):
        with self.lock:
            self._cleanup_locked()
            job = self.job_store.get_job(job_id)
        if not job:
            return None
        if job["status"] != "ready":
            return {"status": job["status"], "error": job["error"]}
        artifact_path = job.get("artifact_path")
        if not artifact_path or not Path(artifact_path).exists():
            return {"status": "error", "error": "Generated artifact is no longer available."}
        return {
            "filename": job["filename"],
            "artifactPath": artifact_path,
        }

    def get_health(self):
        with self.lock:
            self._cleanup_locked()
            status_counts = self.job_store.get_status_counts()
            metrics = self.job_store.get_recent_metrics(self.metrics_window_hours)

        active_jobs = status_counts["queuedJobs"] + status_counts["runningJobs"]
        return {
            **status_counts,
            "activeJobs": active_jobs,
            "maxConcurrentJobs": self.max_workers,
            "maxPendingJobs": self.max_pending_jobs,
            "acceptingJobs": active_jobs < self.max_pending_jobs,
            "metricsWindowHours": self.metrics_window_hours,
            "recentMetrics": metrics,
        }


def create_app():
    from config import (
        DB_URI,
        JOB_STATE_DB_PATH,
        REPORT_ARTIFACTS_DIR,
        REPORT_JOB_RETENTION_SECONDS,
        REPORT_MAX_CONCURRENT_JOBS,
        REPORT_MAX_PENDING_JOBS,
        REPORT_METRICS_WINDOW_HOURS,
        REPORT_STATUS_POLL_INTERVAL_MS,
        SMART_SUGGESTIONS,
    )
    from core import KnowledgeBase, ReportGenerator

    app = Flask(__name__)
    CORS(app)

    knowledge_base = KnowledgeBase(DB_URI)
    report_generator = ReportGenerator(knowledge_base)
    job_store = ReportJobStore(JOB_STATE_DB_PATH, REPORT_ARTIFACTS_DIR)
    job_manager = ReportJobManager(
        report_generator=report_generator,
        max_workers=REPORT_MAX_CONCURRENT_JOBS,
        max_pending_jobs=REPORT_MAX_PENDING_JOBS,
        retention_seconds=REPORT_JOB_RETENTION_SECONDS,
        artifacts_dir=REPORT_ARTIFACTS_DIR,
        job_store=job_store,
        metrics_window_hours=REPORT_METRICS_WINDOW_HOURS,
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
        try:
            job_id = active_job_manager.submit(notes)
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
