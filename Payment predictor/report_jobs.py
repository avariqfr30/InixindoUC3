import concurrent.futures
import logging
import sqlite3
import threading
import time
import uuid
from pathlib import Path

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
                    quality_gate_passed INTEGER DEFAULT 0,
                    completeness_score REAL DEFAULT 0
                )
                """
            )
            existing_columns = {
                row["name"]
                for row in connection.execute("PRAGMA table_info(report_jobs)").fetchall()
            }
            column_migrations = {
                "fallback_used": "ALTER TABLE report_jobs ADD COLUMN fallback_used INTEGER DEFAULT 0",
                "osint_available": "ALTER TABLE report_jobs ADD COLUMN osint_available INTEGER DEFAULT 0",
                "visuals_included": "ALTER TABLE report_jobs ADD COLUMN visuals_included INTEGER DEFAULT 0",
                "quality_gate_passed": "ALTER TABLE report_jobs ADD COLUMN quality_gate_passed INTEGER DEFAULT 0",
                "completeness_score": "ALTER TABLE report_jobs ADD COLUMN completeness_score REAL DEFAULT 0",
            }
            for column_name, migration_sql in column_migrations.items():
                if column_name not in existing_columns:
                    try:
                        connection.execute(migration_sql)
                    except sqlite3.OperationalError as exc:
                        if "duplicate column name" not in str(exc).lower():
                            raise
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
        job["completeness_score"] = float(job.get("completeness_score") or 0)
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
                    SUM(CASE WHEN quality_gate_passed = 1 THEN 1 ELSE 0 END) AS accepted_jobs,
                    AVG(completeness_score) AS avg_completeness_score,
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
        accepted_jobs = int(row["accepted_jobs"] or 0)
        avg_duration = round(float(row["avg_duration"]), 2) if row["avg_duration"] is not None else None
        avg_completeness = (
            round(float(row["avg_completeness_score"]), 1)
            if row["avg_completeness_score"] is not None
            else None
        )
        success_rate = round((ready_jobs / completed_jobs) * 100, 1) if completed_jobs else None
        fallback_rate = round((fallback_jobs / completed_jobs) * 100, 1) if completed_jobs else None
        accepted_rate = round((accepted_jobs / completed_jobs) * 100, 1) if completed_jobs else None

        return {
            "completedJobs": completed_jobs,
            "readyJobs": ready_jobs,
            "errorJobs": error_jobs,
            "fallbackJobs": fallback_jobs,
            "acceptedJobs": accepted_jobs,
            "averageDurationSeconds": avg_duration,
            "averageCompletenessScore": avg_completeness,
            "successRatePct": success_rate,
            "fallbackRatePct": fallback_rate,
            "acceptedRatePct": accepted_rate,
        }


class ReportJobManager:
    def __init__(
        self,
        report_generator,
        max_workers,
        max_pending_jobs,
        retention_seconds,
        artifacts_dir,
        job_store,
        metrics_window_hours,
    ):
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
            "completenessScore": job["completeness_score"],
        }

    def submit(self, notes, analysis_context="", analysis_payload=None):
        job_id = uuid.uuid4().hex
        preview_source = (notes or "").strip() or (analysis_context or "").strip()
        notes_preview = preview_source.replace("\n", " ")[:240]

        with self.lock:
            self._cleanup_locked()
            active_jobs = self.job_store.count_active_jobs()
            if active_jobs >= self.max_pending_jobs:
                raise QueueCapacityError(active_jobs, self.max_pending_jobs)
            self.job_store.create_job(job_id, notes_preview)

        self.executor.submit(self._run_job, job_id, notes, analysis_context, analysis_payload)
        return job_id

    def _run_job(self, job_id, notes, analysis_context="", analysis_payload=None):
        started_at = time.time()
        self.job_store.update_job(
            job_id,
            status="running",
            started_at=started_at,
            updated_at=started_at,
            error=None,
        )

        try:
            document, file_name, run_metadata = self.report_generator.run(
                notes,
                analysis_context,
                analysis_payload=analysis_payload,
            )
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
            completeness_score=float(run_metadata.get("completeness_score") or 0),
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
