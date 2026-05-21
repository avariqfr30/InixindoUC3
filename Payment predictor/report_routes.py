from flask import current_app, jsonify, render_template, request, send_file, session

from app_services import build_sync_snapshot, get_auth_security_snapshot
from report_jobs import QueueCapacityError


def register_report_routes(app):
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
                    "syncStatus": build_sync_snapshot(),
                    "dataSourceContract": active_knowledge_base.get_internal_data_contract(),
                    "authSecurity": get_auth_security_snapshot(),
                }
            )
        review_context = active_knowledge_base.get_review_context()

        return jsonify(
            {
                "suggestions": current_app.config["smart_suggestions"],
                "statusPollIntervalMs": current_app.config["status_poll_interval_ms"],
                "reviewContext": review_context,
                "syncStatus": build_sync_snapshot(),
                "dataSourceContract": active_knowledge_base.get_internal_data_contract(),
                "authSecurity": get_auth_security_snapshot(),
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

    @app.route("/api/report-prefetch", methods=["POST"])
    def prefetch_report_context():
        payload = request.get_json(silent=True) or {}
        notes = payload.get("notes", "")
        active_knowledge_base = current_app.config["knowledge_base"]
        return jsonify(active_knowledge_base.prefetch_report_context(notes)), 202

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
                "syncStatus": build_sync_snapshot(),
            }
        )
