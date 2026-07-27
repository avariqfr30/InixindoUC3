from pathlib import Path

from flask import current_app, jsonify, render_template, request, send_file, session

from app_services import build_sync_snapshot, get_auth_security_snapshot
from report_jobs import QueueCapacityError
from adaptive_feedback import FeedbackError, FeedbackValidationError
from learning_feedback import UC3_FEEDBACK_POLICY


def register_report_routes(app):
    @app.route("/")
    def home():
        return render_template(
            "index.html",
            current_username=session.get("username", ""),
            current_user_fullname=session.get("user_fullname", ""),
        )

    @app.route("/settings")
    def data_settings():
        return render_template(
            "data_settings.html",
            current_username=session.get("username", ""),
            current_user_fullname=session.get("user_fullname", ""),
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
        feedback_store = current_app.config["learning_feedback_store"]
        owner_key = session.get("username", "")
        guidance = feedback_store.guidance_for(owner_key, "report")
        for item in feedback_store.guidance_for(owner_key, "dashboard"):
            if item not in guidance:
                guidance.append(item)
        feedback_id = str(payload.get("feedback_id") or "").strip()
        feedback_run_id = str(payload.get("feedback_run_id") or "").strip()
        feedback_scope = str(payload.get("feedback_scope") or "report").strip().lower()
        if feedback_scope not in {"dashboard", "report"}:
            return jsonify({"error": "Ruang lingkup feedback tidak didukung."}), 400
        if bool(feedback_id) != bool(feedback_run_id):
            return jsonify({"error": "feedback_id dan feedback_run_id wajib dikirim bersama."}), 400
        if feedback_id:
            if feedback_scope == "dashboard":
                allowed_runs = session.get("dashboard_feedback_runs", [])
                if feedback_run_id not in allowed_runs:
                    return jsonify({"error": "Snapshot dashboard tidak tersedia atau bukan milik Anda."}), 400
            else:
                source = active_job_manager.get_status(feedback_run_id)
                if not source or source.get("status") != "ready" or source.get("submitted_by") != owner_key:
                    return jsonify({"error": "Laporan sumber feedback tidak tersedia atau bukan milik Anda."}), 400
            try:
                immediate = feedback_store.regeneration_guidance(
                    feedback_id, owner_key=owner_key, run_id=feedback_run_id
                )
            except FeedbackError as exc:
                return jsonify({"error": str(exc)}), 400
            guidance.append(immediate["guidance"])
            if immediate.get("note"):
                guidance.append("Catatan untuk pembuatan ulang ini: " + immediate["note"])
        try:
            job_id = active_job_manager.submit(
                notes, analysis_context,
                analysis_payload=analysis_payload,
                submitted_by=owner_key,
                improvement_guidance="\n".join(f"- {item}" for item in guidance),
            )
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
        if feedback_id:
            feedback_store.link_regeneration(
                feedback_id, owner_key=session.get("username", ""), child_run_id=job_id
            )
        return jsonify({"jobId": job_id}), 202

    @app.route("/api/learning-feedback/options")
    def learning_feedback_options():
        return jsonify(UC3_FEEDBACK_POLICY.public_options())

    @app.route("/api/learning-feedback", methods=["POST"])
    def submit_learning_feedback():
        payload = request.get_json(silent=True) or {}
        run_id = str(payload.get("run_id") or "").strip()
        feedback_scope = str(payload.get("feedback_scope") or "report").strip().lower()
        if feedback_scope == "dashboard":
            if run_id not in session.get("dashboard_feedback_runs", []):
                return jsonify({"error": "Snapshot dashboard tidak ditemukan atau bukan milik Anda."}), 404
        elif feedback_scope == "report":
            job = current_app.config["job_manager"].get_status(run_id)
            if not job or job.get("status") != "ready" or job.get("submitted_by") != session.get("username", ""):
                return jsonify({"error": "Laporan tidak ditemukan atau bukan milik Anda."}), 404
        else:
            return jsonify({"error": "Ruang lingkup feedback tidak didukung."}), 400
        try:
            result = current_app.config["learning_feedback_store"].record(
                owner_key=session.get("username", ""), context_key=feedback_scope, run_id=run_id,
                rating=payload.get("rating"), reason_code=payload.get("reason_code"),
                note=payload.get("note"), remember=payload.get("remember", False),
            )
        except FeedbackValidationError as exc:
            return jsonify({"error": str(exc)}), 400
        return jsonify(result), 201

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
        if status.get("submitted_by") and status["submitted_by"] != session.get("username"):
            return jsonify({"error": "Akses ditolak."}), 403
        return jsonify(status)

    @app.route("/jobs/<job_id>/download")
    def download_job(job_id):
        active_job_manager = current_app.config["job_manager"]
        download_payload = active_job_manager.get_download(job_id)
        if download_payload is None:
            return jsonify({"error": "Job not found."}), 404
        if download_payload.get("submitted_by") and download_payload["submitted_by"] != session.get("username"):
            return jsonify({"error": "Akses ditolak."}), 403
        if "artifactPath" not in download_payload:
            return jsonify(download_payload), 409
        artifact = Path(download_payload["artifactPath"]).resolve()
        artifacts_dir = current_app.config.get("report_artifacts_dir")
        if artifacts_dir and not str(artifact).startswith(str(Path(artifacts_dir).resolve())):
            return jsonify({"error": "Invalid artifact path."}), 403
        return send_file(
            artifact,
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
