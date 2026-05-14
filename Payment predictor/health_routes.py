from flask import current_app, jsonify

from app_services import build_sync_snapshot, get_auth_security_snapshot


def register_health_routes(app):
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
        health_snapshot["authSecurity"] = get_auth_security_snapshot()
        health_snapshot["syncStatus"] = build_sync_snapshot()
        return jsonify(health_snapshot)
