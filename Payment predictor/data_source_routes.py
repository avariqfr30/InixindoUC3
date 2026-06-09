from flask import current_app, jsonify, request

from app_services import build_sync_snapshot, internal_api_source_key, is_internal_api_active
from data_sources import summarize_source_profile
from internal_api_connection import connect_internal_data_source as connect_internal_data_source_service
from source_preview_service import preview_source_profile


def register_data_source_routes(app):
    @app.route("/api/internal-api/refresh", methods=["POST"])
    @app.route("/api/internal-data/refresh", methods=["POST"])
    def refresh_internal_api_dataset():
        sync_snapshot = build_sync_snapshot()
        api_source_key = internal_api_source_key(sync_snapshot)
        if not (is_internal_api_active(sync_snapshot) or api_source_key):
            return (
                jsonify(
                    {
                        "status": "not_configured",
                        "apiConnectionActive": False,
                        "error": "Internal API belum aktif. Simpan dan aktifkan koneksi Internal API sebelum refresh dataset.",
                        "syncStatus": sync_snapshot,
                    }
                ),
                400,
            )

        if not is_internal_api_active(sync_snapshot):
            activation = current_app.config["knowledge_base"].activate_source(api_source_key)
            if not activation.get("activated"):
                return (
                    jsonify(
                        {
                            "status": "activation_failed",
                            "apiConnectionActive": False,
                            "error": activation.get("message") or "Internal API belum bisa diaktifkan.",
                            "syncStatus": build_sync_snapshot(),
                        }
                    ),
                    409,
                )
            current_app.config["forecast_cache"].clear()
            current_app.config["cash_out_store"].rebind_source_profile(
                current_app.config["knowledge_base"].source_profile,
                refresh=True,
            )

        refresh_result = current_app.config["refresh_coordinator"].refresh_all()
        refreshed_snapshot = build_sync_snapshot()
        success = bool(refresh_result["knowledgeBase"])
        return (
            jsonify(
                {
                    "status": "refreshed" if success else "error",
                    "refreshResult": refresh_result,
                    "syncStatus": refreshed_snapshot,
                    "apiConnectionActive": is_internal_api_active(refreshed_snapshot),
                }
            ),
            200 if success else 503,
        )

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
                sync_snapshot=build_sync_snapshot,
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
                    "syncStatus": build_sync_snapshot(),
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
            try:
                active_kb = current_app.config["knowledge_base"]
                active_kb._reload_source_registry()
                profile = active_kb.source_registry.get(source_key)
                if not profile:
                    return jsonify({"error": f"Sumber data `{source_key}` tidak tersedia."}), 404

                if profile.get("type") == "json_api":
                    preview = preview_source_profile(profile, preview_rows=preview_rows)
                    return jsonify({
                        "preview": True,
                        "ready": preview["ready"],
                        "message": preview["message"],
                        "recordCount": preview["recordCount"],
                        "previewRows": preview["previewRows"],
                        "sampleRecords": preview["sampleRecords"],
                        "contractSummary": preview["contractSummary"],
                        "extractionSummary": preview["extractionSummary"],
                        "syncStatus": build_sync_snapshot(),
                    })
                return jsonify({
                    "preview": True,
                    "message": "Preview hanya tersedia untuk sumber tipe json_api.",
                    "syncStatus": build_sync_snapshot(),
                })
            except Exception as exc:
                return jsonify({
                    "preview": True,
                    "ready": False,
                    "message": str(exc),
                    "syncStatus": build_sync_snapshot(),
                }), 400

        try:
            validation = current_app.config["knowledge_base"].validate_source(source_key)
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 404
        return jsonify(
            {
                **validation,
                "syncStatus": build_sync_snapshot(),
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
        current_app.config["cash_out_store"].rebind_source_profile(
            current_app.config["knowledge_base"].source_profile,
            refresh=True,
        )
        response_payload = {
            **activation,
            "syncStatus": build_sync_snapshot(),
            "reviewContext": current_app.config["knowledge_base"].get_review_context()
            if activation.get("activated")
            else None,
        }
        if not activation.get("activated"):
            return jsonify(response_payload), 409
        return jsonify(response_payload)

    @app.route("/api/data-source/reload-profiles", methods=["POST"])
    def reload_data_source_profiles():
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
                "syncStatus": build_sync_snapshot(),
            })
        except Exception as exc:
            return jsonify({"reloaded": False, "error": str(exc)}), 500

    @app.route("/api/data-source/check-connectivity", methods=["POST"])
    def check_data_source_connectivity():
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
