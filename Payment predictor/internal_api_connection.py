from data_contract import build_internal_data_summary, normalize_financial_dataframe
from data_sources import build_internal_api_profile_from_connection_payload, write_source_profile


def connect_internal_data_source(payload, knowledge_base, forecast_cache, cash_out_store, sync_snapshot):
    from config import INTERNAL_API_CONFIG_FILE
    from finance_api_clients import InternalAPIClient

    payload = payload or {}
    preview_rows = max(min(int(payload.get("previewRows") or 10), 25), 1)
    should_activate = bool(payload.get("activate", True))
    profile = build_internal_api_profile_from_connection_payload(payload)

    client = InternalAPIClient(source_profile=profile)
    records, extraction_summary = client.fetch_records(preview_limit=preview_rows)
    raw_df = knowledge_base._normalize_records(records)

    if raw_df.empty:
        write_source_profile(INTERNAL_API_CONFIG_FILE, profile)
        knowledge_base._reload_source_registry()
        return {
            "ready": False,
            "activated": False,
            "profileSaved": True,
            "message": "API terbaca, tetapi tidak ada record yang dapat dianalisis.",
            "recordCount": 0,
            "nextSteps": [
                "Pastikan endpoint mengembalikan array record finansial, atau isi records_key ke path array yang benar."
            ],
            "extractionSummary": extraction_summary,
            "syncStatus": sync_snapshot(),
        }, 200

    normalized_df, _ = normalize_financial_dataframe(
        raw_df,
        explicit_field_map=client.field_map,
    )
    data_summary = build_internal_data_summary(
        normalized_df,
        explicit_field_map=client.field_map,
        extraction_summary=extraction_summary,
    )
    write_source_profile(INTERNAL_API_CONFIG_FILE, profile)
    knowledge_base._reload_source_registry()

    ready = bool(data_summary.get("isReady"))
    next_steps = knowledge_base._build_source_validation_next_steps(data_summary)
    activation = {"activated": False}
    if ready and should_activate:
        activation = knowledge_base.activate_source("production")
        forecast_cache.clear()
        cash_out_store.refresh_data()

    sample_records = raw_df.head(preview_rows).to_dict(orient="records")
    response_payload = {
        "ready": ready,
        "activated": bool(activation.get("activated")),
        "profileSaved": True,
        "message": (
            "API internal berhasil disambungkan dan data sudah aktif."
            if activation.get("activated")
            else "API internal tersimpan, tetapi field wajib belum lengkap untuk dipakai sebagai basis analisis."
        ),
        "recordCount": int(len(records)),
        "previewRows": len(sample_records),
        "sampleRecords": sample_records,
        "contractSummary": data_summary,
        "extractionSummary": extraction_summary,
        "nextSteps": next_steps,
        "syncStatus": sync_snapshot(),
        "reviewContext": knowledge_base.get_review_context() if activation.get("activated") else None,
    }

    if ready and should_activate and not activation.get("activated"):
        response_payload["message"] = activation.get("message") or "API terbaca, tetapi aktivasi data gagal."
        return response_payload, 409
    return response_payload, 200
