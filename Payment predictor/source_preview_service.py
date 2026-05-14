from data_contract import build_internal_data_summary, normalize_financial_dataframe, normalize_records
from finance_api_clients import InternalAPIClient


def preview_source_profile(source_profile, preview_rows=5):
    client = InternalAPIClient(source_profile=source_profile)
    records, extraction_summary = client.fetch_records(preview_limit=max(int(preview_rows or 5), 1))
    raw_data_frame = normalize_records(records)
    if raw_data_frame.empty:
        return {
            "ready": False,
            "recordCount": 0,
            "previewRows": 0,
            "sampleRecords": [],
            "contractSummary": build_internal_data_summary(
                None,
                explicit_field_map=client.field_map,
                extraction_summary=extraction_summary,
            ),
            "extractionSummary": extraction_summary,
            "message": "Preview fetch returned no records.",
        }

    normalized_df, _ = normalize_financial_dataframe(
        raw_data_frame,
        explicit_field_map=client.field_map,
    )
    contract_summary = build_internal_data_summary(
        normalized_df,
        explicit_field_map=client.field_map,
        extraction_summary=extraction_summary,
    )
    sample_records = raw_data_frame.head(max(int(preview_rows or 5), 1)).to_dict(orient="records")
    return {
        "ready": bool(contract_summary.get("isReady")),
        "recordCount": len(records),
        "previewRows": len(sample_records),
        "sampleRecords": sample_records,
        "contractSummary": contract_summary,
        "extractionSummary": extraction_summary,
        "message": "Preview berhasil." if contract_summary.get("isReady") else "Field wajib belum lengkap.",
    }
