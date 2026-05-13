import argparse
import json
import os
import sys

import pandas as pd


def _check(name, status, message, details=None):
    return {
        "name": name,
        "status": status,
        "message": message,
        "details": details or {},
    }


def _status_from_bool(value):
    return "pass" if value else "fail"


def _normalize_records(records):
    data_frame = pd.json_normalize(records, sep="_")
    if data_frame.empty:
        return data_frame

    for column in data_frame.columns:
        data_frame[column] = data_frame[column].apply(
            lambda value: json.dumps(value, ensure_ascii=False)
            if isinstance(value, (dict, list))
            else value
        )
    data_frame.columns = [str(column).strip() for column in data_frame.columns]
    return data_frame


def _load_production_profile():
    from config import (
        DATA_ACQUISITION_MODE,
        DATA_SOURCE_DEMO_PROFILE_PATH,
        DATA_SOURCE_PRODUCTION_PROFILE_PATH,
        DEMO_CSV_PATH,
        INTERNAL_API_BASE_URL,
        INTERNAL_API_CONFIG_FILE,
        INTERNAL_API_DATASET_PATH,
        INTERNAL_API_ENDPOINT_URL,
    )
    from data_sources import load_available_source_profiles

    profiles, issues, default_key = load_available_source_profiles(
        demo_csv_path=DEMO_CSV_PATH,
        legacy_data_mode=DATA_ACQUISITION_MODE,
        internal_api_endpoint_url=INTERNAL_API_ENDPOINT_URL,
        internal_api_base_url=INTERNAL_API_BASE_URL,
        internal_api_dataset_path=INTERNAL_API_DATASET_PATH,
        demo_profile_path=DATA_SOURCE_DEMO_PROFILE_PATH,
        production_profile_path=DATA_SOURCE_PRODUCTION_PROFILE_PATH,
        config_file_path=INTERNAL_API_CONFIG_FILE,
    )
    return profiles.get("production"), issues, default_key


def _build_auth_shape(client):
    raw_profile_auth = (client.source_profile.get("auth") or {}) if client.source_profile else {}
    bearer_from_env = str(raw_profile_auth.get("bearer_token") or "").strip() == "__ENV__"
    if client.basic_username:
        auth_mode = "basic"
    elif client.auth_token:
        auth_mode = "bearer"
    else:
        auth_mode = "none"

    return {
        "mode": auth_mode,
        "bearerTokenConfigured": bool(client.auth_token),
        "bearerTokenFromEnv": bearer_from_env,
        "basicUsernameConfigured": bool(client.basic_username),
        "basicPasswordConfigured": bool(client.basic_password),
        "headerCount": len(client.headers),
    }


def _build_auth_check(auth_shape):
    if auth_shape["mode"] == "basic" and not auth_shape["basicPasswordConfigured"]:
        return _check(
            "auth_shape",
            "fail",
            "Basic auth username is configured but the password is missing.",
            auth_shape,
        )
    if auth_shape["basicPasswordConfigured"] and not auth_shape["basicUsernameConfigured"]:
        return _check(
            "auth_shape",
            "fail",
            "Basic auth password is configured but the username is missing.",
            auth_shape,
        )
    if auth_shape["bearerTokenFromEnv"] and not auth_shape["bearerTokenConfigured"]:
        return _check(
            "auth_shape",
            "fail",
            "Bearer auth is configured to use INTERNAL_API_AUTH_TOKEN, but the environment token is empty.",
            auth_shape,
        )
    if auth_shape["mode"] == "none":
        return _check(
            "auth_shape",
            "warn",
            "No API auth is configured. This is acceptable only if the internal endpoint is intentionally open inside the network.",
            auth_shape,
        )
    return _check("auth_shape", "pass", f"API auth shape is {auth_shape['mode']}.", auth_shape)


def _build_next_steps(contract_summary, extraction_summary, profile_summary, checks):
    next_steps = []
    endpoint = profile_summary.get("endpoint", {}) if isinstance(profile_summary, dict) else {}
    records_failed = any(check["name"] == "records_path" and check["status"] == "fail" for check in checks)
    if not endpoint.get("records_key") and extraction_summary.get("resolvedRecordsPath"):
        next_steps.append(
            "Lock the verified records path into endpoint.records_key for production handover: "
            f"{extraction_summary['resolvedRecordsPath']}"
        )
    if records_failed:
        next_steps.append("Fix API fetch, auth validity, or records path extraction before reviewing field_map readiness.")
    elif contract_summary.get("missingRequiredFields"):
        missing = ", ".join(contract_summary.get("missingRequiredFields") or [])
        next_steps.append(f"Complete field_map for missing required fields: {missing}.")
    if not records_failed and contract_summary.get("lowConfidenceFields"):
        low_confidence = ", ".join(contract_summary.get("lowConfidenceFields") or [])
        next_steps.append(f"Review low-confidence inferred fields before handover: {low_confidence}.")
    if not records_failed and contract_summary.get("fieldMapSuggestionJson"):
        next_steps.append("Use fieldMapSuggestionJson as the explicit field_map draft if inference is not stable enough.")
    if any(check["status"] == "fail" for check in checks):
        next_steps.append("Fix failed checks, then rerun the production source doctor before enabling the source.")
    if not next_steps:
        next_steps.append("Production source is ready for activation under the current application contract.")
    return next_steps


def run_production_source_doctor(source_profile=None, preview_rows=10):
    from data_contract import build_internal_data_summary, normalize_financial_dataframe
    from data_sources import summarize_source_profile
    from finance_api_clients import InternalAPIClient

    profile = source_profile
    registry_issues = []
    default_key = None
    if profile is None:
        profile, registry_issues, default_key = _load_production_profile()

    checks = []
    if not profile:
        checks.append(
            _check(
                "profile_configured",
                "fail",
                "No production source profile is configured. Set INTERNAL_API_CONFIG_FILE, DATA_SOURCE_PRODUCTION_PROFILE_PATH, or INTERNAL_API_ENDPOINT_URL.",
            )
        )
        return {
            "ok": False,
            "sourceKey": "production",
            "checks": checks,
            "registryIssues": registry_issues,
            "nextSteps": ["Create the production JSON profile from deployment/internal-api.production.example.json."],
        }

    profile_summary = summarize_source_profile(profile)
    endpoint = profile.get("endpoint", {}) or {}
    profile_configured = bool(profile_summary.get("configured")) and profile.get("type") == "json_api"
    checks.append(
        _check(
            "profile_configured",
            _status_from_bool(profile_configured),
            "Production JSON API profile is configured." if profile_configured else "Production profile is not a configured JSON API source.",
            profile_summary,
        )
    )

    client = InternalAPIClient(source_profile=profile)
    auth_shape = _build_auth_shape(client)
    checks.append(_build_auth_check(auth_shape))

    request_shape = {
        "method": client.method,
        "datasetUrl": client.get_dataset_url() if client.is_configured() else None,
        "timeoutSeconds": client.timeout,
        "verifySsl": client.verify_ssl,
        "bodyConfigured": client.body is not None,
        "bodyFormat": client.body_format,
        "queryParamCount": len(client.query_params),
        "headerCount": len(client.headers),
        "paginationMode": client.pagination_mode,
        "pageSize": client.page_size,
    }
    checks.append(
        _check(
            "request_shape",
            "pass" if client.method in {"GET", "POST", "PUT", "PATCH"} and client.is_configured() else "fail",
            f"{client.method} request shape is ready for {request_shape['datasetUrl']}.",
            request_shape,
        )
    )

    ok, message = client.validate_endpoint_url()
    checks.append(_check("connectivity", _status_from_bool(ok), message, {"datasetUrl": request_shape["datasetUrl"]}))

    records = []
    extraction_summary = {}
    contract_summary = build_internal_data_summary(None, explicit_field_map=client.field_map)
    fetch_error = None
    try:
        records, extraction_summary = client.fetch_records(preview_limit=max(int(preview_rows or 10), 1))
        raw_df = _normalize_records(records)
        if raw_df.empty:
            checks.append(_check("records_path", "fail", "The API response was reachable but no financial records were returned."))
        else:
            normalized_df, _ = normalize_financial_dataframe(raw_df, explicit_field_map=client.field_map)
            contract_summary = build_internal_data_summary(
                normalized_df,
                explicit_field_map=client.field_map,
                extraction_summary=extraction_summary,
            )
            records_path_status = "pass" if extraction_summary.get("resolvedRecordsPath") else "fail"
            if records_path_status == "pass" and not endpoint.get("records_key"):
                records_path_status = "warn"
            checks.append(
                _check(
                    "records_path",
                    records_path_status,
                    "Records path resolved from the API response."
                    if records_path_status != "fail"
                    else "No usable records path was resolved from the API response.",
                    {
                        "configuredRecordsKey": endpoint.get("records_key") or "",
                        "resolvedRecordsPath": extraction_summary.get("resolvedRecordsPath"),
                        "strategy": extraction_summary.get("strategy"),
                        "recordCount": extraction_summary.get("recordCount"),
                        "candidateRecordSetsScanned": extraction_summary.get("candidateCount"),
                    },
                )
            )
    except Exception as exc:
        fetch_error = str(exc)
        checks.append(
            _check(
                "records_path",
                "fail",
                f"API fetch or record extraction failed: {fetch_error}",
                {"configuredRecordsKey": endpoint.get("records_key") or ""},
            )
        )

    field_ready = bool(contract_summary.get("isReady"))
    handover_ready = bool(
        field_ready
        and not contract_summary.get("lowConfidenceFields")
        and not fetch_error
        and bool(extraction_summary.get("resolvedRecordsPath"))
    )
    checks.append(
        _check(
            "field_mapping_readiness",
            "pass" if handover_ready else ("warn" if field_ready else "fail"),
            "Field mapping satisfies the required internal finance contract."
            if field_ready
            else (
                "Field mapping could not be checked because API fetch or record extraction failed."
                if fetch_error
                else "Field mapping is missing required finance fields."
            ),
            {
                "isReady": field_ready,
                "handoverReady": handover_ready,
                "blockedByFetchError": bool(fetch_error),
                "missingRequiredFields": contract_summary.get("missingRequiredFields") or [],
                "lowConfidenceFields": contract_summary.get("lowConfidenceFields") or [],
                "fieldMapSuggestionJson": contract_summary.get("fieldMapSuggestionJson"),
            },
        )
    )

    activation_ready = bool(
        profile_configured
        and client.is_configured()
        and not fetch_error
        and len(records) > 0
        and field_ready
        and not registry_issues
    )
    checks.append(
        _check(
            "activation_readiness",
            _status_from_bool(activation_ready),
            "The production source is ready to activate with the app's current activation code."
            if activation_ready
            else "The production source is not ready to activate yet.",
            {
                "activationReady": activation_ready,
                "recordCount": len(records),
                "defaultSourceKeyFromConfig": default_key,
                "registryIssues": registry_issues,
            },
        )
    )

    next_steps = _build_next_steps(contract_summary, extraction_summary, profile, checks)
    return {
        "ok": activation_ready,
        "sourceKey": "production",
        "profile": profile_summary,
        "authShape": auth_shape,
        "requestShape": request_shape,
        "records": {
            "recordCount": len(records),
            "extractionSummary": extraction_summary,
            "fetchError": fetch_error,
        },
        "fieldMapping": contract_summary,
        "activation": {
            "activationReady": activation_ready,
            "handoverReady": handover_ready,
            "defaultSourceKeyFromConfig": default_key,
        },
        "checks": checks,
        "registryIssues": registry_issues,
        "nextSteps": next_steps,
    }


def _format_text_report(result):
    lines = ["Production Source Doctor", ""]
    for check in result.get("checks", []):
        lines.append(f"{check['status'].upper():<5} {check['name']}: {check['message']}")
    lines.append("")
    activation = result.get("activation") or {}
    lines.append(f"Activation ready: {'yes' if activation.get('activationReady') else 'no'}")
    lines.append(f"Handover ready: {'yes' if activation.get('handoverReady') else 'no'}")
    records = result.get("records") or {}
    extraction = records.get("extractionSummary") or {}
    if extraction:
        lines.append(f"Resolved records path: {extraction.get('resolvedRecordsPath') or '-'}")
        lines.append(f"Record count checked: {records.get('recordCount', 0)}")
    lines.append("")
    lines.append("Next steps:")
    for step in result.get("nextSteps") or []:
        lines.append(f"- {step}")
    return "\n".join(lines)


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Run the production internal API source doctor without exposing data-source mode in the UI."
    )
    parser.add_argument(
        "--profile",
        help="Path to the production JSON profile. Sets INTERNAL_API_CONFIG_FILE for this run.",
    )
    parser.add_argument("--preview-rows", type=int, default=10, help="Number of records to fetch for the diagnostic preview.")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON instead of the concise text report.")
    args = parser.parse_args(argv)

    if args.profile:
        os.environ["INTERNAL_API_CONFIG_FILE"] = args.profile
        os.environ.setdefault("DATA_ACQUISITION_MODE", "internal_api")

    result = run_production_source_doctor(preview_rows=args.preview_rows)
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(_format_text_report(result))
    return 0 if result.get("ok") else 2


if __name__ == "__main__":
    sys.exit(main())
