import json
import os
import re
from copy import deepcopy
from pathlib import Path


def _normalize_key(value):
    return re.sub(r"[^a-z0-9_]+", "_", str(value or "").strip().lower()).strip("_")


def _parse_json_object(raw_value, label):
    raw_value = str(raw_value or "").strip()
    if not raw_value:
        return {}
    try:
        parsed = json.loads(raw_value)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{label} must be valid JSON: {exc}") from exc
    if not isinstance(parsed, dict):
        raise ValueError(f"{label} must be a JSON object.")
    return parsed


def _parse_optional_json_value(raw_value, label):
    raw_value = str(raw_value or "").strip()
    if not raw_value:
        return None
    try:
        return json.loads(raw_value)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{label} must be valid JSON: {exc}") from exc


def build_internal_api_profile_template():
    return {
        "key": "production",
        "name": "Produksi API Internal",
        "mode": "production",
        "type": "json_api",
        "description": "Sumber JSON API internal perusahaan.",
        "endpoint": {
            "url": "https://internal.example.com/api/Resource/dataset",
            "method": "POST",
            "timeout": 20,
            "verify_ssl": True,
            "records_key": "data.dataset_result",
        },
        "auth": {
            "basic_username": "your_username",
            "basic_password": "your_password",
            "bearer_token": "",
        },
        "request": {
            "headers": {},
            "query_params": {},
            "body": {"dataset_code": "ClassReport"},
        },
        "field_map": {
            "period": "your_period_field",
            "partner_type": "your_partner_type_field",
            "service": "your_service_field",
            "payment_class": "your_payment_class_field",
            "invoice_value": "your_invoice_value_field",
            "delay_note": "your_delay_note_field",
        },
        "pagination": {
            "mode": "",
            "page_size": 0,
            "cursor_key": "",
            "offset_param": "offset",
            "limit_param": "limit",
            "max_pages": 50,
        },
        "retry": {
            "max_retries": 3,
            "backoff_base": 1.0,
        },
    }


def _is_full_source_profile(payload):
    if not isinstance(payload, dict):
        return False
    return payload.get("type") == "json_api" and isinstance(payload.get("endpoint"), dict)


def _build_demo_profile(csv_path):
    return {
        "key": "demo",
        "name": "Demo Lokal",
        "mode": "demo",
        "type": "demo_csv",
        "description": "Dataset CSV bawaan aplikasi untuk uji coba dan fallback operasional.",
        "path": str(csv_path),
    }


def _build_json_api_profile_from_env(prefix, key, name, mode, endpoint_url, base_url, dataset_path, config_file_path=""):
    # Load consolidated config file if provided (single-file quickstart)
    file_defaults = {}
    if config_file_path:
        config_path = Path(config_file_path).expanduser()
        if config_path.exists():
            try:
                file_defaults = json.loads(config_path.read_text(encoding="utf-8"))
                if not isinstance(file_defaults, dict):
                    file_defaults = {}
            except (json.JSONDecodeError, OSError):
                file_defaults = {}

    if _is_full_source_profile(file_defaults):
        profile = deepcopy(file_defaults)
        profile.setdefault("key", key)
        profile.setdefault("name", name)
        profile.setdefault("mode", mode)
        profile.setdefault("description", "Sumber JSON API internal perusahaan.")
        return _apply_env_overrides_to_json_api_profile(profile, prefix, endpoint_url, base_url, dataset_path)

    # Env vars override file values; file values override built-in defaults
    endpoint_url = str(endpoint_url or file_defaults.get("url") or "").strip()
    base_url = str(base_url or file_defaults.get("base_url") or "").strip()
    dataset_path = str(dataset_path or file_defaults.get("path") or "").strip()
    if not endpoint_url and not base_url:
        return None

    file_auth = file_defaults.get("auth") or {}
    file_pagination = file_defaults.get("pagination") or {}
    file_retry = file_defaults.get("retry") or {}
    method = os.getenv(f"{prefix}_METHOD", file_defaults.get("method") or "GET").strip().upper()
    profile = {
        "key": key,
        "name": name,
        "mode": mode,
        "type": "json_api",
        "description": "Sumber JSON API internal perusahaan.",
        "endpoint": {
            "url": endpoint_url,
            "base_url": base_url,
            "path": dataset_path,
            "method": method or "GET",
            "timeout": int(os.getenv(f"{prefix}_TIMEOUT", str(file_defaults.get("timeout") or 20))),
            "verify_ssl": os.getenv(f"{prefix}_VERIFY_SSL", str(file_defaults.get("verify_ssl", "true"))).strip().lower() not in {"0", "false", "no"},
            "records_key": os.getenv(f"{prefix}_RECORDS_KEY", file_defaults.get("records_key") or "").strip(),
        },
        "auth": {
            "bearer_token": os.getenv(f"{prefix}_AUTH_TOKEN", "").strip() or str(file_auth.get("bearer_token") or (file_auth.get("basic") or {}).get("token") or "").strip(),
            "basic_username": os.getenv(f"{prefix}_BASIC_USERNAME", "").strip() or str(file_auth.get("basic_username") or (file_auth.get("basic") or {}).get("username") or "").strip(),
            "basic_password": os.getenv(f"{prefix}_BASIC_PASSWORD", "").strip() or str(file_auth.get("basic_password") or (file_auth.get("basic") or {}).get("password") or ""),
        },
        "request": {
            "headers": _parse_json_object(os.getenv(f"{prefix}_HEADERS_JSON", ""), f"{prefix}_HEADERS_JSON") or (file_defaults.get("headers") if isinstance(file_defaults.get("headers"), dict) else {}),
            "query_params": _parse_json_object(
                os.getenv(f"{prefix}_QUERY_PARAMS_JSON", ""),
                f"{prefix}_QUERY_PARAMS_JSON",
            ) or (file_defaults.get("query_params") if isinstance(file_defaults.get("query_params"), dict) else {}),
            "body": _parse_optional_json_value(os.getenv(f"{prefix}_BODY_JSON", ""), f"{prefix}_BODY_JSON") or file_defaults.get("body"),
        },
        "field_map": _parse_json_object(os.getenv(f"{prefix}_FIELD_MAP_JSON", ""), f"{prefix}_FIELD_MAP_JSON") or (file_defaults.get("field_map") if isinstance(file_defaults.get("field_map"), dict) else {}),
        "pagination": {
            "mode": os.getenv(f"{prefix}_PAGINATION_MODE", file_pagination.get("mode") or "").strip().lower(),
            "page_size": int(os.getenv(f"{prefix}_PAGE_SIZE", str(file_pagination.get("page_size") or 0))),
            "cursor_key": os.getenv(f"{prefix}_PAGINATION_CURSOR_KEY", file_pagination.get("cursor_key") or "").strip(),
            "offset_param": os.getenv(f"{prefix}_PAGINATION_OFFSET_PARAM", file_pagination.get("offset_param") or "offset").strip(),
            "limit_param": os.getenv(f"{prefix}_PAGINATION_LIMIT_PARAM", file_pagination.get("limit_param") or "limit").strip(),
            "max_pages": int(os.getenv(f"{prefix}_PAGINATION_MAX_PAGES", str(file_pagination.get("max_pages") or 50))),
        },
        "retry": {
            "max_retries": int(os.getenv(f"{prefix}_MAX_RETRIES", str(file_retry.get("max_retries") or 3))),
            "backoff_base": float(os.getenv(f"{prefix}_RETRY_BACKOFF_BASE", str(file_retry.get("backoff_base") or 1.0))),
        },
    }
    return profile


def _apply_env_overrides_to_json_api_profile(profile, prefix, endpoint_url, base_url, dataset_path):
    profile = deepcopy(profile)
    endpoint = profile.setdefault("endpoint", {})
    auth = profile.setdefault("auth", {})
    request = profile.setdefault("request", {})
    pagination = profile.setdefault("pagination", {})
    retry = profile.setdefault("retry", {})

    if endpoint_url:
        endpoint["url"] = str(endpoint_url).strip()
    if base_url:
        endpoint["base_url"] = str(base_url).strip()
    if dataset_path:
        endpoint["path"] = str(dataset_path).strip()

    env_method = os.getenv(f"{prefix}_METHOD", "").strip().upper()
    if env_method:
        endpoint["method"] = env_method
    env_records_key = os.getenv(f"{prefix}_RECORDS_KEY", "").strip()
    if env_records_key:
        endpoint["records_key"] = env_records_key

    if os.getenv(f"{prefix}_AUTH_TOKEN", "").strip():
        auth["bearer_token"] = os.getenv(f"{prefix}_AUTH_TOKEN", "").strip()
    if os.getenv(f"{prefix}_BASIC_USERNAME", "").strip():
        auth["basic_username"] = os.getenv(f"{prefix}_BASIC_USERNAME", "").strip()
    if os.getenv(f"{prefix}_BASIC_PASSWORD", "").strip():
        auth["basic_password"] = os.getenv(f"{prefix}_BASIC_PASSWORD", "")

    headers = _parse_json_object(os.getenv(f"{prefix}_HEADERS_JSON", ""), f"{prefix}_HEADERS_JSON")
    if headers:
        request["headers"] = headers
    query_params = _parse_json_object(os.getenv(f"{prefix}_QUERY_PARAMS_JSON", ""), f"{prefix}_QUERY_PARAMS_JSON")
    if query_params:
        request["query_params"] = query_params
    body = _parse_optional_json_value(os.getenv(f"{prefix}_BODY_JSON", ""), f"{prefix}_BODY_JSON")
    if body is not None:
        request["body"] = body
    field_map = _parse_json_object(os.getenv(f"{prefix}_FIELD_MAP_JSON", ""), f"{prefix}_FIELD_MAP_JSON")
    if field_map:
        profile["field_map"] = field_map

    for env_name, target, caster in (
        (f"{prefix}_TIMEOUT", endpoint, int),
        (f"{prefix}_PAGE_SIZE", pagination, int),
        (f"{prefix}_PAGINATION_MAX_PAGES", pagination, int),
        (f"{prefix}_MAX_RETRIES", retry, int),
        (f"{prefix}_RETRY_BACKOFF_BASE", retry, float),
    ):
        raw_value = os.getenv(env_name, "").strip()
        if raw_value:
            target_key = {
                f"{prefix}_TIMEOUT": "timeout",
                f"{prefix}_PAGE_SIZE": "page_size",
                f"{prefix}_PAGINATION_MAX_PAGES": "max_pages",
                f"{prefix}_MAX_RETRIES": "max_retries",
                f"{prefix}_RETRY_BACKOFF_BASE": "backoff_base",
            }[env_name]
            target[target_key] = caster(raw_value)

    if os.getenv(f"{prefix}_VERIFY_SSL", "").strip():
        endpoint["verify_ssl"] = os.getenv(f"{prefix}_VERIFY_SSL", "").strip().lower() not in {"0", "false", "no"}
    if os.getenv(f"{prefix}_PAGINATION_MODE", "").strip():
        pagination["mode"] = os.getenv(f"{prefix}_PAGINATION_MODE", "").strip().lower()
    if os.getenv(f"{prefix}_PAGINATION_CURSOR_KEY", "").strip():
        pagination["cursor_key"] = os.getenv(f"{prefix}_PAGINATION_CURSOR_KEY", "").strip()
    if os.getenv(f"{prefix}_PAGINATION_OFFSET_PARAM", "").strip():
        pagination["offset_param"] = os.getenv(f"{prefix}_PAGINATION_OFFSET_PARAM", "").strip()
    if os.getenv(f"{prefix}_PAGINATION_LIMIT_PARAM", "").strip():
        pagination["limit_param"] = os.getenv(f"{prefix}_PAGINATION_LIMIT_PARAM", "").strip()

    return profile


def _profile_from_file(path):
    if not path:
        return None
    profile_path = Path(path).expanduser()
    if not profile_path.exists():
        raise ValueError(f"Profile file not found: {profile_path}")
    payload = json.loads(profile_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Profile file must contain a JSON object: {profile_path}")
    return payload


def summarize_source_profile(profile):
    profile = deepcopy(profile or {})
    summary = {
        "key": profile.get("key") or _normalize_key(profile.get("name") or profile.get("type") or "source"),
        "name": profile.get("name") or "Unnamed source",
        "mode": profile.get("mode") or "demo",
        "type": profile.get("type") or "unknown",
        "description": profile.get("description") or "",
        "configured": False,
    }
    if summary["type"] == "demo_csv":
        csv_path = str(profile.get("path") or "").strip()
        summary.update(
            {
                "configured": bool(csv_path),
                "path": csv_path,
                "pathExists": bool(csv_path) and Path(csv_path).expanduser().exists(),
            }
        )
        return summary

    endpoint = profile.get("endpoint", {}) or {}
    auth = profile.get("auth", {}) or {}
    request = profile.get("request", {}) or {}
    summary.update(
        {
            "configured": bool(endpoint.get("url") or endpoint.get("base_url")),
            "datasetUrl": endpoint.get("url") or endpoint.get("base_url"),
            "method": endpoint.get("method") or "GET",
            "recordsKeyConfigured": bool(endpoint.get("records_key")),
            "basicAuthConfigured": bool(auth.get("basic_username")),
            "bearerAuthConfigured": bool(auth.get("bearer_token")),
            "bodyConfigured": request.get("body") is not None,
            "headerCount": len(request.get("headers") or {}),
            "queryParamCount": len(request.get("query_params") or {}),
        }
    )
    return summary


def load_available_source_profiles(
    demo_csv_path,
    legacy_data_mode,
    internal_api_endpoint_url,
    internal_api_base_url,
    internal_api_dataset_path,
    demo_profile_path="",
    production_profile_path="",
    config_file_path="",
):
    profiles = {}
    issues = []

    try:
        demo_profile = _profile_from_file(demo_profile_path) or _build_demo_profile(demo_csv_path)
        demo_profile.setdefault("key", "demo")
        demo_profile.setdefault("name", "Demo Lokal")
        demo_profile.setdefault("mode", "demo")
        demo_profile.setdefault("type", "demo_csv")
        profiles["demo"] = demo_profile
    except Exception as exc:
        issues.append(f"Profil demo gagal dimuat: {exc}")

    try:
        production_profile = (
            _profile_from_file(production_profile_path)
            if production_profile_path
            else _build_json_api_profile_from_env(
                prefix="INTERNAL_API",
                key="production",
                name="Produksi API Internal",
                mode="production",
                endpoint_url=internal_api_endpoint_url,
                base_url=internal_api_base_url,
                dataset_path=internal_api_dataset_path,
                config_file_path=config_file_path,
            )
        )
        if production_profile:
            production_profile.setdefault("key", "production")
            production_profile.setdefault("name", "Produksi API Internal")
            production_profile.setdefault("mode", "production")
            production_profile.setdefault("type", "json_api")
            profiles["production"] = production_profile
    except Exception as exc:
        issues.append(f"Profil produksi gagal dimuat: {exc}")

    default_key = "production" if str(legacy_data_mode or "").strip().lower() == "internal_api" else "demo"
    return profiles, issues, default_key


def read_active_source_key(state_path):
    if not state_path:
        return None
    path = Path(state_path).expanduser()
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    source_key = str(payload.get("active_source_key") or "").strip().lower()
    return source_key or None


def write_active_source_key(state_path, source_key):
    if not state_path:
        return
    path = Path(state_path).expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"active_source_key": str(source_key or "").strip().lower()}
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def resolve_active_source_profile(profiles, state_path, legacy_default_key):
    profiles = profiles or {}
    selected_key = read_active_source_key(state_path) or legacy_default_key or "demo"
    if selected_key in profiles:
        return selected_key, deepcopy(profiles[selected_key])
    if "demo" in profiles:
        return "demo", deepcopy(profiles["demo"])
    if profiles:
        first_key = next(iter(profiles.keys()))
        return first_key, deepcopy(profiles[first_key])
    raise RuntimeError("No data source profiles are available.")
