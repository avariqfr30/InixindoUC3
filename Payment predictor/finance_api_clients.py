import copy
import json
import logging
import re
import threading
import time
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests

from config import (
    CASH_OUT_API_AUTH_TOKEN,
    CASH_OUT_API_BASIC_PASSWORD,
    CASH_OUT_API_BASIC_USERNAME,
    CASH_OUT_API_BODY_JSON,
    CASH_OUT_API_ENDPOINT_URL,
    CASH_OUT_API_HEADERS_JSON,
    CASH_OUT_API_METHOD,
    CASH_OUT_API_QUERY_PARAMS_JSON,
    CASH_OUT_API_RECORDS_KEY,
    CASH_OUT_API_TIMEOUT,
    CASH_OUT_API_VERIFY_SSL,
    CASH_OUT_FIELD_MAP_JSON,
    INTERNAL_API_AUTH_TOKEN,
    INTERNAL_API_BASE_URL,
    INTERNAL_API_BASIC_PASSWORD,
    INTERNAL_API_BASIC_USERNAME,
    INTERNAL_API_BODY_JSON,
    INTERNAL_API_DATASET_PATH,
    INTERNAL_API_ENDPOINT_URL,
    INTERNAL_API_FIELD_MAP_JSON,
    INTERNAL_API_HEADERS_JSON,
    INTERNAL_API_MAX_RETRIES,
    INTERNAL_API_METHOD,
    INTERNAL_API_PAGE_SIZE,
    INTERNAL_API_PAGINATION_CURSOR_KEY,
    INTERNAL_API_PAGINATION_LIMIT_PARAM,
    INTERNAL_API_PAGINATION_MAX_PAGES,
    INTERNAL_API_PAGINATION_MODE,
    INTERNAL_API_PAGINATION_OFFSET_PARAM,
    INTERNAL_API_QUERY_PARAMS_JSON,
    INTERNAL_API_RECORDS_KEY,
    INTERNAL_API_RETRY_BACKOFF_BASE,
    INTERNAL_API_TIMEOUT,
    INTERNAL_API_VERIFY_SSL,
)
from data_contract import extract_records_from_payload, parse_internal_api_field_map
from forecast_engine import parse_idr_amount

logger = logging.getLogger(__name__)

class InternalAPIClient:
    def __init__(self, source_profile=None):
        profile = copy.deepcopy(source_profile) if source_profile else {
            "type": "json_api",
            "endpoint": {
                "url": INTERNAL_API_ENDPOINT_URL.strip(),
                "base_url": INTERNAL_API_BASE_URL.rstrip("/"),
                "path": INTERNAL_API_DATASET_PATH.strip() or "/api/finance/invoices",
                "method": (INTERNAL_API_METHOD or "GET").strip().upper(),
                "timeout": INTERNAL_API_TIMEOUT,
                "verify_ssl": INTERNAL_API_VERIFY_SSL,
                "records_key": INTERNAL_API_RECORDS_KEY.strip(),
            },
            "auth": {
                "bearer_token": INTERNAL_API_AUTH_TOKEN.strip(),
                "basic_username": INTERNAL_API_BASIC_USERNAME.strip(),
                "basic_password": INTERNAL_API_BASIC_PASSWORD,
            },
            "request": {
                "headers": self._parse_json_object(INTERNAL_API_HEADERS_JSON, "headers"),
                "query_params": self._parse_json_object(
                    INTERNAL_API_QUERY_PARAMS_JSON,
                    "query params",
                ),
                "body": self._parse_optional_json_value(INTERNAL_API_BODY_JSON, "body"),
            },
            "field_map": parse_internal_api_field_map(INTERNAL_API_FIELD_MAP_JSON),
            "pagination": {
                "mode": INTERNAL_API_PAGINATION_MODE,
                "page_size": INTERNAL_API_PAGE_SIZE,
                "cursor_key": INTERNAL_API_PAGINATION_CURSOR_KEY,
                "offset_param": INTERNAL_API_PAGINATION_OFFSET_PARAM,
                "limit_param": INTERNAL_API_PAGINATION_LIMIT_PARAM,
                "max_pages": INTERNAL_API_PAGINATION_MAX_PAGES,
            },
            "retry": {
                "max_retries": INTERNAL_API_MAX_RETRIES,
                "backoff_base": INTERNAL_API_RETRY_BACKOFF_BASE,
            },
        }
        endpoint = profile.get("endpoint", {}) or {}
        auth = profile.get("auth", {}) or {}
        request_config = profile.get("request", {}) or {}
        pagination_config = profile.get("pagination", {}) or {}
        retry_config = profile.get("retry", {}) or {}

        self.source_profile = profile
        self.endpoint_url = str(endpoint.get("url") or "").strip()
        self.base_url = str(endpoint.get("base_url") or "").strip().rstrip("/")
        self.dataset_path = str(endpoint.get("path") or INTERNAL_API_DATASET_PATH or "/api/finance/invoices").strip()
        self.method = str(endpoint.get("method") or "GET").strip().upper()
        self.records_key = str(endpoint.get("records_key") or "").strip()
        raw_auth_token = str(auth.get("bearer_token") or "").strip()
        self.auth_token = INTERNAL_API_AUTH_TOKEN.strip() if raw_auth_token == "__ENV__" else raw_auth_token
        self.basic_username = str(auth.get("basic_username") or "").strip()
        self.basic_password = str(auth.get("basic_password") or "")
        self.timeout = int(endpoint.get("timeout") or INTERNAL_API_TIMEOUT)
        verify_ssl_value = endpoint.get("verify_ssl")
        if isinstance(verify_ssl_value, str):
            self.verify_ssl = verify_ssl_value.strip().lower() not in {"0", "false", "no"}
        elif verify_ssl_value is None:
            self.verify_ssl = INTERNAL_API_VERIFY_SSL
        else:
            self.verify_ssl = bool(verify_ssl_value)
        self.headers = dict(request_config.get("headers") or {})
        self.body = request_config.get("body")
        self.body_format = str(request_config.get("body_format") or "json").strip().lower()
        self.query_params = dict(request_config.get("query_params") or {})
        self.field_map = parse_internal_api_field_map(profile.get("field_map") or {})

        # Pagination config
        self.pagination_mode = str(pagination_config.get("mode") or "").strip().lower()
        self.page_size = int(pagination_config.get("page_size") or 0)
        self.pagination_cursor_key = str(pagination_config.get("cursor_key") or "").strip()
        self.pagination_offset_param = str(pagination_config.get("offset_param") or "offset").strip()
        self.pagination_limit_param = str(pagination_config.get("limit_param") or "limit").strip()
        self.pagination_max_pages = int(pagination_config.get("max_pages") or 50)

        # Retry config
        self.max_retries = max(int(retry_config.get("max_retries") or 3), 1)
        self.retry_backoff_base = max(float(retry_config.get("backoff_base") or 1.0), 0.1)

    @staticmethod
    def _parse_json_object(raw_value, label):
        if not raw_value:
            return {}

        try:
            parsed = json.loads(raw_value)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid INTERNAL_API_{label.upper().replace(' ', '_')}_JSON: {exc}") from exc

        if not isinstance(parsed, dict):
            raise ValueError(f"INTERNAL_API_{label.upper().replace(' ', '_')}_JSON must be a JSON object.")

        return parsed

    @staticmethod
    def _parse_optional_json_value(raw_value, label):
        if not raw_value:
            return None
        try:
            return json.loads(raw_value)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid INTERNAL_API_{label.upper()}_JSON: {exc}") from exc

    def is_configured(self):
        return bool(self.endpoint_url or self.base_url)

    def get_dataset_url(self):
        if self.endpoint_url:
            return self.endpoint_url
        return urljoin(f"{self.base_url}/", self.dataset_path.lstrip("/"))

    def validate_endpoint_url(self):
        """Boot-time connectivity pre-check. Returns (ok, message)."""
        if not self.is_configured():
            return False, "Internal API endpoint is not configured (INTERNAL_API_ENDPOINT_URL or INTERNAL_API_BASE_URL is empty)."
        dataset_url = self.get_dataset_url()
        parsed = urlparse(dataset_url)
        if parsed.scheme not in {"http", "https"}:
            return False, f"Internal API URL has invalid scheme '{parsed.scheme}': {dataset_url}"
        if not parsed.hostname:
            return False, f"Internal API URL has no hostname: {dataset_url}"
        try:
            requests.head(dataset_url, timeout=5, verify=self.verify_ssl)
            return True, f"Internal API endpoint is reachable: {dataset_url}"
        except requests.ConnectionError:
            return False, f"Internal API endpoint is unreachable (connection refused or DNS failure): {dataset_url}"
        except requests.Timeout:
            return False, f"Internal API endpoint timed out after 5s: {dataset_url}"
        except Exception as exc:
            return False, f"Internal API endpoint pre-check failed: {dataset_url} — {exc}"

    def _build_request_kwargs(self, extra_params=None):
        headers = {"Accept": "application/json"}
        headers.update(self.headers)
        auth = None
        if self.basic_username:
            auth = (self.basic_username, self.basic_password)
        if self.auth_token:
            headers.setdefault("Authorization", f"Bearer {self.auth_token}")

        params = dict(self.query_params)
        if extra_params:
            params.update(extra_params)

        request_kwargs = {
            "headers": headers,
            "params": params,
            "timeout": self.timeout,
            "verify": self.verify_ssl,
        }
        if auth:
            request_kwargs["auth"] = auth
        if self.method != "GET" and self.body is not None:
            if self.body_format == "form":
                request_kwargs["data"] = self.body
                headers.setdefault("Content-Type", "application/x-www-form-urlencoded")
            else:
                request_kwargs["json"] = self.body
                headers.setdefault("Content-Type", "application/json")

        return request_kwargs, auth

    def _execute_request_with_retry(self, dataset_url, request_kwargs):
        """Execute HTTP request with exponential backoff retry."""
        last_exc = None
        for attempt in range(self.max_retries):
            try:
                response = requests.request(
                    self.method,
                    dataset_url,
                    **request_kwargs,
                )
                if response.status_code >= 500 and attempt < self.max_retries - 1:
                    logger.warning(
                        "Internal API returned HTTP %s on attempt %s/%s for %s %s. Retrying...",
                        response.status_code, attempt + 1, self.max_retries, self.method, dataset_url,
                    )
                    time.sleep(self.retry_backoff_base * (2 ** attempt))
                    continue
                if response.status_code >= 400:
                    body_preview = (response.text or "")[:300]
                    raise RuntimeError(
                        f"Internal API returned HTTP {response.status_code} for {self.method} {dataset_url}. "
                        f"Response body (truncated): {body_preview}"
                    )
                return response
            except requests.ConnectionError as exc:
                last_exc = exc
                if attempt < self.max_retries - 1:
                    wait = self.retry_backoff_base * (2 ** attempt)
                    logger.warning(
                        "Connection to %s failed on attempt %s/%s. Retrying in %.1fs...",
                        dataset_url, attempt + 1, self.max_retries, wait,
                    )
                    time.sleep(wait)
                    continue
                raise RuntimeError(
                    f"Internal API at {dataset_url} is unreachable after {self.max_retries} attempts. "
                    f"Check INTERNAL_API_ENDPOINT_URL and network connectivity. Last error: {exc}"
                ) from exc
            except requests.Timeout as exc:
                last_exc = exc
                if attempt < self.max_retries - 1:
                    wait = self.retry_backoff_base * (2 ** attempt)
                    logger.warning(
                        "Request to %s timed out on attempt %s/%s. Retrying in %.1fs...",
                        dataset_url, attempt + 1, self.max_retries, wait,
                    )
                    time.sleep(wait)
                    continue
                raise RuntimeError(
                    f"Internal API at {dataset_url} timed out after {self.timeout}s "
                    f"({self.max_retries} attempts). Increase INTERNAL_API_TIMEOUT or check endpoint performance."
                ) from exc
        raise RuntimeError(f"Internal API request failed after {self.max_retries} retries.") from last_exc

    def _parse_response_payload(self, response, dataset_url):
        """Parse and validate the JSON response, returning the raw payload."""
        try:
            payload = response.json()
        except ValueError:
            body_preview = (response.text or "")[:300]
            raise RuntimeError(
                f"Internal API at {dataset_url} returned non-JSON response "
                f"(Content-Type: {response.headers.get('Content-Type', 'unknown')}). "
                f"Body (truncated): {body_preview}"
            )

        if isinstance(payload, dict) and payload.get("success") is False:
            message = payload.get("message") or payload.get("error") or "unknown error"
            raise RuntimeError(
                f"Internal API returned a business error for {self.method} {dataset_url}: {message}"
            )
        if (
            isinstance(payload, dict)
            and payload.get("success") is True
            and "data" in payload
            and payload.get("data") is None
        ):
            raise RuntimeError(
                f"Internal API at {dataset_url} returned `data: null`. The endpoint is reachable and "
                "credentials are valid, but it still needs the correct POST payload. "
                "Set INTERNAL_API_BODY_JSON once the company shares the required request body."
            )
        return payload

    def _fetch_single_page(self, extra_params=None):
        """Fetch a single page of records."""
        dataset_url = self.get_dataset_url()
        request_kwargs, auth = self._build_request_kwargs(extra_params)
        response = self._execute_request_with_retry(dataset_url, request_kwargs)
        payload = self._parse_response_payload(response, dataset_url)
        return payload, response, auth

    def _is_pagination_enabled(self):
        return self.pagination_mode in {"offset", "cursor", "link"} and self.page_size > 0

    def _fetch_with_pagination(self):
        """Fetch all records across multiple pages."""
        dataset_url = self.get_dataset_url()
        all_records = []
        total_pages = 0

        if self.pagination_mode == "offset":
            offset = 0
            for page in range(self.pagination_max_pages):
                total_pages += 1
                extra_params = {
                    self.pagination_offset_param: offset,
                    self.pagination_limit_param: self.page_size,
                }
                payload, response, auth = self._fetch_single_page(extra_params)
                page_records, _ = extract_records_from_payload(
                    payload, explicit_records_path=self.records_key or None,
                )
                all_records.extend(page_records)
                if len(page_records) < self.page_size:
                    break
                offset += self.page_size

        elif self.pagination_mode == "cursor":
            cursor = None
            for page in range(self.pagination_max_pages):
                total_pages += 1
                extra_params = {self.pagination_limit_param: self.page_size}
                if cursor:
                    extra_params[self.pagination_cursor_key or "cursor"] = cursor
                payload, response, auth = self._fetch_single_page(extra_params)
                page_records, _ = extract_records_from_payload(
                    payload, explicit_records_path=self.records_key or None,
                )
                all_records.extend(page_records)
                next_cursor = None
                if isinstance(payload, dict):
                    cursor_path = self.pagination_cursor_key or "next_cursor"
                    next_cursor = payload.get(cursor_path) or payload.get("meta", {}).get(cursor_path)
                if not next_cursor or len(page_records) < self.page_size:
                    break
                cursor = next_cursor

        elif self.pagination_mode == "link":
            next_url = dataset_url
            for page in range(self.pagination_max_pages):
                total_pages += 1
                extra_params = {self.pagination_limit_param: self.page_size} if page == 0 else {}
                if page == 0:
                    payload, response, auth = self._fetch_single_page(extra_params)
                else:
                    request_kwargs, auth = self._build_request_kwargs()
                    request_kwargs["params"] = {}
                    response = self._execute_request_with_retry(next_url, request_kwargs)
                    payload = self._parse_response_payload(response, next_url)
                page_records, _ = extract_records_from_payload(
                    payload, explicit_records_path=self.records_key or None,
                )
                all_records.extend(page_records)
                link_header = response.headers.get("Link", "")
                next_url = self._parse_link_next(link_header)
                if not next_url or len(page_records) < self.page_size:
                    break

        logger.info("Pagination completed: %s records across %s pages.", len(all_records), total_pages)
        return all_records, auth, total_pages

    @staticmethod
    def _parse_link_next(link_header):
        """Parse RFC 5988 Link header for rel='next'."""
        if not link_header:
            return None
        for part in link_header.split(","):
            if 'rel="next"' in part or "rel='next'" in part:
                match = re.search(r"<([^>]+)>", part)
                if match:
                    return match.group(1)
        return None

    def fetch_records(self, preview_limit=0):
        if not self.is_configured():
            raise RuntimeError(
                "Internal API endpoint is not configured. "
                "Set INTERNAL_API_ENDPOINT_URL or INTERNAL_API_BASE_URL."
            )

        dataset_url = self.get_dataset_url()

        if self._is_pagination_enabled() and preview_limit <= 0:
            all_records, auth, total_pages = self._fetch_with_pagination()
            extraction_summary = {
                "strategy": "paginated",
                "paginationMode": self.pagination_mode,
                "pageSize": self.page_size,
                "totalPages": total_pages,
                "recordCount": len(all_records),
            }
        else:
            extra_params = {}
            if preview_limit > 0 and self.page_size > 0:
                extra_params[self.pagination_limit_param] = preview_limit
            payload, response, auth = self._fetch_single_page(extra_params)
            all_records, extraction_summary = extract_records_from_payload(
                payload,
                explicit_records_path=self.records_key or None,
            )
            if preview_limit > 0:
                all_records = all_records[:preview_limit]
                extraction_summary["previewLimit"] = preview_limit
                extraction_summary["recordCount"] = len(all_records)

        extraction_summary["datasetUrl"] = dataset_url
        extraction_summary["requestMethod"] = self.method
        extraction_summary["authMode"] = "basic" if auth else ("bearer" if self.auth_token else "none")
        return all_records, extraction_summary

class CashOutAPIClient(InternalAPIClient):
    FIELD_ALIASES = {
        "amount": (
            "amount",
            "amount_idr",
            "nominal",
            "nilai",
            "cash_out",
            "outflow",
            "expense_amount",
            "planned_amount",
            "total_amount",
            "value",
        ),
        "due_date": (
            "due_date",
            "payment_date",
            "planned_date",
            "tanggal_bayar",
            "tanggal_jatuh_tempo",
            "jatuh_tempo",
            "due",
            "date",
            "tanggal",
        ),
        "category": (
            "category",
            "expense_category",
            "cash_out_category",
            "jenis_biaya",
            "kategori",
            "cost_center",
        ),
        "reference": (
            "reference",
            "reference_code",
            "code",
            "id",
            "reference_id",
            "document_no",
            "invoice_no",
            "nama",
            "name",
        ),
        "status": (
            "status",
            "payment_status",
            "approval_status",
            "state",
        ),
        "description": (
            "description",
            "note",
            "notes",
            "memo",
            "remarks",
            "keterangan",
        ),
    }

    REQUIRED_FIELDS = ("amount", "due_date")

    def __init__(self):
        self.endpoint_url = CASH_OUT_API_ENDPOINT_URL.strip()
        self.base_url = ""
        self.dataset_path = ""
        self.method = (CASH_OUT_API_METHOD or "GET").strip().upper()
        self.records_key = CASH_OUT_API_RECORDS_KEY.strip()
        self.auth_token = CASH_OUT_API_AUTH_TOKEN.strip()
        self.basic_username = CASH_OUT_API_BASIC_USERNAME.strip()
        self.basic_password = CASH_OUT_API_BASIC_PASSWORD
        self.timeout = CASH_OUT_API_TIMEOUT
        self.verify_ssl = CASH_OUT_API_VERIFY_SSL
        self.headers = self._parse_json_object(CASH_OUT_API_HEADERS_JSON, "headers")
        self.body = self._parse_optional_json_value(CASH_OUT_API_BODY_JSON, "body")
        self.field_map = self._parse_field_map(CASH_OUT_FIELD_MAP_JSON)
        self.query_params = self._parse_json_object(
            CASH_OUT_API_QUERY_PARAMS_JSON,
            "query params",
        )

    @classmethod
    def _parse_field_map(cls, raw_value):
        if not raw_value:
            return {}
        try:
            candidate = json.loads(raw_value)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid CASH_OUT_FIELD_MAP_JSON: {exc}") from exc
        if not isinstance(candidate, dict):
            raise ValueError("CASH_OUT_FIELD_MAP_JSON must be a JSON object.")

        normalized_map = {}
        for key, value in candidate.items():
            normalized_key = cls._normalize_key(key)
            if normalized_key not in cls.FIELD_ALIASES:
                raise ValueError(
                    "CASH_OUT_FIELD_MAP_JSON contains an unknown field key: "
                    f"{key}. Use amount, due_date, category, reference, status, or description."
                )
            normalized_map[normalized_key] = str(value).strip()
        return normalized_map

    @staticmethod
    def _normalize_key(value):
        return re.sub(r"[^a-z0-9]+", "_", str(value or "").strip().lower()).strip("_")

    @classmethod
    def _resolve_columns(cls, data_frame, explicit_field_map=None):
        explicit_field_map = explicit_field_map or {}
        columns = list(data_frame.columns)
        normalized_columns = {
            cls._normalize_key(column): column
            for column in columns
        }

        resolved = {}
        missing = []
        for canonical_field, aliases in cls.FIELD_ALIASES.items():
            explicit_column = explicit_field_map.get(canonical_field)
            if explicit_column:
                if explicit_column in data_frame.columns:
                    resolved[canonical_field] = explicit_column
                    continue
                normalized_explicit = cls._normalize_key(explicit_column)
                if normalized_explicit in normalized_columns:
                    resolved[canonical_field] = normalized_columns[normalized_explicit]
                    continue

            for alias in aliases:
                normalized_alias = cls._normalize_key(alias)
                if normalized_alias in normalized_columns:
                    resolved[canonical_field] = normalized_columns[normalized_alias]
                    break

            if canonical_field in cls.REQUIRED_FIELDS and canonical_field not in resolved:
                missing.append(canonical_field)

        return resolved, missing

    @staticmethod
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

    @staticmethod
    def _parse_due_date(raw_value):
        if raw_value is None or (isinstance(raw_value, float) and pd.isna(raw_value)):
            return None
        parsed_value = pd.to_datetime(raw_value, errors="coerce")
        if pd.isna(parsed_value):
            return None
        return parsed_value.to_pydatetime()

    @staticmethod
    def _is_open_status(raw_status):
        status = str(raw_status or "").strip().lower()
        if not status:
            return True
        return status not in {"paid", "settled", "closed", "cancelled", "canceled", "done"}

    def fetch_commitments(self):
        if not self.is_configured():
            return [], self.get_summary()

        records, extraction_summary = self.fetch_records()
        raw_data_frame = self._normalize_records(records)
        if raw_data_frame.empty:
            return [], {
                **self.get_summary(),
                "datasetUrl": self.get_dataset_url(),
                "requestMethod": self.method,
                "recordCount": 0,
                "missingRequiredFields": list(self.REQUIRED_FIELDS),
            }

        resolved_columns, missing_fields = self._resolve_columns(raw_data_frame, self.field_map)
        normalized_records = []
        for _, row in raw_data_frame.iterrows():
            amount_column = resolved_columns.get("amount")
            due_date_column = resolved_columns.get("due_date")
            try:
                amount = parse_idr_amount(row.get(amount_column, 0)) if amount_column else 0
            except ValueError:
                continue
            due_date = self._parse_due_date(row.get(due_date_column)) if due_date_column else None
            if amount <= 0 or due_date is None:
                continue
            normalized_records.append(
                {
                    "amount": amount,
                    "due_date": due_date,
                    "category": str(row.get(resolved_columns.get("category"), "")).strip(),
                    "reference": str(row.get(resolved_columns.get("reference"), "")).strip(),
                    "status": str(row.get(resolved_columns.get("status"), "")).strip(),
                    "description": str(row.get(resolved_columns.get("description"), "")).strip(),
                    "is_open": self._is_open_status(row.get(resolved_columns.get("status"), "")),
                }
            )

        summary = {
            **self.get_summary(),
            **extraction_summary,
            "recordCount": len(normalized_records),
            "resolvedFields": resolved_columns,
            "missingRequiredFields": missing_fields,
        }
        return normalized_records, summary

    def get_summary(self):
        return {
            "configured": self.is_configured(),
            "datasetUrl": self.get_dataset_url() if self.is_configured() else None,
            "requestMethod": self.method,
        }

class CashOutStore:
    def __init__(self):
        self.client = CashOutAPIClient()
        self.records = []
        self.summary = self.client.get_summary()
        self.lock = threading.Lock()
        self.refresh_lock = threading.Lock()
        self.version = 0
        self.sync_status = "not_configured" if not self.client.is_configured() else "not_loaded"
        self.last_sync_started_at = None
        self.last_sync_at = None
        self.last_success_at = None
        self.last_sync_duration_seconds = None
        self.last_sync_error = None
        if self.client.is_configured():
            self.refresh_data()

    def refresh_data(self):
        if not self.client.is_configured():
            with self.lock:
                self.records = []
                self.summary = self.client.get_summary()
                self.sync_status = "not_configured"
                self.last_sync_error = None
            return False

        with self.refresh_lock:
            started_at = datetime.now()
            with self.lock:
                self.sync_status = "refreshing"
                self.last_sync_started_at = started_at
                self.last_sync_error = None

            try:
                records, summary = self.client.fetch_commitments()
                completed_at = datetime.now()
                with self.lock:
                    self.records = records
                    self.summary = summary
                    self.version += 1
                    self.sync_status = "ready"
                    self.last_sync_at = completed_at
                    self.last_success_at = completed_at
                    self.last_sync_duration_seconds = round((completed_at - started_at).total_seconds(), 2)
                    self.last_sync_error = None
                return True
            except Exception as exc:
                completed_at = datetime.now()
                logger.error("Cash-out source sync failed: %s", exc)
                with self.lock:
                    self.records = []
                    self.summary = {
                        **self.client.get_summary(),
                        "recordCount": 0,
                        "missingRequiredFields": list(self.client.REQUIRED_FIELDS),
                    }
                    self.sync_status = "error"
                    self.last_sync_at = completed_at
                    self.last_sync_duration_seconds = round((completed_at - started_at).total_seconds(), 2)
                    self.last_sync_error = str(exc)
                return False

    def get_records(self):
        with self.lock:
            return copy.deepcopy(self.records)

    def get_status(self, refresh_interval_seconds=0):
        with self.lock:
            last_success_at = self.last_success_at
            next_refresh_at = None
            if refresh_interval_seconds > 0 and last_success_at is not None:
                next_refresh_at = last_success_at + timedelta(seconds=refresh_interval_seconds)
            source_age_minutes = None
            if last_success_at is not None:
                source_age_minutes = round((datetime.now() - last_success_at).total_seconds() / 60, 1)
            return {
                "configured": self.client.is_configured(),
                "syncStatus": self.sync_status,
                "lastSyncStartedAt": self.last_sync_started_at.isoformat() if self.last_sync_started_at else None,
                "lastSyncAt": self.last_sync_at.isoformat() if self.last_sync_at else None,
                "lastSuccessAt": last_success_at.isoformat() if last_success_at else None,
                "lastSyncDurationSeconds": self.last_sync_duration_seconds,
                "lastSyncError": self.last_sync_error,
                "sourceAgeMinutes": source_age_minutes,
                "nextRefreshAt": next_refresh_at.isoformat() if next_refresh_at else None,
                "recordCount": len(self.records),
                "version": self.version,
                "summary": copy.deepcopy(self.summary),
            }
