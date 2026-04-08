import json
import re
from copy import deepcopy

import pandas as pd


INTERNAL_API_FIELD_SPECS = {
    "period": {
        "label": "Periode Laporan",
        "required": True,
        "description": "Label periode atau horizon invoice, misalnya `Q2 2025` atau `Januari 2026`.",
        "aliases": (
            "period",
            "report period",
            "invoice period",
            "periode",
            "periode laporan",
            "billing period",
            "periode tagihan",
            "reporting_period",
            "invoice_period",
        ),
        "example": "Q2 2025",
    },
    "partner_type": {
        "label": "Tipe Partner",
        "required": True,
        "description": "Segmentasi partner atau tipe pelanggan.",
        "aliases": (
            "partner type",
            "partner",
            "customer type",
            "segment",
            "customer segment",
            "tipe partner",
            "jenis partner",
            "customer_segment",
            "partner_segment",
            "client_type",
        ),
        "example": "Instansi Pemerintah",
    },
    "service": {
        "label": "Layanan",
        "required": True,
        "description": "Nama layanan, produk, atau use case yang ditagihkan.",
        "aliases": (
            "service",
            "product",
            "offering",
            "line of business",
            "layanan",
            "service_name",
            "product_name",
            "program_name",
        ),
        "example": "Audit SPBE",
    },
    "payment_class": {
        "label": "Kelas Pembayaran",
        "required": True,
        "description": "Kelas perilaku pembayaran, misalnya `Kelas A` sampai `Kelas E`.",
        "aliases": (
            "payment class",
            "payment_class",
            "collection class",
            "risk class",
            "kelas pembayaran",
            "bucket pembayaran",
            "collection_bucket",
            "payment_bucket",
        ),
        "example": "Kelas C (Telat 1-2 Bulan)",
    },
    "invoice_value": {
        "label": "Nilai Invoice",
        "required": True,
        "description": "Nominal invoice dalam Rupiah/IDR.",
        "aliases": (
            "invoice value",
            "invoice amount",
            "amount",
            "amount_idr",
            "outstanding",
            "nilai invoice",
            "nominal invoice",
            "nominal",
            "invoice_total",
            "tagihan",
            "balance",
        ),
        "example": "Rp 180.000.000",
    },
    "delay_note": {
        "label": "Catatan Historis Keterlambatan",
        "required": False,
        "description": "Catatan penyebab keterlambatan, approval, dispute, atau hambatan operasional.",
        "aliases": (
            "delay note",
            "delay notes",
            "delay reason",
            "collection note",
            "historical delay note",
            "catatan keterlambatan",
            "catatan historis keterlambatan",
            "catatan penagihan",
            "notes",
            "delay_reason",
            "reason",
            "comment",
        ),
        "example": "Pembayaran tertunda karena revisi DIPA dan verifikasi dokumen termin belum selesai.",
    },
}

INTERNAL_API_ENVELOPE_CANDIDATES = ("records", "items", "results", "data", "invoices")

MONTH_TOKENS = (
    "jan",
    "feb",
    "mar",
    "apr",
    "mei",
    "may",
    "jun",
    "jul",
    "aug",
    "agu",
    "sep",
    "oct",
    "okt",
    "nov",
    "dec",
    "des",
    "januari",
    "februari",
    "maret",
    "april",
    "juni",
    "juli",
    "agustus",
    "september",
    "oktober",
    "november",
    "desember",
)
PARTNER_HINTS = (
    "pemerintah",
    "instansi",
    "startup",
    "swasta",
    "universitas",
    "kampus",
    "bumn",
    "enterprise",
    "korporasi",
    "partner",
    "customer",
    "pelanggan",
    "klien",
    "komunitas",
    "umkm",
)
DELAY_HINTS = (
    "delay",
    "telat",
    "tertunda",
    "approval",
    "dokumen",
    "document",
    "revisi",
    "budget",
    "anggaran",
    "cashflow",
    "dispute",
    "klarifikasi",
    "follow up",
    "follow-up",
    "invoice",
)


def _normalize_name(value):
    text = str(value or "").strip().lower()
    return re.sub(r"[^a-z0-9]+", "", text)


def _value_to_text(value):
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return str(value).strip()


def _canonical_key_from_config_key(config_key):
    if config_key in INTERNAL_API_FIELD_SPECS:
        return config_key

    normalized = _normalize_name(config_key)
    for canonical_key, field_spec in INTERNAL_API_FIELD_SPECS.items():
        candidate_names = (field_spec["label"], *field_spec["aliases"])
        if any(normalized == _normalize_name(candidate_name) for candidate_name in candidate_names):
            return canonical_key
    return None


def parse_internal_api_field_map(raw_value):
    if not raw_value:
        return {}

    if isinstance(raw_value, dict):
        candidate = raw_value
    else:
        try:
            candidate = json.loads(raw_value)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid INTERNAL_API_FIELD_MAP_JSON: {exc}") from exc

    if not isinstance(candidate, dict):
        raise ValueError("INTERNAL_API_FIELD_MAP_JSON must be a JSON object.")

    normalized_mapping = {}
    for config_key, source_field in candidate.items():
        canonical_key = _canonical_key_from_config_key(config_key)
        if canonical_key is None:
            raise ValueError(
                "INTERNAL_API_FIELD_MAP_JSON contains an unknown field key: "
                f"{config_key}. Use canonical keys such as period, partner_type, "
                "service, payment_class, invoice_value, or delay_note."
            )
        normalized_mapping[canonical_key] = str(source_field).strip()

    return normalized_mapping


def _split_path_tokens(path):
    tokens = []
    for raw_chunk in str(path or "").split("."):
        if raw_chunk == "":
            continue
        sub_tokens = re.findall(r"[^\[\]]+|\[\d+\]", raw_chunk)
        for token in sub_tokens:
            if token.startswith("[") and token.endswith("]"):
                tokens.append(int(token[1:-1]))
            else:
                tokens.append(token)
    return tokens


def extract_path_value(payload, path):
    current = payload
    for token in _split_path_tokens(path):
        if isinstance(token, int):
            if not isinstance(current, list) or token >= len(current):
                raise KeyError(path)
            current = current[token]
            continue

        if not isinstance(current, dict) or token not in current:
            raise KeyError(path)
        current = current[token]
    return current


def _coerce_node_to_records(node):
    if isinstance(node, list):
        dict_records = [item for item in node if isinstance(item, dict)]
        if dict_records:
            return dict_records
        return None
    if isinstance(node, dict):
        return [node]
    return None


def _iter_payload_candidates(node, path="$", depth=0, max_depth=7):
    if depth > max_depth:
        return

    candidate_records = _coerce_node_to_records(node)
    if candidate_records:
        yield path, candidate_records

    if isinstance(node, dict):
        for key, value in node.items():
            yield from _iter_payload_candidates(value, f"{path}.{key}", depth + 1, max_depth=max_depth)
    elif isinstance(node, list):
        for index, value in enumerate(node[:5]):
            if isinstance(value, (dict, list)):
                yield from _iter_payload_candidates(value, f"{path}[{index}]", depth + 1, max_depth=max_depth)


def _has_numeric_shape(text):
    cleaned = re.sub(r"[^\d]", "", text)
    return bool(cleaned)


def _safe_unique_ratio(series):
    non_null = series.dropna()
    if non_null.empty:
        return 0.0
    sample = non_null.astype(str).str.strip()
    return float(sample.nunique(dropna=True)) / float(len(sample))


def _score_invoice_value(series):
    if pd.api.types.is_numeric_dtype(series):
        return 0.95

    samples = [_value_to_text(value) for value in series.dropna().head(25)]
    if not samples:
        return 0.0
    parseable = sum(1 for sample in samples if _has_numeric_shape(sample))
    ratio = parseable / len(samples)
    if ratio < 0.6:
        return 0.0
    if any("rp" in sample.lower() or "idr" in sample.lower() for sample in samples):
        return min(0.98, 0.65 + ratio * 0.3)
    return 0.55 + ratio * 0.25


def _score_payment_class(series):
    samples = [_value_to_text(value).lower() for value in series.dropna().head(25)]
    if not samples:
        return 0.0
    hits = sum(
        1
        for sample in samples
        if re.search(r"\bkelas\s*[a-e]\b", sample) or "telat" in sample or "bucket" in sample
    )
    return hits / len(samples)


def _score_period(series):
    if pd.api.types.is_datetime64_any_dtype(series):
        return 0.95
    samples = [_value_to_text(value).lower() for value in series.dropna().head(25)]
    if not samples:
        return 0.0
    hits = 0
    for sample in samples:
        if any(token in sample for token in MONTH_TOKENS):
            hits += 1
            continue
        if re.search(r"\bq[1-4]\b", sample):
            hits += 1
            continue
        if re.search(r"\b20\d{2}\b", sample) and any(char in sample for char in ("-", "/", " ")):
            hits += 1
            continue
        if re.search(r"\d{1,2}\s*-\s*\d{1,2}", sample):
            hits += 1
    return hits / len(samples)


def _score_delay_note(series):
    samples = [_value_to_text(value).strip() for value in series.dropna().head(25)]
    if not samples:
        return 0.0
    long_text_hits = 0
    keyword_hits = 0
    for sample in samples:
        lowered = sample.lower()
        if len(sample.split()) >= 4:
            long_text_hits += 1
        if any(hint in lowered for hint in DELAY_HINTS):
            keyword_hits += 1
    return min(1.0, (long_text_hits / len(samples)) * 0.5 + (keyword_hits / len(samples)) * 0.7)


def _score_partner_type(series):
    samples = [_value_to_text(value).strip() for value in series.dropna().head(25)]
    if not samples:
        return 0.0
    unique_ratio = _safe_unique_ratio(series)
    keyword_hits = sum(1 for sample in samples if any(hint in sample.lower() for hint in PARTNER_HINTS))
    keyword_score = keyword_hits / len(samples)
    range_score = 0.0
    if 0.05 <= unique_ratio <= 0.85:
        range_score = 0.3
    return min(1.0, keyword_score * 0.75 + range_score)


def _score_service(series):
    samples = [_value_to_text(value).strip() for value in series.dropna().head(25)]
    if not samples:
        return 0.0
    unique_ratio = _safe_unique_ratio(series)
    average_words = sum(len(sample.split()) for sample in samples) / len(samples)
    if unique_ratio < 0.1:
        return 0.0
    score = 0.25
    if 0.2 <= unique_ratio <= 1.0:
        score += 0.35
    if average_words >= 1.2:
        score += 0.15
    if not any(any(hint in sample.lower() for hint in PARTNER_HINTS) for sample in samples):
        score += 0.15
    return min(1.0, score)


def _semantic_score(column_name, series, canonical_key):
    normalized_name = _normalize_name(column_name)
    field_spec = INTERNAL_API_FIELD_SPECS[canonical_key]
    if normalized_name == _normalize_name(field_spec["label"]):
        return 1.0
    if any(normalized_name == _normalize_name(alias) for alias in field_spec["aliases"]):
        return 0.98

    name_bonus = 0.0
    if any(_normalize_name(alias) in normalized_name for alias in field_spec["aliases"]):
        name_bonus = 0.18

    if canonical_key == "invoice_value":
        return min(1.0, _score_invoice_value(series) + name_bonus)
    if canonical_key == "payment_class":
        return min(1.0, _score_payment_class(series) + name_bonus)
    if canonical_key == "period":
        return min(1.0, _score_period(series) + name_bonus)
    if canonical_key == "delay_note":
        return min(1.0, _score_delay_note(series) + name_bonus)
    if canonical_key == "partner_type":
        return min(1.0, _score_partner_type(series) + name_bonus)
    if canonical_key == "service":
        return min(1.0, _score_service(series) + name_bonus)
    return 0.0


def resolve_financial_columns(data_frame, explicit_field_map=None, enable_semantic_inference=True):
    if data_frame is None:
        return {}

    explicit_field_map = explicit_field_map or {}
    columns = list(data_frame.columns)
    normalized_columns = {_normalize_name(column): column for column in columns}
    resolved_columns = {}

    for canonical_key, configured_source in explicit_field_map.items():
        if canonical_key not in INTERNAL_API_FIELD_SPECS:
            continue
        configured_column = str(configured_source).strip()
        source_column = columns[columns.index(configured_column)] if configured_column in columns else normalized_columns.get(
            _normalize_name(configured_column)
        )
        if source_column:
            resolved_columns[canonical_key] = source_column

    for canonical_key, field_spec in INTERNAL_API_FIELD_SPECS.items():
        if canonical_key in resolved_columns:
            continue

        candidate_names = (field_spec["label"], *field_spec["aliases"])
        for candidate_name in candidate_names:
            source_column = normalized_columns.get(_normalize_name(candidate_name))
            if source_column:
                resolved_columns[canonical_key] = source_column
                break

    if not enable_semantic_inference:
        return resolved_columns

    assignments = []
    used_columns = set(resolved_columns.values())
    for canonical_key in INTERNAL_API_FIELD_SPECS:
        if canonical_key in resolved_columns:
            continue
        for column in columns:
            if column in used_columns:
                continue
            score = _semantic_score(column, data_frame[column], canonical_key)
            if score >= 0.55:
                assignments.append((score, canonical_key, column))

    for score, canonical_key, column in sorted(assignments, reverse=True):
        if canonical_key in resolved_columns or column in used_columns:
            continue
        resolved_columns[canonical_key] = column
        used_columns.add(column)

    return resolved_columns


def normalize_financial_dataframe(data_frame, explicit_field_map=None):
    if data_frame is None:
        return None, build_internal_data_summary(None, explicit_field_map=explicit_field_map)

    working_frame = data_frame.copy()
    resolved_columns = resolve_financial_columns(working_frame, explicit_field_map=explicit_field_map)
    rename_map = {}

    for canonical_key, source_column in resolved_columns.items():
        target_label = INTERNAL_API_FIELD_SPECS[canonical_key]["label"]
        if source_column == target_label:
            continue
        if target_label in working_frame.columns:
            continue
        rename_map[source_column] = target_label

    if rename_map:
        working_frame = working_frame.rename(columns=rename_map)

    return working_frame, build_internal_data_summary(working_frame, explicit_field_map=explicit_field_map)


def _score_record_candidate(candidate_records, path):
    frame = pd.json_normalize(candidate_records, sep="_")
    if frame.empty:
        return -1, {}

    frame.columns = [str(column).strip() for column in frame.columns]
    resolved = resolve_financial_columns(frame)
    required_hits = sum(
        1 for canonical_key, field_spec in INTERNAL_API_FIELD_SPECS.items() if field_spec["required"] and canonical_key in resolved
    )
    total_hits = len(resolved)
    row_bonus = min(len(candidate_records), 25)
    path_bonus = 8 if any(token in path.lower() for token in INTERNAL_API_ENVELOPE_CANDIDATES) else 0
    score = required_hits * 100 + total_hits * 25 + row_bonus + path_bonus
    return score, {
        "recordCount": len(candidate_records),
        "resolvedColumns": resolved,
        "availableColumns": list(frame.columns),
        "requiredHits": required_hits,
        "totalHits": total_hits,
    }


def extract_records_from_payload(payload, explicit_records_path=None):
    if explicit_records_path:
        extracted = extract_path_value(payload, explicit_records_path)
        records = _coerce_node_to_records(extracted)
        if not records:
            raise ValueError("Configured INTERNAL_API_RECORDS_KEY does not resolve to an object or list of objects.")
        _, candidate_summary = _score_record_candidate(records, explicit_records_path)
        return records, {
            "strategy": "configured_path",
            "resolvedRecordsPath": explicit_records_path,
            "candidateCount": 1,
            **candidate_summary,
        }

    best_candidate = None
    candidate_count = 0
    for candidate_path, candidate_records in _iter_payload_candidates(payload):
        candidate_count += 1
        score, candidate_summary = _score_record_candidate(candidate_records, candidate_path)
        if best_candidate is None or score > best_candidate["score"]:
            best_candidate = {
                "score": score,
                "records": candidate_records,
                "summary": {
                    "strategy": "auto_detected",
                    "resolvedRecordsPath": candidate_path,
                    "candidateCount": candidate_count,
                    **candidate_summary,
                },
            }

    if best_candidate is None:
        raise ValueError("Internal API response does not contain an object or list of objects that can be adapted.")

    best_candidate["summary"]["candidateCount"] = candidate_count
    return best_candidate["records"], best_candidate["summary"]


def build_internal_data_summary(data_frame, explicit_field_map=None, extraction_summary=None):
    explicit_field_map = explicit_field_map or {}
    extraction_summary = extraction_summary or {}
    if data_frame is None:
        resolved_columns = {}
        available_columns = []
    else:
        resolved_columns = resolve_financial_columns(data_frame, explicit_field_map=explicit_field_map)
        available_columns = list(data_frame.columns)

    fields = []
    missing_required = []
    for canonical_key, field_spec in INTERNAL_API_FIELD_SPECS.items():
        mapped_source = resolved_columns.get(canonical_key)
        field_entry = {
            "key": canonical_key,
            "label": field_spec["label"],
            "required": field_spec["required"],
            "description": field_spec["description"],
            "aliases": list(field_spec["aliases"]),
            "example": field_spec["example"],
            "mappedSource": mapped_source,
            "resolvedLabel": field_spec["label"] if mapped_source else None,
        }
        fields.append(field_entry)
        if field_spec["required"] and not mapped_source:
            missing_required.append(canonical_key)

    return {
        "availableColumns": available_columns,
        "missingRequiredFields": missing_required,
        "resolvedColumns": {
            canonical_key: INTERNAL_API_FIELD_SPECS[canonical_key]["label"]
            for canonical_key in resolved_columns
        },
        "sourceColumns": resolved_columns,
        "fields": fields,
        "recordsPath": extraction_summary.get("resolvedRecordsPath"),
        "recordsPathStrategy": extraction_summary.get("strategy"),
        "recordCount": extraction_summary.get("recordCount"),
        "candidateRecordSetsScanned": extraction_summary.get("candidateCount"),
        "isReady": not missing_required,
    }


def get_internal_api_contract():
    fields = []
    example_record = {}
    field_map_template = {}
    for canonical_key, field_spec in INTERNAL_API_FIELD_SPECS.items():
        field_payload = deepcopy(field_spec)
        field_payload["key"] = canonical_key
        fields.append(field_payload)
        example_record[canonical_key] = field_spec["example"]
        field_map_template[canonical_key] = f"your_{canonical_key}_field"

    return {
        "endpointUrlEnvVar": "INTERNAL_API_ENDPOINT_URL",
        "recordsEnvelopeCandidates": list(INTERNAL_API_ENVELOPE_CANDIDATES),
        "recordsKeyEnvVar": "INTERNAL_API_RECORDS_KEY",
        "fieldMapEnvVar": "INTERNAL_API_FIELD_MAP_JSON",
        "fieldMapTemplate": field_map_template,
        "fields": fields,
        "exampleResponse": {
            "records": [example_record],
        },
        "notes": [
            "Cara paling mudah: arahkan app ke endpoint JSON apa pun melalui INTERNAL_API_ENDPOINT_URL.",
            "App akan mencoba mendeteksi sendiri path array/object record yang paling relevan dari JSON response.",
            "App juga akan mencoba menebak field penting berdasarkan nama kolom dan pola nilainya, lalu memakai INTERNAL_API_FIELD_MAP_JSON hanya bila inference perlu dibantu.",
            "Jika payload bersarang dan auto-detect salah, isi INTERNAL_API_RECORDS_KEY dengan path seperti `payload.data.items` atau `payload.data[0].rows`.",
            "Nilai invoice harus sudah dalam Rupiah/IDR.",
        ],
    }
