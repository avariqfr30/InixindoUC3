import copy
import hashlib
import logging
import os
import re
import threading
from datetime import datetime, timedelta

import chromadb
import pandas as pd
from chromadb.config import Settings
from chromadb.utils import embedding_functions
from sqlalchemy import create_engine

from config import (
    APP_SERVER,
    DATA_ACQUISITION_MODE,
    DATA_DIR,
    DATA_SOURCE_ACTIVE_STATE_PATH,
    DATA_SOURCE_DEMO_PROFILE_PATH,
    DATA_SOURCE_PRODUCTION_PROFILE_PATH,
    DEMO_CSV_PATH,
    EMBED_MODEL,
    INTERNAL_API_BASE_URL,
    INTERNAL_API_CONFIG_FILE,
    INTERNAL_API_DATASET_PATH,
    INTERNAL_API_ENDPOINT_URL,
    OLLAMA_HOST,
    REPORT_MAX_CONCURRENT_JOBS,
    WAITRESS_THREADS,
)
from data_contract import (
    build_internal_data_summary,
    enrich_invoice_records_with_payment_behavior,
    enrich_records_with_account_reference,
    get_internal_api_contract,
    normalize_records,
    normalize_financial_dataframe,
)
from data_sources import (
    load_available_source_profiles,
    resolve_active_source_profile,
    summarize_source_profile,
    write_active_source_key,
)
from cashflow_intelligence_desk import CashflowIntelligenceDesk
from finance_api_clients import InternalAPIClient
from financial_analyzer import FinancialAnalyzer

logger = logging.getLogger(__name__)


def _int_env(name, default):
    raw = os.getenv(name, str(default))
    try:
        return int(str(raw).strip())
    except (TypeError, ValueError):
        logger.warning("Invalid integer for %s=%r, using default %s", name, raw, default)
        return default


EMBEDDING_BATCH_SIZE = max(_int_env("EMBEDDING_BATCH_SIZE", 8), 1)
EMBEDDING_SYNC_ENABLED = os.getenv("EMBEDDING_SYNC_ENABLED", "false").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
VECTOR_INDEX_DIR = os.getenv(
    "VECTOR_INDEX_DIR",
    os.path.join(DATA_DIR, "vector_index"),
)


def _dataframe_fingerprint(data_frame):
    normalized = data_frame.fillna("").astype(str)
    digest = hashlib.sha256()
    digest.update("\x1f".join(map(str, normalized.columns)).encode("utf-8"))
    digest.update(pd.util.hash_pandas_object(normalized, index=True).values.tobytes())
    return digest.hexdigest()


def _collection_prefix(model_name):
    normalized = re.sub(r"[^a-z0-9]+", "_", str(model_name or "").lower()).strip("_")
    return f"finance_{normalized or 'embedding'}"


def _unique_text(values, *, limit, max_length):
    output = []
    seen = set()
    for value in values:
        text = str(value or "").strip()
        if not text or text.lower() in {"nan", "none", "null", "-"}:
            continue
        normalized = text.casefold()
        if normalized in seen:
            continue
        seen.add(normalized)
        output.append(text[:max_length])
        if len(output) >= limit:
            break
    return output


def _build_financial_embedding_records(data_frame):
    group_columns = [
        column
        for column in (
            "Status Pembayaran Invoice",
            "Kelas Pembayaran",
            "Layanan",
        )
        if column in data_frame.columns
    ]
    if not group_columns:
        group_columns = [data_frame.columns[0]]

    ids, documents, metadatas = [], [], []
    for position, (group_key, group) in enumerate(
        data_frame.fillna("").groupby(group_columns, dropna=False, sort=True)
    ):
        group_values = group_key if isinstance(group_key, tuple) else (group_key,)
        group_metadata = {
            column: str(value or "")
            for column, value in zip(group_columns, group_values)
        }
        invoice_values = pd.to_numeric(
            group.get("Nilai Invoice"),
            errors="coerce",
        ).dropna()
        partners = _unique_text(
            group.get("Tipe Partner", pd.Series(dtype=str)),
            limit=8,
            max_length=100,
        )
        invoice_numbers = _unique_text(
            group.get("invoice_number", pd.Series(dtype=str)),
            limit=8,
            max_length=80,
        )
        historical_notes = _unique_text(
            group.get("Catatan Historis Keterlambatan", pd.Series(dtype=str)),
            limit=3,
            max_length=180,
        )
        due_dates = _unique_text(
            group.get("Tanggal Jatuh Tempo Invoice", pd.Series(dtype=str)),
            limit=5,
            max_length=40,
        )
        fields = [
            *(f"{column}: {value}" for column, value in group_metadata.items() if value),
            f"Jumlah invoice: {len(group)}",
            f"Total nilai invoice: {invoice_values.sum():.2f}" if not invoice_values.empty else "",
            f"Rata-rata nilai invoice: {invoice_values.mean():.2f}" if not invoice_values.empty else "",
            "Contoh partner: " + ", ".join(partners) if partners else "",
            "Contoh invoice: " + ", ".join(invoice_numbers) if invoice_numbers else "",
            "Tanggal jatuh tempo representatif: " + ", ".join(due_dates) if due_dates else "",
            "Catatan historis: " + " ; ".join(historical_notes) if historical_notes else "",
        ]
        ids.append(str(position))
        documents.append(" | ".join(field for field in fields if field))
        metadatas.append(
            {
                **group_metadata,
                "record_count": int(len(group)),
                "customer": partners[0] if partners else "",
            }
        )
    return ids, documents, metadatas


class KnowledgeBase:
    def __init__(self, db_uri):
        os.makedirs(DATA_DIR, exist_ok=True)
        self.engine = create_engine(db_uri)
        self.source_registry = {}
        self.source_registry_issues = []
        self.active_source_state_path = DATA_SOURCE_ACTIVE_STATE_PATH
        self.active_source_key = "demo"
        self.source_profile = {}
        self.data_mode = "demo"
        self.table_name = "invoices_demo"
        self.internal_api_client = None
        os.makedirs(VECTOR_INDEX_DIR, exist_ok=True)
        self.chroma = chromadb.PersistentClient(
            path=VECTOR_INDEX_DIR,
            settings=Settings(anonymized_telemetry=False),
        )
        self.embed_fn = embedding_functions.OllamaEmbeddingFunction(
            url=f"{OLLAMA_HOST}/api/embeddings",
            model_name=EMBED_MODEL,
            timeout=300,
        )
        self.collection = None
        self.df = None
        self.report_context_cache = None
        self.focused_report_context_cache = {}
        self.data_contract_summary = build_internal_data_summary(None)
        self.cache_lock = threading.Lock()
        self.refresh_lock = threading.Lock()
        self.sync_status = "not_loaded"
        self.last_sync_started_at = None
        self.last_sync_at = None
        self.last_success_at = None
        self.last_sync_duration_seconds = None
        self.last_sync_error = None
        self.data_version = 0
        self.runtime_profile = {
            "app_server": APP_SERVER,
            "report_max_concurrent_jobs": REPORT_MAX_CONCURRENT_JOBS,
            "waitress_threads": WAITRESS_THREADS,
        }
        self._reload_source_registry()
        self.refresh_data()

    @staticmethod
    def _normalize_records(records):
        return normalize_records(records)

    @staticmethod
    def _build_table_name(source_key):
        normalized_key = re.sub(r"[^a-z0-9_]+", "_", str(source_key or "demo").strip().lower()).strip("_")
        return f"invoices_{normalized_key or 'demo'}"

    def _reload_source_registry(self):
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
        self.source_registry = profiles
        self.source_registry_issues = issues
        selected_key, selected_profile = resolve_active_source_profile(
            profiles=profiles,
            state_path=self.active_source_state_path,
            legacy_default_key=default_key,
        )
        self._set_active_source(selected_key, selected_profile, persist=False)

    def _set_active_source(self, source_key, source_profile, persist=True):
        self.active_source_key = source_key
        self.source_profile = copy.deepcopy(source_profile or {})
        self.data_mode = str(self.source_profile.get("mode") or "demo").strip().lower() or "demo"
        self.table_name = self._build_table_name(source_key)
        if self.source_profile.get("type") == "json_api":
            self.internal_api_client = InternalAPIClient(source_profile=self.source_profile)
        else:
            self.internal_api_client = None
        if persist:
            write_active_source_key(self.active_source_state_path, source_key)

    def _load_demo_data(self, profile=None):
        active_profile = profile or self.source_profile
        csv_path = str((active_profile or {}).get("path") or DEMO_CSV_PATH)
        data_frame = None
        try:
            data_frame = pd.read_sql(f"SELECT * FROM {self.table_name}", self.engine)
        except Exception:
            data_frame = None

        if data_frame is None or data_frame.empty:
            if not os.path.exists(csv_path):
                raise FileNotFoundError(f"Demo CSV source is unavailable: {csv_path}")
            data_frame = pd.read_csv(csv_path)
            data_frame.columns = [column.strip() for column in data_frame.columns]

        normalized_df, data_summary = normalize_financial_dataframe(data_frame)
        return normalized_df, data_summary

    def _load_internal_api_data(self, profile=None):
        client = InternalAPIClient(source_profile=profile or self.source_profile)
        if not client.is_configured():
            raise RuntimeError("Internal data source is not configured.")

        if hasattr(client, "fetch_invoice_records"):
            records, extraction_summary = client.fetch_invoice_records()
        else:
            records, extraction_summary = client.fetch_records()
        records, invoice_behavior_summary = enrich_invoice_records_with_payment_behavior(records)
        reference_enrichment_summary = None
        try:
            account_records, _ = client.fetch_reference_account_records()
            records, reference_enrichment_summary = enrich_records_with_account_reference(
                records,
                account_records,
            )
        except Exception as exc:
            logger.info("ReferenceAccount enrichment skipped: %s", exc)

        raw_data_frame = self._normalize_records(records)
        if raw_data_frame.empty:
            raise RuntimeError("Internal data source returned no records.")

        normalized_df, _ = normalize_financial_dataframe(
            raw_data_frame,
            explicit_field_map=client.field_map,
        )
        data_summary = build_internal_data_summary(
            normalized_df,
            explicit_field_map=client.field_map,
            extraction_summary=extraction_summary,
        )
        if reference_enrichment_summary:
            data_summary["referenceAccountEnrichment"] = reference_enrichment_summary
        data_summary["invoiceBehaviorEnrichment"] = invoice_behavior_summary
        data_summary["loadedInvoiceDatasets"] = extraction_summary.get("loadedDatasets", [])
        data_summary["unavailableInvoiceDatasets"] = extraction_summary.get("unavailableDatasets", [])

        if data_summary["missingRequiredFields"]:
            logger.warning(
                "Internal data source is missing required fields after normalization: %s",
                ", ".join(data_summary["missingRequiredFields"]),
            )

        return normalized_df, data_summary

    def _load_source_data(self, profile=None):
        active_profile = profile or self.source_profile
        source_type = str((active_profile or {}).get("type") or "demo_csv").strip().lower()
        if source_type == "json_api":
            return self._load_internal_api_data(profile=active_profile)
        return self._load_demo_data(profile=active_profile)

    def _rebuild_embeddings(self, data_frame):
        if data_frame is None or data_frame.empty:
            return False
        if not EMBEDDING_SYNC_ENABLED:
            logger.info("Embedding sync skipped; financial tables remain the source of truth for startup.")
            return True

        ids, documents, metadatas = _build_financial_embedding_records(data_frame)

        if not ids:
            return False

        prefix = _collection_prefix(EMBED_MODEL)
        fingerprint = _dataframe_fingerprint(data_frame)
        collection_name = f"{prefix}_{fingerprint[:12]}"
        previous_collection = self.collection
        candidate_collection = self.chroma.get_or_create_collection(
            name=collection_name,
            embedding_function=self.embed_fn,
            metadata={
                "model": EMBED_MODEL,
                "source_fingerprint": fingerprint,
            },
        )
        if candidate_collection.count() == len(ids):
            self.collection = candidate_collection
            logger.info(
                "Reusing financial vector index %s with %s records.",
                collection_name,
                len(ids),
            )
            return True

        existing_ids = candidate_collection.get().get("ids", [])
        if existing_ids:
            candidate_collection.delete(ids=existing_ids)

        try:
            logger.info(
                "Indexing %s financial records with %s in batches of %s.",
                len(ids),
                EMBED_MODEL,
                EMBEDDING_BATCH_SIZE,
            )
            for start in range(0, len(ids), EMBEDDING_BATCH_SIZE):
                end = start + EMBEDDING_BATCH_SIZE
                candidate_collection.add(
                    documents=documents[start:end],
                    metadatas=metadatas[start:end],
                    ids=ids[start:end],
                )
            if candidate_collection.count() != len(ids):
                raise RuntimeError("vector index count does not match financial record count")
        except Exception as exc:
            logger.error("Embedding sync failed: %s", exc)
            self.collection = previous_collection
            try:
                self.chroma.delete_collection(collection_name)
            except Exception:
                logger.warning("Failed to remove incomplete vector collection %s.", collection_name)
            return False

        self.collection = candidate_collection
        for existing in self.chroma.list_collections():
            existing_name = getattr(existing, "name", str(existing))
            if existing_name.startswith(f"{prefix}_") and existing_name != collection_name:
                self.chroma.delete_collection(existing_name)
        return True

    def refresh_data(self):
        with self.refresh_lock:
            started_at = datetime.now()
            self.sync_status = "refreshing"
            self.last_sync_started_at = started_at
            self.last_sync_error = None

            try:
                loaded_df, loaded_summary = self._load_source_data()
            except Exception as exc:
                completed_at = datetime.now()
                self.sync_status = "error"
                self.last_sync_at = completed_at
                self.last_sync_duration_seconds = round((completed_at - started_at).total_seconds(), 2)
                self.last_sync_error = str(exc)
                return False

            rebuilt = self._rebuild_embeddings(loaded_df)
            completed_at = datetime.now()
            self.df = loaded_df
            self.data_contract_summary = loaded_summary
            self.df.to_sql(self.table_name, self.engine, index=False, if_exists="replace")
            with self.cache_lock:
                self.report_context_cache = None
                self.focused_report_context_cache = {}
            self.sync_status = "ready" if rebuilt else "degraded"
            self.last_sync_at = completed_at
            self.last_success_at = completed_at
            self.last_sync_duration_seconds = round((completed_at - started_at).total_seconds(), 2)
            self.last_sync_error = None if rebuilt else "Embedding store gagal diperbarui. Dashboard tetap memakai data finansial terbaru."
            self.data_version += 1
            return True

    def validate_source(self, source_key):
        self._reload_source_registry()
        profile = self.source_registry.get(source_key)
        if not profile:
            raise ValueError(f"Sumber data `{source_key}` tidak tersedia.")

        summary = summarize_source_profile(profile)
        validation = {
            "source": summary,
            "ready": False,
            "message": "",
            "recordCount": None,
            "missingRequiredFields": [],
            "contractSummary": None,
            "nextSteps": [],
        }

        try:
            data_frame, data_summary = self._load_source_data(profile=profile)
        except Exception as exc:
            validation["message"] = str(exc)
            return validation

        validation["ready"] = bool(data_summary.get("isReady"))
        validation["message"] = "Sumber data valid dan siap diaktifkan." if validation["ready"] else "Sumber data terbaca, tetapi field wajib belum lengkap."
        validation["recordCount"] = int(len(data_frame))
        validation["missingRequiredFields"] = list(data_summary.get("missingRequiredFields") or [])
        validation["contractSummary"] = data_summary
        validation["nextSteps"] = self._build_source_validation_next_steps(data_summary)
        return validation

    @staticmethod
    def _build_source_validation_next_steps(data_summary):
        steps = []
        if not data_summary.get("recordsPath"):
            steps.append("Isi endpoint.records_key jika array record utama belum terdeteksi dengan benar.")
        if data_summary.get("missingRequiredFields"):
            missing = ", ".join(data_summary.get("missingRequiredFields") or [])
            steps.append(f"Lengkapi field_map untuk field wajib yang belum terbaca: {missing}.")
        if data_summary.get("lowConfidenceFields"):
            low_confidence = ", ".join(data_summary.get("lowConfidenceFields") or [])
            steps.append(f"Review mapping otomatis untuk field ber-confidence rendah: {low_confidence}.")
        if data_summary.get("fieldMapSuggestionJson"):
            steps.append("Gunakan fieldMapSuggestionJson sebagai draft field_map bila perlu mapping eksplisit.")
        if not steps:
            steps.append("Sumber data siap diaktifkan sebagai production knowledge base.")
        return steps

    def activate_source(self, source_key):
        validation = self.validate_source(source_key)
        if not validation["ready"]:
            return {**validation, "activated": False}

        profile = self.source_registry[source_key]
        previous_key = self.active_source_key
        previous_profile = copy.deepcopy(self.source_profile)
        self._set_active_source(source_key, profile, persist=False)
        if self.refresh_data():
            write_active_source_key(self.active_source_state_path, source_key)
            return {**validation, "activated": True, "activeSourceKey": source_key}

        error_message = self.last_sync_error or "Aktivasi gagal."
        self._set_active_source(previous_key, previous_profile, persist=False)
        self.refresh_data()
        return {
            **validation,
            "activated": False,
            "message": error_message,
            "activeSourceKey": previous_key,
        }

    @staticmethod
    def _rerank_documents(query_text, documents, metadatas=None, distances=None, limit=12, enabled=None):
        if enabled is None:
            enabled = os.getenv("EVIDENCE_QUALITY_ENABLED", "true").strip().lower() in {"1", "true", "yes", "on"}
        if not enabled:
            return list(documents[:limit])
        try:
            from evidence_quality import rerank
            candidates = []
            for index, document in enumerate(documents):
                metadata = metadatas[index] if metadatas and index < len(metadatas) else {}
                distance = distances[index] if distances and index < len(distances) else 1.0
                try:
                    semantic_score = 1.0 / (1.0 + max(0.0, float(distance)))
                except (TypeError, ValueError):
                    semantic_score = 0.0
                normalized_meta = dict(metadata or {})
                for target, aliases in {
                    "customer": ("customer", "customer_name", "company_name", "client"),
                    "invoice": ("invoice", "invoice_number", "invoice_no"),
                    "status": ("status", "payment_status", "invoice_status"),
                    "age_days": ("age_days", "days_overdue", "overdue_days"),
                }.items():
                    if not normalized_meta.get(target):
                        normalized_meta[target] = next((normalized_meta.get(alias) for alias in aliases if normalized_meta.get(alias)), "")
                candidates.append({"id": str(index), "text": str(document or ""), "semantic_score": semantic_score, "metadata": normalized_meta})
            retrieval_intent = {
                "goal": "find invoice, payment status, aging, partner, and cashflow evidence",
                "preferred_datasets": ["InvoiceTraining", "InvoiceConsultant", "ReferenceAccount"],
                "exclude": ["FinanceInvoice", "ProjectStandards"],
                "preferred_terms": [query_text],
            }
            ranked = rerank("payment", query_text, candidates, limit=limit, retrieval_intent=retrieval_intent)
            ordered = [str(item.get("text") or "") for item in ranked if isinstance(item, dict)]
            return ordered or list(documents[:limit])
        except Exception:
            return list(documents[:limit])

    def query(self, context_keywords="", max_results=12):
        base_query = (
            "Keterlambatan invoice historis, perilaku pembayaran kelas A-E, "
            "risiko keuangan sistemik, dan hambatan penagihan. "
            f"{context_keywords or ''}"
        )
        query_text = (
            "Instruct: Temukan bukti invoice dan pembayaran internal yang paling relevan "
            "untuk analisis arus kas.\n"
            f"Query: {base_query}"
        )
        if self.df is None or self.df.empty:
            return "Tidak ada data finansial internal yang dapat dipakai."

        max_results = min(max_results, len(self.df))
        collection_size = self.collection.count() if self.collection is not None else 0
        if collection_size <= 0:
            return ""
        output_limit = min(max_results, collection_size)
        candidate_limit = min(max(output_limit * 3, output_limit), collection_size)

        try:
            result = self.collection.query(
                query_texts=[query_text],
                n_results=candidate_limit,
                include=["documents", "metadatas", "distances"],
            )
            documents = result.get("documents", [])
            if documents and documents[0]:
                metadatas = (result.get("metadatas") or [[]])[0]
                distances = (result.get("distances") or [[]])[0]
                ranked = self._rerank_documents(query_text, documents[0], metadatas, distances, limit=output_limit)
                return "\n---\n".join(ranked)
        except Exception as exc:
            logger.error("Query error: %s", exc)

        return ""

    def get_report_context(self, notes=""):
        with self.cache_lock:
            if self.report_context_cache is None:
                self.report_context_cache = FinancialAnalyzer.build_report_context(
                    self.df,
                    data_mode=self.data_mode,
                )

        context = copy.deepcopy(self.report_context_cache)
        notes = (notes or "").strip()
        if notes:
            focused_cache_key = self._focused_report_context_cache_key(notes)
            with self.cache_lock:
                cached_context = self.focused_report_context_cache.get(focused_cache_key)
            if cached_context is not None:
                return copy.deepcopy(cached_context)
            focused_evidence = self.query(notes, max_results=10) or context["evidence"]
            context["evidence"] = FinancialAnalyzer.normalize_evidence_text(focused_evidence)
        context.update(FinancialAnalyzer.apply_silent_assessment(context, notes, runtime_profile=self.runtime_profile))
        if notes:
            context = CashflowIntelligenceDesk.apply_notes_context(context, notes)
        if notes:
            with self.cache_lock:
                self.focused_report_context_cache[focused_cache_key] = copy.deepcopy(context)
        return context

    def _focused_report_context_cache_key(self, notes):
        digest = hashlib.sha256(str(notes or "").strip().encode("utf-8")).hexdigest()
        return (self.data_version, self.active_source_key, digest)

    def prefetch_report_context(self, notes=""):
        context = self.get_report_context(notes)
        return {
            "status": "ready",
            "dataVersion": self.data_version,
            "activeSourceKey": self.active_source_key,
            "focusedNotes": bool(str(notes or "").strip()),
            "evidenceLength": len(str(context.get("evidence") or "")),
            "reviewContextReady": bool(context.get("review_context")),
        }

    def get_review_context(self):
        report_context = self.get_report_context("")
        return report_context.get("review_context", {})

    def get_internal_data_contract(self):
        contract = get_internal_api_contract()
        contract["currentSummary"] = self.data_contract_summary
        contract["dataMode"] = self.data_mode
        contract["internalApiConfigured"] = bool(self.internal_api_client and self.internal_api_client.is_configured())
        contract["datasetUrl"] = self.internal_api_client.get_dataset_url() if self.internal_api_client and self.internal_api_client.is_configured() else None
        contract["activeSourceKey"] = self.active_source_key
        contract["activeSource"] = summarize_source_profile(self.source_profile)
        contract["availableSources"] = [
            summarize_source_profile(profile)
            for _, profile in sorted(self.source_registry.items(), key=lambda item: item[0])
        ]
        contract["registryIssues"] = list(self.source_registry_issues)
        return contract

    def get_sync_status(self, refresh_interval_seconds=0):
        next_refresh_at = None
        if refresh_interval_seconds > 0 and self.last_success_at is not None:
            next_refresh_at = self.last_success_at + timedelta(seconds=refresh_interval_seconds)

        source_age_minutes = None
        if self.last_success_at is not None:
            source_age_minutes = round((datetime.now() - self.last_success_at).total_seconds() / 60, 1)

        return {
            "dataMode": self.data_mode,
            "syncStatus": self.sync_status,
            "lastSyncStartedAt": self.last_sync_started_at.isoformat() if self.last_sync_started_at else None,
            "lastSyncAt": self.last_sync_at.isoformat() if self.last_sync_at else None,
            "lastSuccessAt": self.last_success_at.isoformat() if self.last_success_at else None,
            "lastSyncDurationSeconds": self.last_sync_duration_seconds,
            "lastSyncError": self.last_sync_error,
            "sourceAgeMinutes": source_age_minutes,
            "nextRefreshAt": next_refresh_at.isoformat() if next_refresh_at else None,
            "recordCount": 0 if self.df is None else int(len(self.df)),
            "dataVersion": self.data_version,
            "contractReady": bool(self.data_contract_summary.get("isReady")),
            "activeSourceKey": self.active_source_key,
            "activeSource": summarize_source_profile(self.source_profile),
            "availableSources": [
                summarize_source_profile(profile)
                for _, profile in sorted(self.source_registry.items(), key=lambda item: item[0])
            ],
            "sourceRegistryIssues": list(self.source_registry_issues),
        }
