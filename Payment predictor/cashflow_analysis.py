import copy
import json
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
)
from data_contract import (
    build_internal_data_summary,
    get_internal_api_contract,
    normalize_financial_dataframe,
)
from data_sources import (
    load_available_source_profiles,
    resolve_active_source_profile,
    summarize_source_profile,
    write_active_source_key,
)
from finance_api_clients import InternalAPIClient
from financial_analyzer import FinancialAnalyzer

logger = logging.getLogger(__name__)

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
        self.chroma = chromadb.Client(Settings(anonymized_telemetry=False))
        self.embed_fn = embedding_functions.OllamaEmbeddingFunction(
            url=f"{OLLAMA_HOST}/api/embeddings",
            model_name=EMBED_MODEL,
        )
        self.collection = self.chroma.get_or_create_collection(
            name="finance_holistic_db",
            embedding_function=self.embed_fn,
        )
        self.df = None
        self.report_context_cache = None
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
        self._reload_source_registry()
        self.refresh_data()

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

        data_frame.columns = [column.strip() for column in data_frame.columns]
        return data_frame

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
        try:
            data_frame = pd.read_sql(f"SELECT * FROM {self.table_name}", self.engine)
        except Exception:
            if not os.path.exists(csv_path):
                raise FileNotFoundError(f"Demo CSV source is unavailable: {csv_path}")

            raw_df = pd.read_csv(csv_path)
            raw_df.columns = [column.strip() for column in raw_df.columns]
            data_frame = raw_df

        normalized_df, data_summary = normalize_financial_dataframe(data_frame)
        return normalized_df, data_summary

    def _load_internal_api_data(self, profile=None):
        client = InternalAPIClient(source_profile=profile or self.source_profile)
        if not client.is_configured():
            raise RuntimeError("Internal data source is not configured.")

        records, extraction_summary = client.fetch_records()

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

        existing_ids = self.collection.get().get("ids", [])
        if existing_ids:
            self.collection.delete(ids=existing_ids)

        ids = []
        documents = []
        metadatas = []

        for index, row in data_frame.iterrows():
            text_representation = " | ".join(
                f"{column}: {value}" for column, value in row.items()
            )
            ids.append(str(index))
            documents.append(text_representation)
            metadatas.append(row.astype(str).to_dict())

        if not ids:
            return False

        try:
            logger.info(
                "Syncing %s financial records to the embedding store.",
                len(ids),
            )
            self.collection.add(
                documents=documents,
                metadatas=metadatas,
                ids=ids,
            )
        except Exception as exc:
            logger.error("Embedding sync failed: %s", exc)
            return False

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

    def query(self, context_keywords="", max_results=12):
        query_text = (
            "Historical invoice delays, payment behavior class A-E, "
            "systemic financial risk, collection bottlenecks. "
            f"{context_keywords or ''}"
        )
        if self.df is None or self.df.empty:
            return "Tidak ada data finansial internal yang dapat dipakai."

        max_results = min(max_results, len(self.df))
        collection_size = self.collection.count()
        if collection_size <= 0:
            return "Tidak ada data finansial internal yang dapat dipakai."
        max_results = min(max_results, collection_size)

        try:
            result = self.collection.query(query_texts=[query_text], n_results=max_results)
            documents = result.get("documents", [])
            if documents and documents[0]:
                return "\n---\n".join(documents[0])
        except Exception as exc:
            logger.error("Query error: %s", exc)

        return "Tidak ada data finansial internal yang dapat dipakai."

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
            focused_evidence = self.query(notes, max_results=10) or context["evidence"]
            context["evidence"] = FinancialAnalyzer.normalize_evidence_text(focused_evidence)
        context.update(FinancialAnalyzer.apply_silent_assessment(context, notes))
        return context

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
