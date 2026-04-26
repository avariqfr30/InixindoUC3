import copy
import json
import logging
import os
import re
import statistics
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
from forecast_engine import parse_idr_amount

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

class FinancialAnalyzer:
    CLASS_SCORE_MAP = {
        "kelas a": 1,
        "kelas b": 2,
        "kelas c": 3,
        "kelas d": 4,
        "kelas e": 5,
    }
    PAYMENT_CLASS_ORDER = {
        "Kelas A": 1,
        "Kelas B": 2,
        "Kelas C": 3,
        "Kelas D": 4,
        "Kelas E": 5,
        "Tidak Diketahui": 6,
    }
    REALIZATION_RATE_MAP = {
        "Kelas A": 0.98,
        "Kelas B": 0.90,
        "Kelas C": 0.72,
        "Kelas D": 0.45,
        "Kelas E": 0.18,
        "Tidak Diketahui": 0.65,
    }
    UPSIDE_RATE_MAP = {
        "Kelas A": 1.00,
        "Kelas B": 0.95,
        "Kelas C": 0.82,
        "Kelas D": 0.60,
        "Kelas E": 0.28,
        "Tidak Diketahui": 0.72,
    }
    DOWNSIDE_RATE_MAP = {
        "Kelas A": 0.95,
        "Kelas B": 0.82,
        "Kelas C": 0.60,
        "Kelas D": 0.30,
        "Kelas E": 0.05,
        "Tidak Diketahui": 0.50,
    }
    CONFIDENCE_LABELS = {
        1: "rendah",
        2: "rendah",
        3: "menengah",
        4: "tinggi",
        5: "tinggi",
    }
    OWNERSHIP_KEYWORDS = (
        "owner",
        "pic",
        "account manager",
        "finance manager",
        "finance",
        "collection",
        "cfo",
        "direktur",
        "direksi",
        "sponsor",
        "project lead",
        "project manager",
        "kepala",
        "manajer",
        "tim",
        "business translator",
        "ai engineer",
        "it",
    )
    ADOPTION_KEYWORDS = (
        "pilot",
        "uat",
        "testing",
        "rollout",
        "implementasi",
        "adopsi",
        "pelatihan",
        "sosialisasi",
        "workshop",
        "change management",
        "manajemen",
        "governance",
        "kontrol",
        "roadmap",
    )
    CAUTION_KEYWORDS = (
        "asumsi",
        "batasan",
        "risiko",
        "kontrol",
        "governance",
        "pilot",
        "owner",
        "pic",
        "siap",
        "implementasi",
    )

    DELAY_THEME_KEYWORDS = {
        "Siklus anggaran": ("dipa", "anggaran", "apbn", "apbd", "pencairan", "budget"),
        "Persetujuan internal klien": ("direksi", "approval", "persetujuan", "otorisasi", "tanda tangan"),
        "Dokumen dan administrasi": ("bast", "dokumen", "administrasi", "berita acara", "po", "invoice revisi"),
        "Likuiditas pelanggan": ("cashflow", "likuiditas", "arus kas", "pending dana", "menunggu dana"),
        "Sengketa atau klarifikasi": ("dispute", "klarifikasi", "komplain", "revisi", "sengketa"),
    }
    THEME_ACTION_MAP = {
        "Siklus anggaran": {
            "action": "Validasi posisi anggaran dan jadwal pencairan dengan PIC klien, lalu siapkan eskalasi sebelum cut-off termin.",
            "impact": "Memperjelas kapan invoice realistis dapat direalisasikan dan menurunkan slip karena siklus anggaran.",
            "owner": "Finance Collection + Account Manager",
        },
        "Persetujuan internal klien": {
            "action": "Kunci jalur approval, percepat BAST/BA final, dan pastikan sponsor internal klien ikut mendorong sign-off.",
            "impact": "Memangkas waktu tunggu approval yang selama ini menahan pelepasan pembayaran.",
            "owner": "Account Manager + Project Lead",
        },
        "Dokumen dan administrasi": {
            "action": "Selesaikan kelengkapan dokumen, revisi invoice/PO, dan lakukan quality check sebelum follow-up penagihan berikutnya.",
            "impact": "Mengurangi alasan administratif yang membuat invoice technically ready tetapi belum bisa dibayar.",
            "owner": "Project Admin + Finance Collection",
        },
        "Likuiditas pelanggan": {
            "action": "Negosiasikan skema termin, minta komitmen tanggal bayar tertulis, dan siapkan eskalasi manajemen untuk akun rentan.",
            "impact": "Meningkatkan peluang pemulihan arus kas masuk pada akun yang menghadapi tekanan likuiditas.",
            "owner": "Finance Manager + Account Manager",
        },
        "Sengketa atau klarifikasi": {
            "action": "Tutup ruang lingkup yang disengketakan, dokumentasikan keputusan bersama, dan tahan ekspansi pekerjaan sampai dispute selesai.",
            "impact": "Membuka blokir pembayaran yang tertahan karena ketidakjelasan ruang lingkup atau deliverable.",
            "owner": "Project Lead + Finance Manager",
        },
        "Follow-up umum": {
            "action": "Lakukan follow-up berbasis bukti dengan PIC AP/finance dan pastikan komitmen bayar terbaru tercatat.",
            "impact": "Menjaga ritme collection pada akun yang belum menunjukkan isu dominan tertentu.",
            "owner": "Finance Collection",
        },
    }

    COLUMN_ALIASES = {
        "period": ("periode laporan", "period", "report period", "invoice period"),
        "partner": ("tipe partner", "partner type", "partner_type", "customer segment", "segment"),
        "service": ("layanan", "service", "product", "offering"),
        "payment_class": ("kelas pembayaran", "payment class", "payment_class", "collection class"),
        "invoice_value": ("nilai invoice", "invoice value", "invoice_amount", "amount", "outstanding"),
        "notes": (
            "catatan historis keterlambatan",
            "collection note",
            "collection notes",
            "notes",
            "delay note",
        ),
    }

    @staticmethod
    def _normalize_column_name(column_name):
        return re.sub(r"[^a-z0-9]+", " ", str(column_name).strip().lower()).strip()

    @classmethod
    def _find_column(cls, df, alias_key):
        normalized_map = {
            cls._normalize_column_name(column): column
            for column in df.columns
        }
        for candidate in cls.COLUMN_ALIASES[alias_key]:
            column = normalized_map.get(candidate)
            if column:
                return column
        return None

    @staticmethod
    def _parse_currency(value):
        return parse_idr_amount(value)

    @classmethod
    def _detect_payment_class(cls, raw_value):
        text = str(raw_value or "").lower()
        for class_name, score in cls.CLASS_SCORE_MAP.items():
            if class_name in text:
                return class_name.upper().replace("KELAS", "Kelas"), score
        return "Tidak Diketahui", 3

    @staticmethod
    def _format_currency(amount):
        return f"Rp {amount:,.0f}".replace(",", ".")

    @staticmethod
    def _format_percentage(value):
        return f"{value:.1f}%"

    @staticmethod
    def _normalize_report_text_fragment(text):
        normalized = re.sub(r"\s+", " ", str(text or "")).strip()
        normalized = normalized.replace("…", " ")
        normalized = re.sub(r"\.{3,}", " ", normalized)
        normalized = re.sub(r"\s+([,.;:!?])", r"\1", normalized)
        normalized = re.sub(r"[\"'`]+$", "", normalized).strip(" -–—")
        return normalized.strip()

    @classmethod
    def _trim_note_for_report(cls, note, max_length=220):
        normalized = cls._normalize_report_text_fragment(note)
        if not normalized:
            return ""
        if len(normalized) <= max_length:
            return normalized

        candidate = normalized[: max_length + 1]
        sentence_breaks = [candidate.rfind(marker) for marker in (".", "!", "?", ";", ":")]
        best_sentence_break = max(sentence_breaks)
        if best_sentence_break >= int(max_length * 0.55):
            candidate = candidate[: best_sentence_break + 1]
        else:
            last_space = candidate.rfind(" ")
            if last_space > int(max_length * 0.55):
                candidate = candidate[:last_space]
            else:
                candidate = candidate[:max_length]

        return candidate.strip(" ,;:-")

    @staticmethod
    def _parse_period_sort_key(period_label):
        text = str(period_label or "").strip().lower()
        match = re.search(r"q([1-4])\s*(20\d{2})", text)
        if match:
            return int(match.group(2)), int(match.group(1))
        year_match = re.search(r"(20\d{2})", text)
        if year_match:
            return int(year_match.group(1)), 0
        return 0, 0

    @classmethod
    def _extract_delay_themes(cls, notes_series):
        counts = {theme: 0 for theme in cls.DELAY_THEME_KEYWORDS}
        for note in notes_series.dropna().astype(str):
            lowered_note = note.lower()
            for theme, keywords in cls.DELAY_THEME_KEYWORDS.items():
                if any(keyword in lowered_note for keyword in keywords):
                    counts[theme] += 1

        ranked = [(theme, count) for theme, count in counts.items() if count > 0]
        ranked.sort(key=lambda item: item[1], reverse=True)
        return ranked[:4]

    @classmethod
    def _detect_note_themes(cls, note):
        matched_themes = []
        lowered_note = str(note or "").lower()
        for theme, keywords in cls.DELAY_THEME_KEYWORDS.items():
            if any(keyword in lowered_note for keyword in keywords):
                matched_themes.append(theme)
        return matched_themes

    @classmethod
    def _get_action_plan(cls, note):
        themes = cls._detect_note_themes(note)
        selected_theme = themes[0] if themes else "Follow-up umum"
        action_plan = cls.THEME_ACTION_MAP.get(selected_theme, cls.THEME_ACTION_MAP["Follow-up umum"])
        return selected_theme, action_plan

    @staticmethod
    def _format_evidence_chunk(chunk):
        normalized = re.sub(r"\s+", " ", str(chunk or "")).strip()
        if not normalized:
            return ""

        pattern = re.compile(
            r"Periode Laporan:\s*(?P<period>.*?)\s*\|\s*"
            r"Tipe Partner:\s*(?P<partner>.*?)\s*\|\s*"
            r"Layanan:\s*(?P<service>.*?)\s*\|\s*"
            r"Kelas Pembayaran:\s*(?P<payment_class>.*?)\s*\|\s*"
            r"Nilai Invoice:\s*(?P<invoice_value>.*?)\s*\|\s*"
            r"Catatan Historis Keterlambatan:\s*(?P<note>.*)$"
        )
        match = pattern.match(normalized)
        if not match:
            return f"- {normalized}"

        parts = match.groupdict()
        return "\n".join(
            [
                f"- {parts['period']} | {parts['partner']} | {parts['service']}",
                f"  - Kelas pembayaran: {parts['payment_class']}",
                f"  - Nilai invoice: {parts['invoice_value']}",
                f"  - Catatan utama: {parts['note']}",
            ]
        )

    @staticmethod
    def normalize_evidence_text(raw_text):
        lines = []
        chunks = [chunk.strip() for chunk in str(raw_text or "").split("\n---\n") if chunk.strip()]

        if len(chunks) > 1 or any("Periode Laporan:" in chunk for chunk in chunks):
            for chunk in chunks[:10]:
                lines.append(FinancialAnalyzer._format_evidence_chunk(chunk))
            return "\n".join(lines) if lines else "- Tidak ada catatan historis yang cukup untuk dikutip."

        for raw_line in str(raw_text or "").splitlines():
            cleaned_line = raw_line.strip()
            if not cleaned_line or cleaned_line == "---":
                continue
            if cleaned_line.startswith("- ") or re.match(r"^\d+\.", cleaned_line):
                lines.append(cleaned_line)
            else:
                lines.append(f"- {cleaned_line}")

        return "\n".join(lines) if lines else "- Tidak ada catatan historis yang cukup untuk dikutip."

    @staticmethod
    def _clamp_score(score):
        return max(1, min(5, int(round(score))))

    @classmethod
    def _score_to_confidence(cls, score):
        return cls.CONFIDENCE_LABELS.get(cls._clamp_score(score), "menengah")

    @staticmethod
    def _format_list_as_sentence(items):
        filtered_items = [str(item).strip() for item in items if str(item).strip()]
        if not filtered_items:
            return "-"
        if len(filtered_items) == 1:
            return filtered_items[0]
        if len(filtered_items) == 2:
            return f"{filtered_items[0]} dan {filtered_items[1]}"
        return f"{', '.join(filtered_items[:-1])}, dan {filtered_items[-1]}"

    @classmethod
    def _count_keyword_hits(cls, raw_text, keywords):
        lowered_text = str(raw_text or "").lower()
        return sum(1 for keyword in keywords if keyword in lowered_text)

    @classmethod
    def _build_hidden_dimensions(cls, base_profile, notes):
        notes = (notes or "").strip()
        missing_core_fields = len(base_profile.get("missing_core_fields", []))
        total_invoices = base_profile.get("total_invoices", 0)
        data_mode = base_profile.get("data_mode", "demo")
        owner_hits = cls._count_keyword_hits(notes, cls.OWNERSHIP_KEYWORDS)
        adoption_hits = cls._count_keyword_hits(notes, cls.ADOPTION_KEYWORDS)

        business_value_score = 4 if total_invoices > 0 and base_profile.get("high_risk_invoices", 0) > 0 else 3
        if notes:
            business_value_score += 1

        data_model_score = 4 if missing_core_fields <= 1 and total_invoices >= 25 else 3
        if data_mode == "demo":
            data_model_score -= 1

        infrastructure_score = 4 if APP_SERVER == "waitress" else 2
        if REPORT_MAX_CONCURRENT_JOBS >= 4:
            infrastructure_score += 1
        if APP_SERVER == "waitress" and WAITRESS_THREADS < 8:
            infrastructure_score -= 1

        people_score = 2
        if owner_hits >= 3:
            people_score = 4
        elif owner_hits >= 1:
            people_score = 3

        governance_score = 4
        if data_mode == "demo":
            governance_score -= 1
        if missing_core_fields >= 2:
            governance_score -= 1

        adoption_score = 2
        if adoption_hits >= 3:
            adoption_score = 4
        elif adoption_hits >= 1 or owner_hits >= 2:
            adoption_score = 3

        dimensions = {
            "business_value_clarity": cls._clamp_score(business_value_score),
            "data_model_readiness": cls._clamp_score(data_model_score),
            "infrastructure_readiness": cls._clamp_score(infrastructure_score),
            "people_ownership_readiness": cls._clamp_score(people_score),
            "governance_control_readiness": cls._clamp_score(governance_score),
            "organizational_adoption_readiness": cls._clamp_score(adoption_score),
        }
        note_profile = {
            "owner_hits": owner_hits,
            "adoption_hits": adoption_hits,
            "has_caution_prompt": bool(cls._count_keyword_hits(notes, cls.CAUTION_KEYWORDS)),
        }
        return dimensions, note_profile

    @classmethod
    def _build_readiness_outputs(cls, base_profile, hidden_dimensions, note_profile):
        data_mode = base_profile.get("data_mode", "demo")
        total_invoices = base_profile.get("total_invoices", 0)
        data_source_label = "dataset demo lokal" if data_mode == "demo" else "API internal perusahaan"
        core_field_summary = (
            f"{base_profile.get('core_fields_available', 0)}/{base_profile.get('core_fields_expected', 6)} atribut inti tersedia"
        )
        missing_fields_sentence = cls._format_list_as_sentence(base_profile.get("missing_core_fields", []))
        partner_focus_sentence = cls._format_list_as_sentence(base_profile.get("top_risk_partners", []))

        data_confidence = cls._score_to_confidence(hidden_dimensions["data_model_readiness"])
        deployment_confidence = cls._score_to_confidence(hidden_dimensions["infrastructure_readiness"])
        ownership_confidence = cls._score_to_confidence(hidden_dimensions["people_ownership_readiness"])
        control_confidence = cls._score_to_confidence(hidden_dimensions["governance_control_readiness"])
        adoption_confidence = cls._score_to_confidence(hidden_dimensions["organizational_adoption_readiness"])
        expected_gap_base = base_profile.get("expected_gap_base", 0)

        if data_mode == "demo":
            data_mode_line = (
                "Data masih berasal dari dataset demo lokal, sehingga laporan cocok untuk simulasi diskusi internal "
                "dan pengujian alur, bukan untuk komitmen operasional final."
            )
        else:
            data_mode_line = (
                "Data sudah ditarik dari API internal, sehingga laporan lebih layak dipakai sebagai dasar prioritas operasional "
                "selama sinkronisasi dan validasi sumber tetap terjaga."
            )

        readiness_signals = [
            f"- Business value clarity: {hidden_dimensions['business_value_clarity']}/5. Use case cashflow jelas dan langsung terkait percepatan realisasi invoice, pengendalian cash out, pengurangan risiko keterlambatan, dan prioritas follow-up.",
            f"- Data/model readiness: {hidden_dimensions['data_model_readiness']}/5. Sumber data saat ini adalah {data_source_label} dengan {total_invoices} invoice dan {core_field_summary}.",
            f"- Infrastructure/deployment readiness: {hidden_dimensions['infrastructure_readiness']}/5. Runtime aktif adalah {APP_SERVER} dengan queue {REPORT_MAX_CONCURRENT_JOBS} job dan thread Waitress {WAITRESS_THREADS}.",
            f"- People/ownership readiness: {hidden_dimensions['people_ownership_readiness']}/5. Sinyal owner atau sponsor eksplisit dari catatan pengguna = {note_profile['owner_hits']}.",
            f"- Governance/risk control: {hidden_dimensions['governance_control_readiness']}/5. Internal data tetap menjadi sumber fakta utama, OSINT hanya pendukung, dan fallback menjaga konsistensi struktur laporan.",
            f"- Organizational adoption readiness: {hidden_dimensions['organizational_adoption_readiness']}/5. Sinyal kesiapan adopsi atau pilot dari catatan pengguna = {note_profile['adoption_hits']}.",
            "- Gunakan sinyal ini untuk mengatur tingkat keyakinan, caveat, risiko kontrol, kepemilikan tindakan, dan prasyarat implementasi secara halus tanpa menyebut kerangka internal apa pun.",
        ]

        confidence_lines = [
            f"- Tingkat keyakinan data saat ini {data_confidence} karena laporan memakai {data_source_label} dengan {core_field_summary}.",
            f"- Kesiapan operasional aplikasi berada pada tingkat {deployment_confidence}; jalur deployment saat ini {APP_SERVER} dengan dukungan antrean {REPORT_MAX_CONCURRENT_JOBS} pekerjaan paralel.",
            f"- Kepastian penanggung jawab lintas fungsi berada pada tingkat {ownership_confidence}; sinyal owner eksplisit yang tertangkap dari catatan pengguna = {note_profile['owner_hits']}.",
            data_mode_line,
        ]

        assumption_lines = [
            f"- Analisis ini mengasumsikan {total_invoices} invoice yang tersedia sudah mewakili pola penagihan utama pada portofolio yang dibahas.",
            f"- Atribut inti yang terbaca saat ini adalah {core_field_summary}; atribut yang belum kuat atau belum tersedia: {missing_fields_sentence}.",
            "- Proyeksi arus kas masuk dibaca sebagai skenario manajemen berbasis histori kelas pembayaran, bukan kepastian realisasi kas.",
        ]
        if data_mode == "demo":
            assumption_lines.append("- Karena masih demo mode, angka dan narasi diposisikan sebagai bahan kalibrasi diskusi sebelum integrasi source-of-truth internal.")
        if note_profile["owner_hits"] == 0:
            assumption_lines.append("- Catatan pengguna belum menyebut penanggung jawab spesifik, sehingga penetapan owner tindakan masih perlu divalidasi saat rapat.")

        control_lines = [
            f"- Postur kontrol saat ini berada pada tingkat {control_confidence}; invoice prioritas tetap memerlukan verifikasi manual sebelum komitmen eskalasi atau forecast dinaikkan.",
            f"- Fokus kontrol utama saat ini berada pada konsentrasi risiko di {partner_focus_sentence} dan invoice Kelas D/E yang bernilai besar.",
            "- Sebelum eskalasi ke klien, verifikasi ulang invoice prioritas, kelengkapan dokumen, dan status komitmen bayar terbaru.",
            "- OSINT hanya dipakai sebagai konteks eksternal; fakta invoice, nilai, dan prioritas tetap harus mengikuti data internal yang tersedia.",
        ]
        if data_mode == "demo":
            control_lines.append("- Hindari menjadikan hasil demo sebagai dasar komitmen eksternal atau forecast final tanpa verifikasi terhadap sistem internal.")
        if hidden_dimensions["governance_control_readiness"] <= 2:
            control_lines.append("- Risiko salah tafsir naik bila data tambahan dan kontrol review manual tidak disiapkan sebelum sesi tindak lanjut.")

        implementation_lines = [
            f"- Untuk penggunaan operasional yang lebih kuat, pertahankan minimal {REPORT_MAX_CONCURRENT_JOBS} slot antrean dan gunakan runtime {APP_SERVER if APP_SERVER == 'waitress' else 'Waitress pada uji bersama'} untuk akses internal bersama.",
            "- Pastikan ritme refresh data, sumber invoice prioritas, dan jalur eskalasi ke account owner diputuskan sebelum laporan dipakai sebagai dasar action plan mingguan.",
        ]
        if data_mode == "demo":
            implementation_lines.append("- Langkah implementasi terdekat adalah memetakan endpoint API internal, autentikasi, dan struktur data source-of-truth agar prioritas penagihan tidak lagi bergantung pada dataset simulasi.")
        else:
            implementation_lines.append("- Pertahankan monitoring sinkronisasi API, validasi schema, dan fallback dokumen agar kualitas laporan tetap stabil saat dipakai banyak pengguna.")

        organizational_lines = [
            f"- Kesiapan pelaksanaan saat ini berada pada tingkat {adoption_confidence}; laporan akan lebih mudah dieksekusi bila sponsor bisnis, finance collection, dan account owner duduk pada forum review yang sama.",
            "- Gunakan laporan ini sebagai bahan keputusan internal: apa yang harus ditagih lebih dulu, siapa yang memimpin eskalasi, dan kontrol apa yang wajib ditutup sebelum follow-up berikutnya.",
        ]
        if note_profile["owner_hits"] == 0:
            organizational_lines.append("- Tetapkan minimal satu owner bisnis dan satu owner operasional untuk setiap cluster invoice prioritas agar keputusan rapat langsung bisa dijalankan.")
        if note_profile["adoption_hits"] == 0:
            organizational_lines.append("- Siapkan ritme pilot atau review berkala agar penggunaan laporan tidak berhenti di tahap analisis saja.")

        review_context = {
            "dataSource": "Demo dataset lokal" if data_mode == "demo" else "API internal perusahaan",
            "dataStatus": f"{total_invoices} invoice siap dianalisis",
            "operationalScope": "Analisis cashflow (arus kas masuk dan arus kas keluar), risiko realisasi invoice, dan prioritas tindak lanjut 30 hari.",
            "reportPurpose": "Bahan diskusi internal manajemen untuk keputusan penagihan, kontrol risiko, dan kesiapan pelaksanaan.",
            "readinessCaveat": data_mode_line,
            "controlNote": "Fakta internal tetap menjadi sumber utama; konteks eksternal hanya dipakai untuk memperkaya pembacaan risiko.",
        }
        cash_plan_implications = [
            "- Base case sebaiknya dipakai sebagai jangkar pembacaan rencana kas jangka pendek, sedangkan upside dan downside dipakai untuk menguji kebutuhan eskalasi dan ruang koreksi target.",
            "- Semakin besar eksposur Kelas D/E pada partner bernilai tinggi, semakin besar kebutuhan buffer keputusan, ritme follow-up, dan verifikasi dokumen sebelum asumsi arus kas masuk dinaikkan.",
        ]
        if data_mode == "demo":
            cash_plan_implications.append("- Karena masih demo mode, implikasi rencana kas diposisikan sebagai arah diskusi internal, bukan angka forecast final.")
        if expected_gap_base > 0:
            cash_plan_implications.append("- Gap arus kas masuk pada base case harus dibaca sebagai ruang risiko yang perlu diperkecil lewat penagihan prioritas, bukan langsung diasumsikan akan pulih otomatis.")
        cash_plan_implications.append("- Tekanan cash out harus dibaca bersama ending cash, runway, dan coverage ratio; kenaikan arus kas masuk saja tidak cukup bila kewajiban jatuh tempo menumpuk pada horizon yang sama.")

        return {
            "readiness_signals": "\n".join(readiness_signals),
            "confidence_summary": "\n".join(confidence_lines),
            "assumptions": "\n".join(assumption_lines),
            "controls": "\n".join(control_lines),
            "implementation_prerequisites": "\n".join(implementation_lines),
            "organizational_readiness": "\n".join(organizational_lines),
            "cash_plan_implications": "\n".join(cash_plan_implications),
            "review_context": review_context,
        }

    @classmethod
    def _build_visual_prompt(cls, class_distribution):
        labels = []
        for payment_class, metrics in class_distribution.items():
            labels.append(f"{payment_class},{round(metrics['share'], 1)}")
        chart_marker = (
            "[[CHART: Distribusi Historis Kelas Pembayaran | Persentase Invoice | "
            + "; ".join(labels)
            + "]]"
        )
        flow_marker = (
            "[[FLOW: Prioritisasi Invoice Risiko Tinggi -> Penagihan Berbasis Bukti -> "
            "Eskalasi Manajemen -> Pemulihan Cashflow]]"
        )
        return f"{chart_marker}\n{flow_marker}"

    @classmethod
    def apply_silent_assessment(cls, context, notes=""):
        base_profile = context.get("base_profile", {})
        hidden_dimensions, note_profile = cls._build_hidden_dimensions(base_profile, notes)
        visible_outputs = cls._build_readiness_outputs(base_profile, hidden_dimensions, note_profile)
        return {
            **visible_outputs,
            "hidden_dimensions": hidden_dimensions,
            "note_profile": note_profile,
        }

    @classmethod
    def build_report_context(cls, df, data_mode="demo"):
        if df is None or df.empty:
            return {
                "financial_summary": "Tidak ada data finansial internal yang tersedia.",
                "evidence": "Tidak ada catatan historis yang tersedia.",
                "diagnostic_breakdown": "- Belum ada pola hambatan yang dapat dijelaskan karena data masih kosong.",
                "management_brief": "Tidak ada management brief yang dapat disusun dari data kosong.",
                "executive_facts": "- Tidak ada fakta eksekutif yang tersedia.",
                "scenario_table": "| Skenario | Estimasi Arus Kas Masuk | Gap terhadap Total Invoice | Narasi Manajemen |\n|---|---:|---:|---|\n| Base Case | Rp 0 | Rp 0 | Data kosong. |",
                "priority_table": "| Prioritas | Fokus | Penanggung Jawab | Isu Utama | Aksi 30 Hari | Dampak yang Diharapkan |\n|---:|---|---|---|---|---|\n| 1 | Tidak ada data | Finance Collection | - | Lengkapi data terlebih dahulu. | Memberi dasar analisis yang layak. |",
                "meeting_agenda": "1. Pastikan data internal tersedia sebelum rapat dilanjutkan.",
                "base_profile": {
                    "data_mode": data_mode,
                    "total_invoices": 0,
                    "total_invoice_value": 0,
                    "delayed_invoice_value": 0,
                    "high_risk_invoices": 0,
                    "high_risk_invoice_value": 0,
                    "expected_realization_base": 0,
                    "expected_gap_base": 0,
                    "core_fields_available": 0,
                    "core_fields_expected": 6,
                    "missing_core_fields": [
                        "periode",
                        "partner",
                        "layanan",
                        "kelas pembayaran",
                        "nilai invoice",
                        "catatan keterlambatan",
                    ],
                    "top_risk_partners": [],
                },
                "visual_prompt": "Do not force visuals.",
            }

        working_df = df.copy()
        period_column = cls._find_column(working_df, "period")
        partner_column = cls._find_column(working_df, "partner")
        service_column = cls._find_column(working_df, "service")
        payment_class_column = cls._find_column(working_df, "payment_class")
        invoice_value_column = cls._find_column(working_df, "invoice_value")
        notes_column = cls._find_column(working_df, "notes")
        core_field_map = {
            "periode": period_column,
            "partner": partner_column,
            "layanan": service_column,
            "kelas pembayaran": payment_class_column,
            "nilai invoice": invoice_value_column,
            "catatan keterlambatan": notes_column,
        }
        missing_core_fields = [
            label for label, column in core_field_map.items() if not column
        ]

        working_df["__period"] = (
            working_df[period_column].astype(str).fillna("Tidak Diketahui")
            if period_column
            else "Tidak Diketahui"
        )
        working_df["__partner"] = (
            working_df[partner_column].astype(str).fillna("Tidak Diketahui")
            if partner_column
            else "Tidak Diketahui"
        )
        working_df["__service"] = (
            working_df[service_column].astype(str).fillna("Tidak Diketahui")
            if service_column
            else "Tidak Diketahui"
        )
        working_df["__note"] = (
            working_df[notes_column].astype(str).fillna("")
            if notes_column
            else ""
        )
        working_df["__invoice_value"] = (
            working_df[invoice_value_column].apply(cls._parse_currency)
            if invoice_value_column
            else 0
        )
        class_labels = []
        class_scores = []
        source_series = working_df[payment_class_column] if payment_class_column else pd.Series([""] * len(working_df))
        for raw_value in source_series:
            payment_class, score = cls._detect_payment_class(raw_value)
            class_labels.append(payment_class)
            class_scores.append(score)
        working_df["__payment_class"] = class_labels
        working_df["__payment_score"] = class_scores
        working_df["__base_realization"] = working_df.apply(
            lambda row: row["__invoice_value"] * cls.REALIZATION_RATE_MAP.get(row["__payment_class"], 0.65),
            axis=1,
        )
        working_df["__upside_realization"] = working_df.apply(
            lambda row: row["__invoice_value"] * cls.UPSIDE_RATE_MAP.get(row["__payment_class"], 0.72),
            axis=1,
        )
        working_df["__downside_realization"] = working_df.apply(
            lambda row: row["__invoice_value"] * cls.DOWNSIDE_RATE_MAP.get(row["__payment_class"], 0.50),
            axis=1,
        )

        total_invoices = len(working_df)
        total_invoice_value = int(working_df["__invoice_value"].sum())
        delayed_invoices = int((working_df["__payment_score"] > 1).sum())
        high_risk_invoices = int((working_df["__payment_score"] >= 4).sum())
        delayed_invoice_value = int(working_df.loc[working_df["__payment_score"] > 1, "__invoice_value"].sum())
        high_risk_invoice_value = int(working_df.loc[working_df["__payment_score"] >= 4, "__invoice_value"].sum())
        weighted_risk_score = statistics.fmean(working_df["__payment_score"]) if total_invoices else 0
        expected_realization_base = int(round(working_df["__base_realization"].sum()))
        expected_realization_upside = int(round(working_df["__upside_realization"].sum()))
        expected_realization_downside = int(round(working_df["__downside_realization"].sum()))
        expected_gap_base = max(total_invoice_value - expected_realization_base, 0)
        expected_gap_upside = max(total_invoice_value - expected_realization_upside, 0)
        expected_gap_downside = max(total_invoice_value - expected_realization_downside, 0)

        class_summary_df = (
            working_df.groupby("__payment_class", dropna=False)
            .agg(
                invoice_count=("__payment_class", "size"),
                invoice_value=("__invoice_value", "sum"),
                risk_score=("__payment_score", "mean"),
            )
        )
        class_summary_df["__sort_key"] = [
            cls.PAYMENT_CLASS_ORDER.get(index_value, 99) for index_value in class_summary_df.index
        ]
        class_summary_df = class_summary_df.sort_values("__sort_key")
        class_distribution = {}
        for payment_class, row in class_summary_df.iterrows():
            class_distribution[payment_class] = {
                "count": int(row["invoice_count"]),
                "value": int(row["invoice_value"]),
                "share": (row["invoice_count"] / total_invoices) * 100 if total_invoices else 0,
            }

        partner_summary_df = (
            working_df.groupby("__partner", dropna=False)
            .agg(
                invoice_count=("__partner", "size"),
                invoice_value=("__invoice_value", "sum"),
                avg_risk_score=("__payment_score", "mean"),
            )
            .sort_values(["avg_risk_score", "invoice_value"], ascending=[False, False])
            .head(5)
        )
        service_summary_df = (
            working_df.groupby("__service", dropna=False)
            .agg(
                invoice_count=("__service", "size"),
                invoice_value=("__invoice_value", "sum"),
                avg_risk_score=("__payment_score", "mean"),
            )
            .sort_values(["avg_risk_score", "invoice_value"], ascending=[False, False])
            .head(5)
        )
        period_summary_df = (
            working_df.groupby("__period", dropna=False)
            .agg(
                invoice_count=("__period", "size"),
                invoice_value=("__invoice_value", "sum"),
                avg_risk_score=("__payment_score", "mean"),
            )
            .reset_index()
        )
        period_summary_df["__sort_key"] = period_summary_df["__period"].apply(cls._parse_period_sort_key)
        period_summary_df = period_summary_df.sort_values("__sort_key").tail(6)

        high_risk_subset_df = working_df[working_df["__payment_score"] >= 4].copy()
        high_risk_partner_df = (
            high_risk_subset_df.groupby("__partner", dropna=False)
            .agg(
                invoice_count=("__partner", "size"),
                invoice_value=("__invoice_value", "sum"),
                avg_risk_score=("__payment_score", "mean"),
            )
            .sort_values(["invoice_value", "invoice_count"], ascending=[False, False])
            .head(5)
            if not high_risk_subset_df.empty
            else pd.DataFrame(columns=["invoice_count", "invoice_value", "avg_risk_score"])
        )
        high_risk_service_df = (
            high_risk_subset_df.groupby("__service", dropna=False)
            .agg(
                invoice_count=("__service", "size"),
                invoice_value=("__invoice_value", "sum"),
                avg_risk_score=("__payment_score", "mean"),
            )
            .sort_values(["invoice_value", "invoice_count"], ascending=[False, False])
            .head(5)
            if not high_risk_subset_df.empty
            else pd.DataFrame(columns=["invoice_count", "invoice_value", "avg_risk_score"])
        )

        top_themes = cls._extract_delay_themes(working_df["__note"])
        evidence_rows = working_df.sort_values(
            ["__payment_score", "__invoice_value"],
            ascending=[False, False],
        ).head(8)
        priority_rows = []
        for row_index, (_, row) in enumerate(evidence_rows.iterrows(), start=1):
            theme, action_plan = cls._get_action_plan(row["__note"])
            priority_rows.append(
                {
                    "priority": row_index,
                    "focus": f"{row['__partner']} / {row['__service']}",
                    "issue": theme,
                    "payment_class": row["__payment_class"],
                    "invoice_value": int(row["__invoice_value"]),
                    "action": action_plan["action"],
                    "impact": action_plan["impact"],
                    "owner": action_plan["owner"],
                }
            )

        latest_period_summary = period_summary_df.iloc[-1] if not period_summary_df.empty else None
        previous_period_summary = period_summary_df.iloc[-2] if len(period_summary_df) >= 2 else None

        recent_trend_line = "- Belum ada cukup periode untuk membaca perubahan terbaru."
        recent_value_change_line = "- Perubahan nilai invoice belum dapat dibandingkan antarperiode."
        recent_risk_change_line = "- Perubahan skor risiko belum dapat dibandingkan antarperiode."
        recent_period_label = latest_period_summary["__period"] if latest_period_summary is not None else "periode terbaru"

        if latest_period_summary is not None and previous_period_summary is not None:
            value_delta = int(latest_period_summary["invoice_value"] - previous_period_summary["invoice_value"])
            prior_value = int(previous_period_summary["invoice_value"])
            value_delta_pct = (value_delta / prior_value) * 100 if prior_value else 0
            risk_delta = float(latest_period_summary["avg_risk_score"] - previous_period_summary["avg_risk_score"])
            count_delta = int(latest_period_summary["invoice_count"] - previous_period_summary["invoice_count"])

            value_direction = "naik" if value_delta > 0 else "turun" if value_delta < 0 else "relatif stabil"
            risk_direction = "naik" if risk_delta > 0.05 else "turun" if risk_delta < -0.05 else "relatif stabil"
            risk_interpretation = (
                "memburuk"
                if risk_delta > 0.05
                else "membaik"
                if risk_delta < -0.05
                else "stabil"
            )
            recent_trend_line = (
                f"- Periode terbaru yang diamati adalah {previous_period_summary['__period']} ke {latest_period_summary['__period']} "
                f"dengan jumlah invoice berubah {count_delta:+d} dan nilai invoice {value_direction} "
                f"{abs(value_delta_pct):.1f}%."
            )
            recent_value_change_line = (
                f"- Nilai invoice {previous_period_summary['__period']} = {cls._format_currency(int(previous_period_summary['invoice_value']))}; "
                f"{latest_period_summary['__period']} = {cls._format_currency(int(latest_period_summary['invoice_value']))}."
            )
            recent_risk_change_line = (
                f"- Skor risiko rata-rata {risk_direction} {abs(risk_delta):.2f} poin dari "
                f"{previous_period_summary['avg_risk_score']:.2f} ke {latest_period_summary['avg_risk_score']:.2f}, "
                f"yang berarti kondisi penagihan {risk_interpretation}."
            )

        top_risk_partner_names = ", ".join(high_risk_partner_df.index.tolist()[:3]) if not high_risk_partner_df.empty else "-"
        top_risk_service_names = ", ".join(high_risk_service_df.index.tolist()[:3]) if not high_risk_service_df.empty else "-"
        top_theme_map = {theme: count for theme, count in top_themes}
        process_issue_count = top_theme_map.get("Dokumen dan administrasi", 0) + top_theme_map.get("Sengketa atau klarifikasi", 0)
        budget_issue_count = top_theme_map.get("Siklus anggaran", 0) + top_theme_map.get("Persetujuan internal klien", 0)
        liquidity_issue_count = top_theme_map.get("Likuiditas pelanggan", 0)

        diagnostic_breakdown_lines = []
        if process_issue_count:
            diagnostic_breakdown_lines.append(
                f"1. Hambatan proses, dokumen, dan klarifikasi masih dominan dengan {process_issue_count} sinyal historis; ini biasanya menahan invoice yang sebetulnya sudah siap ditagih tetapi belum lolos kelengkapan atau sign-off."
            )
        if budget_issue_count:
            diagnostic_breakdown_lines.append(
                f"2. Hambatan anggaran dan persetujuan internal klien muncul pada {budget_issue_count} catatan; dampaknya paling terasa pada akun pemerintah, BUMN, dan partner dengan approval berlapis."
            )
        if liquidity_issue_count:
            diagnostic_breakdown_lines.append(
                f"3. Tekanan likuiditas pelanggan muncul pada {liquidity_issue_count} catatan; risiko ini perlu dibedakan dari isu administratif karena membutuhkan pola negosiasi dan komitmen bayar yang lebih aktif."
            )
        if top_risk_partner_names != "-":
            diagnostic_breakdown_lines.append(
                f"4. Eksposur dampak terbesar saat ini terkonsentrasi pada {top_risk_partner_names}, sehingga setiap bottleneck di segmen tersebut memberi pengaruh paling besar ke arus kas masuk dan kualitas ending cash."
            )
        if not diagnostic_breakdown_lines:
            diagnostic_breakdown_lines.append("1. Belum ada pola hambatan dominan yang cukup kuat untuk dipisahkan dari catatan historis.")

        financial_summary_lines = [
            "## Snapshot Arus Kas Masuk",
            f"- Total invoice dianalisis: {total_invoices}",
            f"- Total nilai invoice: {cls._format_currency(total_invoice_value)}",
            f"- Porsi invoice terlambat: {cls._format_percentage((delayed_invoices / total_invoices) * 100 if total_invoices else 0)}",
            f"- Nilai invoice terlambat: {cls._format_currency(delayed_invoice_value)}",
            f"- Porsi invoice risiko tinggi (Kelas D/E): {cls._format_percentage((high_risk_invoices / total_invoices) * 100 if total_invoices else 0)}",
            f"- Nilai invoice risiko tinggi (Kelas D/E): {cls._format_currency(high_risk_invoice_value)}",
            f"- Skor risiko penagihan rata-rata: {weighted_risk_score:.2f} dari 5.00",
            f"- Estimasi arus kas masuk risk-adjusted (base case): {cls._format_currency(expected_realization_base)} atau {cls._format_percentage((expected_realization_base / total_invoice_value) * 100 if total_invoice_value else 0)} dari total nilai invoice",
            f"- Gap arus kas masuk pada base case: {cls._format_currency(expected_gap_base)}",
            "",
            "## Pergerakan Periode Terbaru",
            recent_trend_line,
            recent_value_change_line,
            recent_risk_change_line,
            "",
            "## Distribusi Kelas Pembayaran",
            "| Kelas | Jumlah Invoice | Nilai Invoice | Porsi Invoice |",
            "|---|---:|---:|---:|",
        ]
        for payment_class, metrics in class_distribution.items():
            financial_summary_lines.append(
                f"| {payment_class} | {metrics['count']} | {cls._format_currency(metrics['value'])} | {cls._format_percentage(metrics['share'])} |"
            )

        financial_summary_lines.extend(
            [
                "",
                "## Segmentasi Risiko per Tipe Partner",
                "| Tipe Partner | Jumlah Invoice | Nilai Invoice | Skor Risiko Rata-rata |",
                "|---|---:|---:|---:|",
            ]
        )
        for partner, row in partner_summary_df.iterrows():
            financial_summary_lines.append(
                f"| {partner} | {int(row['invoice_count'])} | {cls._format_currency(int(row['invoice_value']))} | {row['avg_risk_score']:.2f} |"
            )

        financial_summary_lines.extend(
            [
                "",
                "## Segmentasi Risiko per Layanan",
                "| Layanan | Jumlah Invoice | Nilai Invoice | Skor Risiko Rata-rata |",
                "|---|---:|---:|---:|",
            ]
        )
        for service, row in service_summary_df.iterrows():
            financial_summary_lines.append(
                f"| {service} | {int(row['invoice_count'])} | {cls._format_currency(int(row['invoice_value']))} | {row['avg_risk_score']:.2f} |"
            )

        financial_summary_lines.extend(
            [
                "",
                "## Tren Ringkas per Periode",
                "| Periode | Jumlah Invoice | Nilai Invoice | Skor Risiko Rata-rata |",
                "|---|---:|---:|---:|",
            ]
        )
        for _, row in period_summary_df.iterrows():
            financial_summary_lines.append(
                f"| {row['__period']} | {int(row['invoice_count'])} | {cls._format_currency(int(row['invoice_value']))} | {row['avg_risk_score']:.2f} |"
            )

        financial_summary_lines.extend(
            [
                "",
                "## Konsentrasi Eksposur Risiko Tinggi",
                "| Tipe Partner | Jumlah Invoice D/E | Nilai Invoice D/E | Skor Risiko Rata-rata |",
                "|---|---:|---:|---:|",
            ]
        )
        if not high_risk_partner_df.empty:
            for partner, row in high_risk_partner_df.iterrows():
                financial_summary_lines.append(
                    f"| {partner} | {int(row['invoice_count'])} | {cls._format_currency(int(row['invoice_value']))} | {row['avg_risk_score']:.2f} |"
                )
        else:
            financial_summary_lines.append("| Tidak ada konsentrasi D/E | 0 | Rp 0 | 0.00 |")

        financial_summary_lines.extend(["", "## Sinyal Diagnostik Utama"])
        if top_themes:
            for index, (theme, count) in enumerate(top_themes, start=1):
                financial_summary_lines.append(f"{index}. {theme} muncul pada {count} catatan historis.")
        else:
            financial_summary_lines.append("1. Belum ada tema dominan yang dapat diambil dari catatan historis.")

        evidence_lines = []
        for _, row in evidence_rows.iterrows():
            note = str(row["__note"]).strip()
            if not note:
                continue
            trimmed_note = cls._trim_note_for_report(note, max_length=220)
            evidence_lines.append(
                "\n".join(
                    [
                        f"- {row['__period']} | {row['__partner']} | {row['__service']}",
                        f"  - Kelas pembayaran: {row['__payment_class']}",
                        f"  - Nilai invoice: {cls._format_currency(int(row['__invoice_value']))}",
                        f"  - Catatan utama: {trimmed_note}",
                    ]
                )
            )
        if not evidence_lines:
            evidence_lines.append("- Tidak ada catatan historis yang cukup untuk dikutip.")

        executive_fact_lines = [
            f"- Portofolio yang dianalisis mencakup {total_invoices} invoice dengan total nilai {cls._format_currency(total_invoice_value)}.",
            f"- Invoice terlambat mencapai {delayed_invoices} kasus atau {cls._format_percentage((delayed_invoices / total_invoices) * 100 if total_invoices else 0)} dari populasi, dengan nilai tertunda {cls._format_currency(delayed_invoice_value)}.",
            f"- Eksposur risiko tinggi Kelas D/E mencapai {high_risk_invoices} invoice senilai {cls._format_currency(high_risk_invoice_value)} dan terkonsentrasi pada {top_risk_partner_names}.",
            f"- Estimasi arus kas masuk risk-adjusted base case adalah {cls._format_currency(expected_realization_base)} dengan gap {cls._format_currency(expected_gap_base)} terhadap total nilai invoice.",
            recent_trend_line,
            recent_risk_change_line,
            f"- Layanan dengan eksposur risiko tinggi paling dominan saat ini: {top_risk_service_names}.",
        ]
        scenario_lines = [
            "| Skenario | Estimasi Arus Kas Masuk | Gap terhadap Total Invoice | Narasi Manajemen |",
            "|---|---:|---:|---|",
            f"| Upside | {cls._format_currency(expected_realization_upside)} | {cls._format_currency(expected_gap_upside)} | Terjadi bila perbaikan approval, kelengkapan dokumen, dan penagihan pada akun prioritas dijalankan cepat. |",
            f"| Base Case | {cls._format_currency(expected_realization_base)} | {cls._format_currency(expected_gap_base)} | Menggambarkan realisasi yang paling realistis bila perilaku penagihan tetap mengikuti pola historis saat ini. |",
            f"| Downside | {cls._format_currency(expected_realization_downside)} | {cls._format_currency(expected_gap_downside)} | Terjadi bila siklus anggaran, likuiditas pelanggan, atau dispute memburuk sehingga semakin banyak invoice bergeser ke kelas risiko tinggi. |",
        ]
        priority_table_lines = [
            "| Prioritas | Fokus | Penanggung Jawab | Isu Utama | Aksi 30 Hari | Dampak yang Diharapkan |",
            "|---:|---|---|---|---|---|",
        ]
        management_priority_lines = [
            "| Prioritas | Fokus | Kelas | Nilai Invoice | Isu Utama | Aksi Awal | Fungsi Utama |",
            "|---:|---|---|---:|---|---|---|",
        ]
        for item in priority_rows[:6]:
            management_priority_lines.append(
                f"| {item['priority']} | {item['focus']} | {item['payment_class']} | {cls._format_currency(item['invoice_value'])} | {item['issue']} | {item['action']} | {item['owner']} |"
            )
            priority_table_lines.append(
                f"| {item['priority']} | {item['focus']} | {item['owner']} | {item['issue']} | {item['action']} | {item['impact']} |"
            )
        meeting_questions = [
            "Segmen partner mana yang paling layak mendapat eskalasi manajemen karena menggabungkan nilai invoice besar dan skor risiko tinggi?",
            "Hambatan apa yang paling sering berulang: anggaran, approval, dokumen, likuiditas, atau sengketa ruang lingkup?",
            f"Apakah strategi 30 hari ke depan harus difokuskan pada {recent_period_label} dan akun-akun D/E agar gap arus kas masuk dapat ditekan tanpa memperburuk tekanan cash out?",
        ]
        agenda_lines = [f"{index}. {question}" for index, question in enumerate(meeting_questions, start=1)]
        management_brief_lines = (
            ["## Fakta Eksekutif yang Wajib Dijaga Konsisten"]
            + executive_fact_lines
            + ["", "## Skenario 1-2 Kuartal"]
            + scenario_lines
            + ["", "## Prioritas Penagihan untuk Pembahasan Internal"]
            + management_priority_lines
            + ["", "## Agenda Diskusi Manajemen"]
            + agenda_lines
        )

        return {
            "financial_summary": "\n".join(financial_summary_lines),
            "evidence": "\n".join(evidence_lines),
            "diagnostic_breakdown": "\n".join(diagnostic_breakdown_lines),
            "management_brief": "\n".join(management_brief_lines),
            "executive_facts": "\n".join(executive_fact_lines),
            "scenario_table": "\n".join(scenario_lines),
            "priority_table": "\n".join(priority_table_lines),
            "meeting_agenda": "\n".join(agenda_lines),
            "base_profile": {
                "data_mode": data_mode,
                "total_invoices": total_invoices,
                "total_invoice_value": total_invoice_value,
                "delayed_invoices": delayed_invoices,
                "delayed_invoice_value": delayed_invoice_value,
                "high_risk_invoices": high_risk_invoices,
                "high_risk_invoice_value": high_risk_invoice_value,
                "expected_realization_base": expected_realization_base,
                "expected_gap_base": expected_gap_base,
                "core_fields_available": len(core_field_map) - len(missing_core_fields),
                "core_fields_expected": len(core_field_map),
                "missing_core_fields": missing_core_fields,
                "top_risk_partners": high_risk_partner_df.index.tolist()[:3],
                "top_risk_services": high_risk_service_df.index.tolist()[:3],
            },
            "visual_prompt": cls._build_visual_prompt(class_distribution),
        }
