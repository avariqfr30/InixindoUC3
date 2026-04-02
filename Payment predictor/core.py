import copy
import concurrent.futures
import io
import json
import logging
import os
import re
import statistics
import textwrap
import threading
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse

import chromadb
import diskcache as dc
import markdown
import matplotlib
import matplotlib.patches as patches
import matplotlib.pyplot as plt
import pandas as pd
import requests
from bs4 import BeautifulSoup, NavigableString, Tag
from chromadb.config import Settings
from chromadb.utils import embedding_functions
from docx import Document
from docx.enum.text import WD_ALIGN_PARAGRAPH, WD_LINE_SPACING
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.shared import Cm, Inches, Pt, RGBColor
from pydantic import BaseModel, Field
from ollama import Client
from sqlalchemy import create_engine

from config import (
    APP_SERVER,
    DATA_ACQUISITION_MODE,
    DATA_DIR,
    DEFAULT_COLOR,
    DEMO_CSV_PATH,
    EMBED_MODEL,
    FINANCE_SYSTEM_PROMPT,
    INTERNAL_API_AUTH_TOKEN,
    INTERNAL_API_BASE_URL,
    INTERNAL_API_DATASET_PATH,
    INTERNAL_API_HEADERS_JSON,
    INTERNAL_API_QUERY_PARAMS_JSON,
    INTERNAL_API_RECORDS_KEY,
    INTERNAL_API_TIMEOUT,
    INTERNAL_API_VERIFY_SSL,
    LLM_MODEL,
    OLLAMA_HOST,
    PERSONAS,
    REPORT_NUM_CTX,
    REPORT_MAX_CONCURRENT_JOBS,
    REPORT_MIN_COMPLETENESS_SCORE,
    REPORT_NUM_PREDICT,
    REPORT_REPEAT_PENALTY,
    REPORT_SECTION_SEQUENCE,
    REPORT_TEMPERATURE,
    REPORT_TOP_P,
    SERPER_API_KEY,
    WAITRESS_THREADS,
    WRITER_FIRM_NAME,
)
from forecast_engine import parse_idr_amount

matplotlib.use("Agg")
logger = logging.getLogger(__name__)


# ==========================================
# PYDANTIC SCHEMAS & FAST CACHING
# ==========================================
class InsightSchema(BaseModel):
    insight: str = Field(description="The extracted insight in Indonesian. 'NOT_FOUND' if missing.")

# Initialize ultra-fast disk caching for OSINT (survives server restarts)
osint_cache_dir = Path(DATA_DIR) / '.osint_cache' if DATA_DIR else Path('./.osint_cache')
osint_cache = dc.Cache(str(osint_cache_dir))
# ==========================================


class InternalAPIClient:
    def __init__(self):
        self.base_url = INTERNAL_API_BASE_URL.rstrip("/")
        self.dataset_path = INTERNAL_API_DATASET_PATH.strip() or "/api/finance/invoices"
        self.records_key = INTERNAL_API_RECORDS_KEY.strip()
        self.auth_token = INTERNAL_API_AUTH_TOKEN.strip()
        self.timeout = INTERNAL_API_TIMEOUT
        self.verify_ssl = INTERNAL_API_VERIFY_SSL
        self.headers = self._parse_json_object(INTERNAL_API_HEADERS_JSON, "headers")
        self.query_params = self._parse_json_object(
            INTERNAL_API_QUERY_PARAMS_JSON,
            "query params",
        )

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
    def _extract_nested_value(payload, path):
        current = payload
        for key in path.split("."):
            if not isinstance(current, dict) or key not in current:
                raise KeyError(path)
            current = current[key]
        return current

    def is_configured(self):
        return bool(self.base_url)

    def fetch_records(self):
        if not self.is_configured():
            raise RuntimeError("Internal API base URL is not configured.")

        headers = {"Accept": "application/json"}
        headers.update(self.headers)
        if self.auth_token:
            headers.setdefault("Authorization", f"Bearer {self.auth_token}")

        dataset_url = urljoin(f"{self.base_url}/", self.dataset_path.lstrip("/"))
        response = requests.get(
            dataset_url,
            headers=headers,
            params=self.query_params,
            timeout=self.timeout,
            verify=self.verify_ssl,
        )
        response.raise_for_status()

        payload = response.json()
        records = payload

        if self.records_key:
            records = self._extract_nested_value(payload, self.records_key)
        elif isinstance(payload, dict):
            for candidate_key in ("records", "items", "results", "data", "invoices"):
                candidate_value = payload.get(candidate_key)
                if isinstance(candidate_value, list):
                    records = candidate_value
                    break

        if not isinstance(records, list):
            raise ValueError("Internal API response must resolve to a list of records.")

        return records


class KnowledgeBase:
    def __init__(self, db_uri):
        os.makedirs(DATA_DIR, exist_ok=True)
        self.engine = create_engine(db_uri)
        self.data_mode = "internal_api" if DATA_ACQUISITION_MODE == "internal_api" else "demo"
        self.table_name = f"invoices_{self.data_mode}"
        self.internal_api_client = InternalAPIClient()
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
        self.cache_lock = threading.Lock()
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

    def _load_demo_data(self):
        try:
            return pd.read_sql(f"SELECT * FROM {self.table_name}", self.engine)
        except Exception:
            csv_path = DEMO_CSV_PATH
            if not os.path.exists(csv_path):
                logger.error("Financial data source is unavailable.")
                return None

            raw_df = pd.read_csv(csv_path)
            raw_df.columns = [column.strip() for column in raw_df.columns]
            raw_df.to_sql(self.table_name, self.engine, index=False, if_exists="replace")
            return raw_df

    def _load_internal_api_data(self):
        if not self.internal_api_client.is_configured():
            logger.error("Internal data source is not configured.")
            return None

        try:
            records = self.internal_api_client.fetch_records()
        except Exception as exc:
            logger.error("Internal data sync failed: %s", exc)
            return None

        data_frame = self._normalize_records(records)
        if data_frame.empty:
            logger.error("Internal data source returned no records.")
            return None

        data_frame.to_sql(self.table_name, self.engine, index=False, if_exists="replace")
        return data_frame

    def _load_source_data(self):
        if self.data_mode == "internal_api":
            return self._load_internal_api_data()
        return self._load_demo_data()

    def _rebuild_embeddings(self):
        if self.df is None or self.df.empty:
            return False

        existing_ids = self.collection.get().get("ids", [])
        if existing_ids:
            self.collection.delete(ids=existing_ids)

        ids = []
        documents = []
        metadatas = []

        for index, row in self.df.iterrows():
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
        self.df = self._load_source_data()
        if self.df is None or self.df.empty:
            return False
        with self.cache_lock:
            self.report_context_cache = None
        return self._rebuild_embeddings()

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
            "impact": "Meningkatkan peluang pemulihan cash in pada akun yang menghadapi tekanan likuiditas.",
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
            f"- Business value clarity: {hidden_dimensions['business_value_clarity']}/5. Use case cash in jelas dan langsung terkait percepatan realisasi invoice, pengurangan risiko keterlambatan, dan prioritas follow-up.",
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
            "- Proyeksi cash in dibaca sebagai skenario manajemen berbasis histori kelas pembayaran, bukan kepastian realisasi kas.",
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
            "operationalScope": "Analisis cash in, risiko realisasi invoice, dan prioritas tindak lanjut 30 hari.",
            "reportPurpose": "Bahan diskusi internal manajemen untuk keputusan penagihan, kontrol risiko, dan kesiapan pelaksanaan.",
            "readinessCaveat": data_mode_line,
            "controlNote": "Fakta internal tetap menjadi sumber utama; konteks eksternal hanya dipakai untuk memperkaya pembacaan risiko.",
        }
        cash_plan_implications = [
            "- Base case sebaiknya dipakai sebagai jangkar pembacaan rencana kas jangka pendek, sedangkan upside dan downside dipakai untuk menguji kebutuhan eskalasi dan ruang koreksi target.",
            "- Semakin besar eksposur Kelas D/E pada partner bernilai tinggi, semakin besar kebutuhan buffer keputusan, ritme follow-up, dan verifikasi dokumen sebelum asumsi cash in dinaikkan.",
        ]
        if data_mode == "demo":
            cash_plan_implications.append("- Karena masih demo mode, implikasi rencana kas diposisikan sebagai arah diskusi internal, bukan angka forecast final.")
        if expected_gap_base > 0:
            cash_plan_implications.append("- Gap cash in pada base case harus dibaca sebagai ruang risiko yang perlu diperkecil lewat penagihan prioritas, bukan langsung diasumsikan akan pulih otomatis.")

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
            "Eskalasi Manajemen -> Pemulihan Cash In]]"
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
                "scenario_table": "| Skenario | Estimasi Realisasi Cash In | Gap terhadap Total Invoice | Narasi Manajemen |\n|---|---:|---:|---|\n| Base Case | Rp 0 | Rp 0 | Data kosong. |",
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
                f"4. Eksposur dampak terbesar saat ini terkonsentrasi pada {top_risk_partner_names}, sehingga setiap bottleneck di segmen tersebut memberi pengaruh paling besar ke realisasi cash in."
            )
        if not diagnostic_breakdown_lines:
            diagnostic_breakdown_lines.append("1. Belum ada pola hambatan dominan yang cukup kuat untuk dipisahkan dari catatan historis.")

        financial_summary_lines = [
            "## Snapshot Cash In",
            f"- Total invoice dianalisis: {total_invoices}",
            f"- Total nilai invoice: {cls._format_currency(total_invoice_value)}",
            f"- Porsi invoice terlambat: {cls._format_percentage((delayed_invoices / total_invoices) * 100 if total_invoices else 0)}",
            f"- Nilai invoice terlambat: {cls._format_currency(delayed_invoice_value)}",
            f"- Porsi invoice risiko tinggi (Kelas D/E): {cls._format_percentage((high_risk_invoices / total_invoices) * 100 if total_invoices else 0)}",
            f"- Nilai invoice risiko tinggi (Kelas D/E): {cls._format_currency(high_risk_invoice_value)}",
            f"- Skor risiko penagihan rata-rata: {weighted_risk_score:.2f} dari 5.00",
            f"- Estimasi cash in risk-adjusted (base case): {cls._format_currency(expected_realization_base)} atau {cls._format_percentage((expected_realization_base / total_invoice_value) * 100 if total_invoice_value else 0)} dari total nilai invoice",
            f"- Gap cash in pada base case: {cls._format_currency(expected_gap_base)}",
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
            trimmed_note = note[:180] + ("..." if len(note) > 180 else "")
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
            f"- Estimasi cash in risk-adjusted base case adalah {cls._format_currency(expected_realization_base)} dengan gap {cls._format_currency(expected_gap_base)} terhadap total nilai invoice.",
            recent_trend_line,
            recent_risk_change_line,
            f"- Layanan dengan eksposur risiko tinggi paling dominan saat ini: {top_risk_service_names}.",
        ]
        scenario_lines = [
            "| Skenario | Estimasi Realisasi Cash In | Gap terhadap Total Invoice | Narasi Manajemen |",
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
            f"Apakah strategi 30 hari ke depan harus difokuskan pada {recent_period_label} dan akun-akun D/E agar gap cash in dapat ditekan?",
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


class Researcher:
    _SERPER_ENDPOINTS = {
        "search": "https://google.serper.dev/search",
        "news": "https://google.serper.dev/news",
    }

    _OSINT_TOPICS = [
        {
            "topic": "Siklus Anggaran Pemerintah",
            "query": "siklus pencairan APBN APBD termin pembayaran vendor Indonesia",
        },
        {
            "topic": "Perilaku Pembayaran BUMN dan Korporasi",
            "query": "tren keterlambatan pembayaran invoice BUMN swasta Indonesia",
        },
        {
            "topic": "Likuiditas dan Piutang Bisnis",
            "query": "risiko likuiditas perusahaan jasa Indonesia karena piutang tertunda",
        },
        {
            "topic": "Regulasi Pengadaan dan Kontrak",
            "query": "regulasi terbaru pengadaan pemerintah termin pembayaran penyedia Indonesia",
        },
    ]

    _DELAY_FACTOR_TOPICS = [
        {
            "factor": "Siklus anggaran pemerintah",
            "query": "siklus pencairan APBN APBD keterlambatan pembayaran vendor Indonesia",
            "delay_days": (10, 30),
            "impact": "Potensi mundur tambahan ketika termin pembayaran bergantung pada pencairan anggaran.",
        },
        {
            "factor": "Approval korporasi dan BUMN",
            "query": "approval internal BUMN korporasi keterlambatan pembayaran invoice Indonesia",
            "delay_days": (7, 21),
            "impact": "Potensi penambahan hari tunggu karena approval berlapis, BAST, atau verifikasi akhir.",
        },
        {
            "factor": "Likuiditas pelanggan",
            "query": "tekanan likuiditas perusahaan Indonesia keterlambatan pembayaran invoice jasa",
            "delay_days": (14, 45),
            "impact": "Potensi penundaan tambahan ketika pelanggan sedang menjaga kas atau menahan pengeluaran.",
        },
        {
            "factor": "Regulasi pengadaan dan administrasi kontrak",
            "query": "regulasi pengadaan pemerintah administrasi kontrak termin pembayaran vendor Indonesia",
            "delay_days": (5, 20),
            "impact": "Potensi penambahan waktu akibat revisi dokumen, termin, atau penyesuaian administrasi kontrak.",
        },
    ]

    @staticmethod
    def _is_serper_available():
        return bool(
            SERPER_API_KEY
            and SERPER_API_KEY.strip()
            and SERPER_API_KEY != "masukkan_api_key_serper_anda_disini"
        )

    @staticmethod
    def fetch_full_markdown(url):
        """Fetches the clean markdown text of any URL using Jina Reader."""
        if not url: return ""
        try:
            jina_url = f"https://r.jina.ai/{url}"
            headers = {'User-Agent': 'Mozilla/5.0'}
            response = requests.get(jina_url, headers=headers, timeout=12)
            if response.status_code == 200:
                return response.text[:6000] 
            return ""
        except Exception as e:
            logger.warning("Failed to fetch full markdown for %s: %s", url, e)
            return ""

    @classmethod
    def extract_insight_with_llm(cls, url, extraction_goal):
        """Universal Deep Scraper: Reads a URL and extracts a specific qualitative insight via Pydantic/LLM."""
        markdown_text = cls.fetch_full_markdown(url)
        if not markdown_text:
            return ""
            
        prompt = f"""
        You are an expert business researcher. Read the following source text.
        Your goal is to extract: {extraction_goal}
        
        SOURCE TEXT:
        {markdown_text}
        
        Respond ONLY with a valid JSON object using this schema. If the information is not present, use "NOT_FOUND".
        {{
            "insight": "<concise professional summary in Indonesian>"
        }}
        """
        try:
            client = Client(host=OLLAMA_HOST)
            res = client.chat(
                model=LLM_MODEL,
                messages=[{'role': 'user', 'content': prompt}],
                options={'temperature': 0.0}
            )
            raw_text = res['message']['content']
            match = re.search(r'\{.*\}', raw_text, re.DOTALL)
            parsed_dict = json.loads(match.group(0)) if match else json.loads(raw_text)
            
            # Pydantic validation
            data = InsightSchema.model_validate(parsed_dict)
            
            if "NOT_FOUND" in data.insight.upper() or not data.insight:
                return ""
            return data.insight
        except Exception as e:
            logger.warning("Insight extraction failed for %s: %s", url, e)
            return ""

    @classmethod
    def _execute_serper_query(cls, query, mode="search", num_results=6):
        if not cls._is_serper_available():
            return []

        endpoint = cls._SERPER_ENDPOINTS.get(mode, cls._SERPER_ENDPOINTS["search"])
        headers = {
            "X-API-KEY": SERPER_API_KEY,
            "Content-Type": "application/json",
        }
        payload = {
            "q": query,
            "num": num_results,
            "gl": "id",
            "hl": "id",
        }

        try:
            response = requests.post(endpoint, headers=headers, data=json.dumps(payload), timeout=10)
            response.raise_for_status()
            body = response.json()
        except Exception as exc:
            logger.warning("Serper %s request failed: %s", mode, exc)
            return []

        result_key = "organic" if mode == "search" else "news"
        rows = body.get(result_key, [])

        normalized = []
        for row in rows:
            title = (row.get("title") or "").strip()
            snippet = (row.get("snippet") or "").strip()
            link = (row.get("link") or "").strip()
            date = (row.get("date") or "").strip()

            if not title and not snippet:
                continue

            domain = urlparse(link).netloc.replace("www.", "") if link else "-"
            normalized.append(
                {
                    "title": title,
                    "snippet": snippet,
                    "link": link,
                    "domain": domain,
                    "date": date,
                }
            )

        return normalized

    @staticmethod
    def _deduplicate(items):
        deduplicated = []
        seen = set()

        for item in items:
            fingerprint = "|".join(
                [
                    item.get("domain", "").lower(),
                    item.get("title", "").lower(),
                    item.get("snippet", "")[:120].lower(),
                ]
            )
            if fingerprint in seen:
                continue
            seen.add(fingerprint)
            deduplicated.append(item)

        return deduplicated

    @staticmethod
    def _format_topic(topic, entries):
        lines = [f"[{topic}]"]

        if not entries:
            lines.append("- Tidak ada sinyal eksternal yang relevan.")
            return "\n".join(lines)

        for index, entry in enumerate(entries[:4], start=1):
            title = entry.get("title") or "Tanpa judul"
            snippet = entry.get("snippet") or "Tidak ada ringkasan."
            source = entry.get("domain") or "-"
            date = f" ({entry['date']})" if entry.get("date") else ""

            lines.append(f"{index}. {title}{date}")
            lines.append(f"   Inti: {snippet}")
            lines.append(f"   Sumber: {source}")

        return "\n".join(lines)

    @classmethod
    @osint_cache.memoize(expire=86400)
    def get_macro_finance_trends(cls, extra_context=""):
        if not cls._is_serper_available():
            return "Data OSINT eksternal tidak tersedia (SERPER_API_KEY belum dikonfigurasi)."

        context_snippet = (extra_context or "").strip()
        search_jobs = []
        for topic_config in cls._OSINT_TOPICS:
            query = topic_config["query"]
            if context_snippet:
                query = f"{query} {context_snippet[:180]}"

            search_jobs.append((topic_config["topic"], query, "search"))
            search_jobs.append((topic_config["topic"], query, "news"))

        topic_results = {topic["topic"]: [] for topic in cls._OSINT_TOPICS}

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            future_map = {
                executor.submit(cls._execute_serper_query, query, mode, 6): topic
                for topic, query, mode in search_jobs
            }
            for future, topic in future_map.items():
                try:
                    topic_results[topic].extend(future.result())
                except Exception as exc:
                    logger.warning("OSINT future failed for %s: %s", topic, exc)

        # --- DEEP SCRAPE THE #1 RESULT ---
        deep_insight_text = ""
        top_link = None
        for topic, entries in topic_results.items():
            if entries and entries[0].get("link"):
                top_link = entries[0]["link"]
                break
                
        if top_link:
            logger.info("Deep scraping OSINT for macro finance trends: %s", top_link)
            goal = "What are the latest macro trends regarding B2B payment behavior, budget cycles, or invoice collection challenges in Indonesia?"
            insight = cls.extract_insight_with_llm(top_link, goal)
            if insight:
                source_domain = urlparse(top_link).netloc.replace("www.", "")
                deep_insight_text = f"**Insight Mendalam (via {source_domain}):** {insight}\n\n"

        blocks = []
        for topic_config in cls._OSINT_TOPICS:
            topic_name = topic_config["topic"]
            unique_entries = cls._deduplicate(topic_results.get(topic_name, []))
            blocks.append(cls._format_topic(topic_name, unique_entries))

        combined = "\n\n".join(blocks).strip()
        if not combined:
            combined = "Tidak ada data OSINT eksternal yang dapat dipakai."

        return deep_insight_text + combined

    @classmethod
    @osint_cache.memoize(expire=86400)
    def get_chapter_signal(cls, chapter_keywords, notes=""):
        if not cls._is_serper_available():
            return "Sinyal OSINT per bab tidak tersedia."

        query = (
            "Indonesia payment behavior invoice collection risk "
            f"{chapter_keywords or ''} {notes or ''}"
        ).strip()

        results = cls._execute_serper_query(query, mode="search", num_results=5)
        unique_results = cls._deduplicate(results)
        if not unique_results:
            return "Tidak ada sinyal OSINT spesifik bab yang cukup relevan."

        lines = []
        for index, entry in enumerate(unique_results[:3], start=1):
            title = entry.get("title") or "Tanpa judul"
            snippet = entry.get("snippet") or "Tidak ada ringkasan."
            source = entry.get("domain") or "-"
            lines.append(f"{index}. {title}")
            lines.append(f"   Ringkasan: {snippet}")
            lines.append(f"   Sumber: {source}")

        return "\n".join(lines)

    @classmethod
    @osint_cache.memoize(expire=86400)
    def get_payment_delay_risks(cls, extra_context=""):
        if not cls._is_serper_available():
            return []

        context_snippet = (extra_context or "").strip()
        factors = []

        for topic in cls._DELAY_FACTOR_TOPICS:
            query = topic["query"]
            if context_snippet:
                query = f"{query} {context_snippet[:180]}"

            search_results = cls._execute_serper_query(query, mode="search", num_results=4)
            news_results = cls._execute_serper_query(query, mode="news", num_results=4)
            combined = cls._deduplicate(search_results + news_results)
            if not combined:
                continue

            sources = []
            snippets = []
            for item in combined[:2]:
                source = item.get("domain") or "-"
                if source not in sources:
                    sources.append(source)
                snippet = (item.get("snippet") or "").strip()
                if snippet:
                    snippets.append(snippet)

            factors.append(
                {
                    "factor": topic["factor"],
                    "potential_delay_days": {
                        "min": topic["delay_days"][0],
                        "max": topic["delay_days"][1],
                    },
                    "impact": topic["impact"],
                    "summary": " ".join(snippets[:2]).strip(),
                    "source_domains": sources,
                }
            )

        return factors


class StyleEngine:
    @staticmethod
    def _insert_field(paragraph, field_code, placeholder="1"):
        begin_run = paragraph.add_run()
        begin_field = OxmlElement("w:fldChar")
        begin_field.set(qn("w:fldCharType"), "begin")
        begin_run._r.append(begin_field)

        instruction_run = paragraph.add_run()
        instruction = OxmlElement("w:instrText")
        instruction.set(qn("xml:space"), "preserve")
        instruction.text = field_code
        instruction_run._r.append(instruction)

        separate_run = paragraph.add_run()
        separate_field = OxmlElement("w:fldChar")
        separate_field.set(qn("w:fldCharType"), "separate")
        separate_run._r.append(separate_field)

        paragraph.add_run(placeholder)

        end_run = paragraph.add_run()
        end_field = OxmlElement("w:fldChar")
        end_field.set(qn("w:fldCharType"), "end")
        end_run._r.append(end_field)

    @classmethod
    def insert_toc_field(cls, paragraph):
        cls._insert_field(
            paragraph,
            'TOC \\o "1-3" \\h \\z \\u',
            "Klik kanan lalu pilih Update Field untuk memuat daftar isi.",
        )

    @classmethod
    def apply_document_styles(cls, doc, theme_color):
        for section in doc.sections:
            section.top_margin = Cm(2.54)
            section.bottom_margin = Cm(2.54)
            section.left_margin = Cm(2.54)
            section.right_margin = Cm(2.54)

            header = section.header
            header_paragraph = (
                header.paragraphs[0] if header.paragraphs else header.add_paragraph()
            )
            header_paragraph.text = ""
            header_paragraph.alignment = WD_ALIGN_PARAGRAPH.LEFT
            header_paragraph.add_run("INIXINDO JOGJA | INTERNAL FINANCE REPORT")
            for run in header_paragraph.runs:
                run.font.name = "Calibri"
                run.font.size = Pt(8)
                run.font.color.rgb = RGBColor(120, 120, 120)

            footer = section.footer
            footer_paragraph = (
                footer.paragraphs[0] if footer.paragraphs else footer.add_paragraph()
            )
            footer_paragraph.text = ""
            footer_paragraph.alignment = WD_ALIGN_PARAGRAPH.RIGHT
            footer_paragraph.add_run("STRICTLY CONFIDENTIAL | Page ")
            cls._insert_field(footer_paragraph, "PAGE")
            footer_paragraph.add_run(" of ")
            cls._insert_field(footer_paragraph, "NUMPAGES")
            for run in footer_paragraph.runs:
                run.font.name = "Calibri"
                run.font.size = Pt(9)
                run.font.color.rgb = RGBColor(110, 110, 110)

        normal_style = doc.styles["Normal"]
        normal_style.font.name = "Calibri"
        normal_style.font.size = Pt(11)
        normal_style.font.color.rgb = RGBColor(33, 37, 41)
        normal_paragraph = normal_style.paragraph_format
        normal_paragraph.alignment = WD_ALIGN_PARAGRAPH.JUSTIFY
        normal_paragraph.line_spacing_rule = WD_LINE_SPACING.MULTIPLE
        normal_paragraph.line_spacing = 1.15
        normal_paragraph.space_after = Pt(8)

        heading_1 = doc.styles["Heading 1"]
        heading_1.font.name = "Calibri"
        heading_1.font.size = Pt(16)
        heading_1.font.bold = True
        heading_1.font.color.rgb = RGBColor(*theme_color)
        heading_1.paragraph_format.space_before = Pt(18)
        heading_1.paragraph_format.space_after = Pt(8)

        heading_2 = doc.styles["Heading 2"]
        heading_2.font.name = "Calibri"
        heading_2.font.size = Pt(13)
        heading_2.font.bold = True
        heading_2.font.color.rgb = RGBColor(0, 0, 0)
        heading_2.paragraph_format.space_before = Pt(14)
        heading_2.paragraph_format.space_after = Pt(4)

        heading_3 = doc.styles["Heading 3"]
        heading_3.font.name = "Calibri"
        heading_3.font.size = Pt(12)
        heading_3.font.bold = True
        heading_3.font.color.rgb = RGBColor(64, 64, 64)
        heading_3.paragraph_format.space_before = Pt(10)
        heading_3.paragraph_format.space_after = Pt(4)

        for style_name in [
            "List Bullet",
            "List Bullet 2",
            "List Bullet 3",
            "List Number",
            "List Number 2",
            "List Number 3",
        ]:
            try:
                list_style = doc.styles[style_name]
                list_style.font.name = "Calibri"
                list_style.font.size = Pt(11)
                list_style.paragraph_format.space_after = Pt(4)
            except KeyError:
                continue

        try:
            caption_style = doc.styles["Caption"]
            caption_style.font.name = "Calibri"
            caption_style.font.size = Pt(10)
            caption_style.font.italic = True
            caption_style.font.color.rgb = RGBColor(80, 80, 80)
        except KeyError:
            pass


class ChartEngine:
    @staticmethod
    def _theme_to_plt_color(theme_color):
        return tuple(component / 255 for component in theme_color)

    @staticmethod
    def create_bar_chart(data_str, theme_color):
        try:
            parts = [part.strip() for part in data_str.split("|")]
            if len(parts) >= 3:
                title = parts[0]
                y_label = parts[1]
                raw_data = "|".join(parts[2:])
            else:
                title = "Distribusi Historis Kelas Pembayaran"
                y_label = "Persentase"
                raw_data = data_str

            labels = []
            values = []
            for chunk in raw_data.split(";"):
                if "," not in chunk:
                    continue
                label, value = chunk.split(",", 1)
                numeric_value = re.sub(r"[^\d.]", "", value)
                if not numeric_value:
                    continue
                labels.append(label.strip())
                values.append(float(numeric_value))

            if not labels:
                return None

            fig, axis = plt.subplots(figsize=(7, 4.5))
            axis.bar(
                labels,
                values,
                color=ChartEngine._theme_to_plt_color(theme_color),
                alpha=0.9,
                width=0.5,
            )
            axis.set_title(title, fontsize=12, fontweight="bold", pad=20)
            axis.set_ylabel(y_label, fontsize=10)
            axis.spines["top"].set_visible(False)
            axis.spines["right"].set_visible(False)

            image_stream = io.BytesIO()
            plt.savefig(image_stream, format="png", bbox_inches="tight", dpi=150)
            plt.close(fig)
            image_stream.seek(0)
            return image_stream
        except Exception:
            return None

    @staticmethod
    def create_flowchart(data_str, theme_color):
        try:
            steps = [
                "\n".join(textwrap.wrap(step.strip(), width=18))
                for step in data_str.split("->")
                if step.strip()
            ]
            if len(steps) < 2:
                return None

            fig, axis = plt.subplots(figsize=(8, 3))
            axis.axis("off")
            x_positions = [index * 2.5 for index in range(len(steps))]

            for index in range(len(steps) - 1):
                axis.annotate(
                    "",
                    xy=(x_positions[index + 1] - 1.0, 0.5),
                    xytext=(x_positions[index] + 1.0, 0.5),
                    arrowprops={"arrowstyle": "-|>", "lw": 1.5},
                )

            for index, step in enumerate(steps):
                box = patches.FancyBboxPatch(
                    (x_positions[index] - 1.0, 0.1),
                    2.0,
                    0.8,
                    boxstyle="round,pad=0.1",
                    fc=ChartEngine._theme_to_plt_color(theme_color),
                    alpha=0.9,
                )
                axis.add_patch(box)
                axis.text(
                    x_positions[index],
                    0.5,
                    step,
                    ha="center",
                    va="center",
                    size=9,
                    color="white",
                    fontweight="bold",
                )

            axis.set_xlim(-1.2, (len(steps) - 1) * 2.5 + 1.2)
            axis.set_ylim(0, 1)

            image_stream = io.BytesIO()
            plt.savefig(
                image_stream,
                format="png",
                bbox_inches="tight",
                dpi=200,
                transparent=True,
            )
            plt.close(fig)
            image_stream.seek(0)
            return image_stream
        except Exception:
            return None


class DocumentBuilder:
    @staticmethod
    def _append_inline_text(paragraph, node, bold=False, italic=False, underline=False, monospace=False):
        if isinstance(node, NavigableString):
            text = str(node)
            if not text:
                return
            run = paragraph.add_run(text)
            run.bold = bold
            run.italic = italic
            run.underline = underline
            if monospace:
                run.font.name = "Consolas"
            return

        if not isinstance(node, Tag):
            return

        next_bold = bold or node.name in {"strong", "b"}
        next_italic = italic or node.name in {"em", "i"}
        next_underline = underline or node.name == "u"
        next_monospace = monospace or node.name == "code"

        if node.name == "br":
            paragraph.add_run("\n")
            return

        if node.name == "a":
            for child in node.children:
                DocumentBuilder._append_inline_text(
                    paragraph,
                    child,
                    bold=next_bold,
                    italic=next_italic,
                    underline=True,
                    monospace=next_monospace,
                )
            return

        for child in node.children:
            DocumentBuilder._append_inline_text(
                paragraph,
                child,
                bold=next_bold,
                italic=next_italic,
                underline=next_underline,
                monospace=next_monospace,
            )

    @staticmethod
    def _resolve_list_style(doc, ordered, level):
        ordered_styles = ["List Number", "List Number 2", "List Number 3"]
        bullet_styles = ["List Bullet", "List Bullet 2", "List Bullet 3"]
        style_names = ordered_styles if ordered else bullet_styles
        preferred_style = style_names[min(level, len(style_names) - 1)]

        try:
            doc.styles[preferred_style]
            return preferred_style
        except KeyError:
            return "List Number" if ordered else "List Bullet"

    @classmethod
    def _add_list(cls, doc, list_tag, level=0, ordered=False):
        style_name = cls._resolve_list_style(doc, ordered, level)

        for list_item in list_tag.find_all("li", recursive=False):
            inline_nodes = []
            nested_lists = []

            for child in list_item.children:
                if isinstance(child, Tag) and child.name in {"ul", "ol"}:
                    nested_lists.append(child)
                else:
                    inline_nodes.append(child)

            if inline_nodes:
                paragraph = doc.add_paragraph(style=style_name)
                paragraph.alignment = WD_ALIGN_PARAGRAPH.LEFT
                for node in inline_nodes:
                    cls._append_inline_text(paragraph, node)

            for nested_list in nested_lists:
                cls._add_list(
                    doc,
                    nested_list,
                    level=level + 1,
                    ordered=nested_list.name == "ol",
                )

    @staticmethod
    def _add_table(doc, table_tag):
        rows = table_tag.find_all("tr")
        if not rows:
            return

        header_cells = rows[0].find_all(["th", "td"])
        if not header_cells:
            return

        column_count = len(header_cells)
        table = doc.add_table(rows=1, cols=column_count)
        table.style = "Table Grid"

        for column_index, cell in enumerate(header_cells):
            paragraph = table.rows[0].cells[column_index].paragraphs[0]
            paragraph.text = cell.get_text(" ", strip=True)
            if paragraph.runs:
                paragraph.runs[0].bold = True

        for html_row in rows[1:]:
            html_cells = html_row.find_all(["th", "td"])
            table_cells = table.add_row().cells
            for column_index in range(column_count):
                value = ""
                if column_index < len(html_cells):
                    value = html_cells[column_index].get_text(" ", strip=True)
                table_cells[column_index].text = value

    @classmethod
    def parse_html_to_docx(cls, doc, html_content, theme_color):
        soup = BeautifulSoup(html_content, "html.parser")

        for element in soup.contents:
            if isinstance(element, NavigableString):
                if not str(element).strip():
                    continue
                paragraph = doc.add_paragraph(str(element).strip())
                paragraph.alignment = WD_ALIGN_PARAGRAPH.JUSTIFY
                continue

            if not isinstance(element, Tag):
                continue

            if element.name in {"h1", "h2", "h3", "h4"}:
                level = min(max(int(element.name[1]), 1), 3)
                heading = doc.add_heading(level=level)
                heading.alignment = WD_ALIGN_PARAGRAPH.LEFT
                cls._append_inline_text(heading, element)
                if level == 1:
                    for run in heading.runs:
                        run.font.color.rgb = RGBColor(*theme_color)
                continue

            if element.name == "p":
                paragraph = doc.add_paragraph()
                paragraph.alignment = WD_ALIGN_PARAGRAPH.JUSTIFY
                for child in element.children:
                    cls._append_inline_text(paragraph, child)
                continue

            if element.name in {"ul", "ol"}:
                cls._add_list(doc, element, level=0, ordered=element.name == "ol")
                continue

            if element.name == "table":
                cls._add_table(doc, element)

    @classmethod
    def _flush_markdown_block(cls, doc, lines, theme_color):
        if not lines:
            return

        markdown_text = "\n".join(lines).strip()
        if not markdown_text:
            return

        html_content = markdown.markdown(markdown_text, extensions=["tables"])
        cls.parse_html_to_docx(doc, html_content, theme_color)

    @staticmethod
    def _add_visual(doc, marker_type, marker_payload, theme_color):
        if marker_type == "CHART":
            image = ChartEngine.create_bar_chart(marker_payload, theme_color)
            width = Inches(5.8)
            caption = "Grafik distribusi historis kelas pembayaran"
        else:
            image = ChartEngine.create_flowchart(marker_payload, theme_color)
            width = Inches(6.3)
            caption = "Diagram alur rekomendasi mitigasi"

        if image is None:
            return

        image_paragraph = doc.add_paragraph()
        image_paragraph.alignment = WD_ALIGN_PARAGRAPH.CENTER
        image_paragraph.add_run().add_picture(image, width=width)

        try:
            caption_paragraph = doc.add_paragraph(caption, style="Caption")
        except KeyError:
            caption_paragraph = doc.add_paragraph(caption)
        caption_paragraph.alignment = WD_ALIGN_PARAGRAPH.CENTER

    @classmethod
    def process_content(cls, doc, raw_text, theme_color=DEFAULT_COLOR):
        markdown_buffer = []

        for raw_line in raw_text.splitlines():
            stripped_line = raw_line.strip()

            if stripped_line.startswith("[[CHART:") and stripped_line.endswith("]]"
            ):
                cls._flush_markdown_block(doc, markdown_buffer, theme_color)
                markdown_buffer = []
                payload = stripped_line.replace("[[CHART:", "", 1).rsplit("]]", 1)[0].strip()
                cls._add_visual(doc, "CHART", payload, theme_color)
                continue

            if stripped_line.startswith("[[FLOW:") and stripped_line.endswith("]]"
            ):
                cls._flush_markdown_block(doc, markdown_buffer, theme_color)
                markdown_buffer = []
                payload = stripped_line.replace("[[FLOW:", "", 1).rsplit("]]", 1)[0].strip()
                cls._add_visual(doc, "FLOW", payload, theme_color)
                continue

            markdown_buffer.append(raw_line.rstrip())

        cls._flush_markdown_block(doc, markdown_buffer, theme_color)

    @staticmethod
    def create_cover(doc, theme_color=DEFAULT_COLOR):
        StyleEngine.apply_document_styles(doc, theme_color)

        properties = doc.core_properties
        properties.title = "Inixindo Cash In Intelligence Report"
        properties.subject = "Internal Cash In Intelligence Report"
        properties.author = WRITER_FIRM_NAME
        properties.category = "Finance"

        for _ in range(4):
            doc.add_paragraph()

        confidentiality = doc.add_paragraph("STRICTLY CONFIDENTIAL")
        confidentiality.alignment = WD_ALIGN_PARAGRAPH.CENTER
        confidentiality.runs[0].font.name = "Calibri"
        confidentiality.runs[0].font.size = Pt(10)
        confidentiality.runs[0].font.bold = True
        confidentiality.runs[0].font.color.rgb = RGBColor(120, 120, 120)

        doc.add_paragraph()

        title = doc.add_paragraph("CASH IN INTELLIGENCE REPORT")
        title.alignment = WD_ALIGN_PARAGRAPH.CENTER
        title.runs[0].font.name = "Calibri"
        title.runs[0].font.size = Pt(22)
        title.runs[0].font.bold = True

        organization = doc.add_paragraph("INIXINDO JOGJA")
        organization.alignment = WD_ALIGN_PARAGRAPH.CENTER
        organization.runs[0].font.name = "Calibri"
        organization.runs[0].font.size = Pt(34)
        organization.runs[0].font.bold = True
        organization.runs[0].font.color.rgb = RGBColor(*theme_color)

        doc.add_paragraph()

        metadata_table = doc.add_table(rows=4, cols=2)
        metadata_table.style = "Table Grid"
        metadata = [
            ("Cakupan Data", "Seluruh histori invoice dan catatan penagihan"),
            ("Tipe Laporan", "Analisis deskriptif, diagnostik, prediktif, dan preskriptif cash in"),
            ("Tanggal Generasi", datetime.now().strftime("%d %B %Y")),
            ("Disusun Oleh", WRITER_FIRM_NAME),
        ]

        for row_index, (label, value) in enumerate(metadata):
            left_cell = metadata_table.rows[row_index].cells[0]
            right_cell = metadata_table.rows[row_index].cells[1]
            left_cell.text = label
            right_cell.text = value
            if left_cell.paragraphs[0].runs:
                left_cell.paragraphs[0].runs[0].bold = True

        doc.add_page_break()

    @staticmethod
    def add_table_of_contents(doc):
        doc.add_heading("Daftar Isi", level=1)
        toc_paragraph = doc.add_paragraph()
        StyleEngine.insert_toc_field(toc_paragraph)

        note = doc.add_paragraph(
            "Catatan: jika daftar isi belum muncul, klik kanan pada area daftar isi lalu pilih Update Field."
        )
        note.alignment = WD_ALIGN_PARAGRAPH.LEFT
        note.runs[0].italic = True
        note.runs[0].font.size = Pt(10)

        doc.add_page_break()


class ReportGenerator:
    SECTION_PASSES = (
        {
            "sections": REPORT_SECTION_SEQUENCE[:1],
            "include_visuals": False,
            "label": "executive_confidence",
        },
        {
            "sections": REPORT_SECTION_SEQUENCE[1:3],
            "include_visuals": True,
            "label": "diagnostic_evidence",
        },
        {
            "sections": REPORT_SECTION_SEQUENCE[3:],
            "include_visuals": False,
            "label": "actions_readiness",
        },
    )

    def __init__(self, kb_instance):
        self.ollama = Client(host=OLLAMA_HOST)
        self.kb = kb_instance
        self.io_pool = concurrent.futures.ThreadPoolExecutor(max_workers=4)

    @staticmethod
    def _build_user_instruction(notes, active_sections):
        notes = (notes or "").strip()
        report_sections = "\n".join(f"- {section}" for section in active_sections)
        focus_block = notes if notes else "Tidak ada fokus tambahan dari pengguna."
        return (
            "Susun laporan internal yang detail, profesional, dan siap dipakai sebagai bahan diskusi rapat manajemen.\n"
            "Jangan menulis seperti jawaban AI generik; tulis seperti memo manajemen yang berbasis data.\n"
            "Kerjakan hanya section yang diminta pada pass ini dan hentikan output setelah section terakhir selesai.\n"
            "Pastikan setiap bagian di bawah terisi secara jelas:\n"
            f"{report_sections}\n\n"
            "Setiap rekomendasi harus menyebutkan fokus tindakan dan dampak yang diharapkan.\n"
            f"Fokus pengguna:\n{focus_block}"
        )

    @staticmethod
    def _build_section_scope(active_sections):
        section_list = "\n".join(f"- {section}" for section in active_sections)
        section_headings = "\n".join(f"   # {section}" for section in active_sections)
        section_scope = (
            "Generate only the sections listed below for this pass.\n"
            "Start with the first heading listed and stop after the last heading listed.\n"
            "Do not repeat prior sections and do not preview later sections.\n"
            f"{section_list}"
        )
        return section_scope, section_headings

    def _build_report_prompt(self, report_context, notes, analysis_context, macro_osint, active_sections, include_visuals):
        persona = PERSONAS.get("default", "Chief Financial Officer")
        section_scope, section_headings = self._build_section_scope(active_sections)
        structured_context = self._format_structured_context_block(analysis_context)
        return FINANCE_SYSTEM_PROMPT.format(
            persona=persona,
            financial_summary=report_context["financial_summary"],
            management_brief=report_context["management_brief"],
            internal_evidence=report_context["evidence"],
            industry_trends=macro_osint,
            user_focus=(notes or "Tidak ada fokus tambahan."),
            cashflow_context=(structured_context or "Tidak ada konteks forecast terstruktur tambahan."),
            readiness_signals=report_context["readiness_signals"],
            section_scope=section_scope,
            section_headings=section_headings,
            visual_prompt=report_context["visual_prompt"] if include_visuals else "",
        )

    @staticmethod
    def _score_report_completeness(raw_text):
        report_text = str(raw_text or "")
        if not report_text.strip():
            return {
                "score": 0.0,
                "passed": False,
                "components": {},
                "missing": ["Dokumen kosong."],
            }

        components = {}
        missing = []

        required_headings = [f"# {section}" for section in REPORT_SECTION_SEQUENCE]
        present_headings = [heading for heading in required_headings if heading in report_text]
        heading_score = (len(present_headings) / len(required_headings)) * 30
        components["top_level_sections"] = round(heading_score, 1)
        if len(present_headings) < len(required_headings):
            missing_sections = [heading.replace("# ", "") for heading in required_headings if heading not in present_headings]
            missing.append(f"Bagian utama belum lengkap: {', '.join(missing_sections)}.")

        required_subheadings = [
            "### Dampak Bisnis",
            "### Tingkat Keyakinan dan Caveat",
            "### Batasan Data dan Asumsi",
            "### Konteks OSINT Pendukung",
            "### Risiko dan Kontrol",
            "### Prasyarat Implementasi",
            "### Kesiapan Pelaksanaan",
        ]
        present_subheadings = [subheading for subheading in required_subheadings if subheading in report_text]
        subheading_score = (len(present_subheadings) / len(required_subheadings)) * 20
        components["required_subsections"] = round(subheading_score, 1)
        if len(present_subheadings) < len(required_subheadings):
            missing_subsections = [subheading.replace("### ", "") for subheading in required_subheadings if subheading not in present_subheadings]
            missing.append(f"Subbagian wajib belum lengkap: {', '.join(missing_subsections)}.")

        table_score = 0
        scenario_table_pattern = re.compile(
            r"\|\s*Skenario\s*\|\s*Estimasi Realisasi Cash[ -]?In\s*\|\s*Gap terhadap Total Invoice\s*\|",
            re.IGNORECASE,
        )
        if scenario_table_pattern.search(report_text):
            table_score += 8
        else:
            missing.append("Tabel skenario cash-in belum ditemukan.")

        priority_table_pattern = re.compile(
            r"\|\s*Prioritas\s*\|\s*Fokus\s*\|\s*Penanggung Jawab\s*\|\s*Isu Utama\s*\|\s*Aksi 30 Hari\s*\|\s*Dampak yang Diharapkan\s*\|"
        )
        if priority_table_pattern.search(report_text):
            table_score += 12
        else:
            missing.append("Tabel prioritas 30 hari belum lengkap.")
        components["tables_and_owners"] = round(table_score, 1)

        enrichment_score = 0
        if "### Konteks OSINT Pendukung" in report_text:
            enrichment_score += 4
        if "[[CHART:" in report_text:
            enrichment_score += 3
        else:
            missing.append("Visual distribusi pembayaran belum masuk.")
        if "[[FLOW:" in report_text:
            enrichment_score += 3
        else:
            missing.append("Visual alur mitigasi belum masuk.")
        components["context_and_visuals"] = round(enrichment_score, 1)

        consistency_score = 10
        contradiction_patterns = [
            r"turun[^.\n]{0,120}memburuk",
            r"menurun[^.\n]{0,120}memburuk",
            r"naik[^.\n]{0,120}membaik",
            r"meningkat[^.\n]{0,120}membaik",
        ]
        if any(re.search(pattern, report_text, re.IGNORECASE) for pattern in contradiction_patterns):
            consistency_score = 0
            missing.append("Narasi tren risiko terdeteksi kontradiktif terhadap arah metrik.")
        components["numeric_consistency"] = round(consistency_score, 1)

        density_score = 0
        if len(report_text.strip()) >= 4500:
            density_score += 4
        elif len(report_text.strip()) >= 3200:
            density_score += 2
        else:
            missing.append("Narasi laporan masih terlalu tipis untuk bahan rapat internal.")

        if report_text.count("\n- ") >= 12:
            density_score += 3
        else:
            missing.append("Rincian bullet operasional masih kurang kaya.")

        if report_text.count("\n1.") >= 1 or report_text.count("\n1. ") >= 1:
            density_score += 1
        else:
            missing.append("Daftar tindakan bernomor belum kuat.")

        if "catatan" in report_text.lower() or "bukti" in report_text.lower():
            density_score += 2
        else:
            missing.append("Rujukan bukti internal belum cukup terlihat.")
        components["narrative_density"] = round(density_score, 1)

        total_score = round(sum(components.values()), 1)
        return {
            "score": total_score,
            "passed": total_score >= REPORT_MIN_COMPLETENESS_SCORE,
            "components": components,
            "missing": missing,
        }

    @staticmethod
    def _is_acceptable_report(raw_text):
        return ReportGenerator._score_report_completeness(raw_text)["passed"]

    @staticmethod
    def _extract_visual_markers(visual_prompt):
        chart_marker = ""
        flow_marker = ""
        for line in str(visual_prompt or "").splitlines():
            stripped_line = line.strip()
            if stripped_line.startswith("[[CHART:"):
                chart_marker = stripped_line
            elif stripped_line.startswith("[[FLOW:"):
                flow_marker = stripped_line
        return chart_marker, flow_marker

    @staticmethod
    def _split_top_level_sections(raw_text):
        matches = list(re.finditer(r"(?m)^# ([^\n]+?)\s*$", raw_text or ""))
        if not matches:
            return []

        sections = []
        for index, match in enumerate(matches):
            section_title = match.group(1).strip()
            section_end = matches[index + 1].start() if index + 1 < len(matches) else len(raw_text)
            section_body = raw_text[match.end():section_end].strip()
            sections.append({"title": section_title, "body": section_body})

        return sections

    @staticmethod
    def _join_top_level_sections(sections):
        blocks = []
        for section in sections:
            section_title = section["title"].strip()
            section_body = section["body"].strip()
            if section_body:
                blocks.append(f"# {section_title}\n{section_body}")
            else:
                blocks.append(f"# {section_title}")
        return "\n\n".join(blocks).strip()

    @staticmethod
    def _inject_subheading_block(section_body, subheading, content, before_subheading=None):
        if not content or not str(content).strip():
            return section_body

        subheading_marker = f"### {subheading}"
        if subheading_marker in section_body:
            return section_body

        new_block = f"{subheading_marker}\n{str(content).strip()}"
        if before_subheading:
            before_match = re.search(rf"(?m)^### {re.escape(before_subheading)}\s*$", section_body)
            if before_match:
                section_prefix = section_body[:before_match.start()].rstrip()
                section_suffix = section_body[before_match.start():].lstrip()
                return f"{section_prefix}\n\n{new_block}\n\n{section_suffix}".strip()

        return f"{section_body.rstrip()}\n\n{new_block}".strip()

    @staticmethod
    def _append_marker_block(section_body, marker):
        marker = str(marker or "").strip()
        if not marker or marker in section_body:
            return section_body
        return f"{section_body.rstrip()}\n\n{marker}".strip()

    @staticmethod
    def _format_structured_context_block(raw_text):
        lines = []
        for raw_line in str(raw_text or "").splitlines():
            cleaned_line = raw_line.strip()
            if not cleaned_line:
                if lines and lines[-1] != "":
                    lines.append("")
                continue
            if cleaned_line.startswith("===") and cleaned_line.endswith("==="):
                cleaned_line = cleaned_line.strip("= ").strip()
            if cleaned_line.endswith(":") and not cleaned_line.startswith("-"):
                lines.append(f"#### {cleaned_line[:-1].strip()}")
            else:
                lines.append(cleaned_line)
        return "\n".join(lines).strip()

    @classmethod
    def _sanitize_generated_report_text(cls, raw_text):
        sanitized = str(raw_text or "")
        sanitized = re.sub(r"(?<!\n)(===\s*[^\n=]+?\s*===)", r"\n\n\1", sanitized)
        sanitized = re.sub(r"\n?===\s*([^\n=]+?)\s*===\n?", lambda match: f"\n\n### {match.group(1).strip()}\n", sanitized)
        sanitized = re.sub(r"[ \t]+\n", "\n", sanitized)
        sanitized = re.sub(r"\n{3,}", "\n\n", sanitized)
        return sanitized.strip()

    def _finalize_report_content(self, raw_text, report_context, macro_osint):
        raw_text = self._sanitize_generated_report_text(raw_text)
        sections = self._split_top_level_sections(raw_text)
        if not sections:
            return raw_text

        chart_marker, flow_marker = self._extract_visual_markers(report_context.get("visual_prompt", ""))
        finalized_sections = []

        for section in sections:
            section_title = section["title"]
            section_body = section["body"]

            if section_title == "Analisis Deskriptif Cash In":
                section_body = self._append_marker_block(section_body, chart_marker)
            elif section_title == "Analisis Diagnostik":
                section_body = self._inject_subheading_block(
                    section_body,
                    "Konteks OSINT Pendukung",
                    macro_osint or "Tidak ada tren finansial eksternal yang tersedia.",
                    before_subheading="Risiko dan Kontrol",
                )
            elif section_title == "Rekomendasi Preskriptif":
                section_body = self._append_marker_block(section_body, flow_marker)

            finalized_sections.append({"title": section_title, "body": section_body})

        return self._join_top_level_sections(finalized_sections)

    def _build_fallback_report(self, report_context, notes, analysis_context, macro_osint):
        chart_marker, flow_marker = self._extract_visual_markers(report_context.get("visual_prompt", ""))
        focus_block = notes.strip() if notes and notes.strip() else "Tidak ada fokus tambahan dari pengguna."
        structured_context_block = self._format_structured_context_block(analysis_context)

        lines = [
            "# Ringkasan Eksekutif",
            "### Dampak Bisnis",
            "- Laporan ini digunakan untuk membantu manajemen membaca risiko cash in, memahami prioritas penagihan, dan menentukan tindakan yang paling cepat berdampak pada realisasi invoice.",
            "- Fokus pengguna tetap dijaga dalam interpretasi, namun narasi dan prioritas diturunkan dari bukti internal, pola historis, serta konteks pelaksanaan yang tersedia saat ini.",
            "",
            "## Fakta Eksekutif",
            report_context["executive_facts"],
            "",
            "### Tingkat Keyakinan dan Caveat",
            report_context["confidence_summary"],
        ]

        lines.extend(
            [
                "",
                "# Analisis Deskriptif Cash In",
                "### Snapshot Portofolio dan Konsentrasi Risiko",
                report_context["financial_summary"],
                "",
                "### Batasan Data dan Asumsi",
                report_context["assumptions"],
                "",
                "# Analisis Diagnostik",
                "### Pola Hambatan Utama",
                report_context["diagnostic_breakdown"],
                "",
                "### Bukti Internal yang Mewakili",
                report_context["evidence"],
                "",
                "### Konteks OSINT Pendukung",
                macro_osint or "Tidak ada tren eksternal yang tersedia.",
                "",
                "### Risiko dan Kontrol",
                report_context["controls"],
                "",
                "### Fokus Pengguna",
                f"- {focus_block}",
            ]
        )

        if structured_context_block:
            lines.extend(
                [
                    "",
                    "### Parameter Forecast dan Ruang Lingkup",
                    structured_context_block,
                ]
            )

        if chart_marker:
            lines.extend(["", chart_marker])

        lines.extend(
            [
                "",
                "# Analisis Prediktif",
                "### Dasar Proyeksi",
                "- Proyeksi menggunakan pendekatan risk-adjusted berdasarkan campuran kelas pembayaran historis, sehingga hasil harus dibaca sebagai skenario manajemen, bukan kepastian kas masuk.",
                "- Base case mewakili perilaku penagihan yang paling mungkin terjadi bila pola historis bertahan, sedangkan upside dan downside menunjukkan ruang perbaikan atau penurunan.",
                "",
                "### Skenario 1-2 Kuartal",
                report_context["scenario_table"],
                "",
                "### Implikasi terhadap Rencana Kas",
                report_context["cash_plan_implications"],
                "",
                "# Rekomendasi Preskriptif",
                "### Prinsip Tindakan",
                "1. Dahulukan invoice bernilai besar dengan skor risiko tinggi dan penyebab yang masih bisa dipulihkan dalam 30 hari.",
                "2. Pisahkan treatment untuk isu anggaran, approval, administrasi, likuiditas, dan sengketa agar collection effort tidak tersebar terlalu tipis.",
                "3. Gunakan bukti internal dan jadwal tindak lanjut yang terdokumentasi agar eskalasi ke manajemen klien lebih kuat.",
                "",
                "### Prasyarat Implementasi",
                report_context["implementation_prerequisites"],
                "",
                "### Kesiapan Pelaksanaan",
                report_context["organizational_readiness"],
                "",
                "# Prioritas Tindakan 30 Hari",
                "### Tabel Prioritas",
                report_context["priority_table"],
            ]
        )

        lines.extend(
            [
                "",
                "### Catatan Pelaksanaan",
                "- Tetapkan owner utama per akun prioritas dan review statusnya minimal mingguan.",
                "- Gunakan rapat internal untuk memastikan hambatan administratif dan eskalasi ke klien ditutup dengan tenggat yang jelas.",
                "- Konsolidasikan hasil follow-up ke finance collection, account owner, dan sponsor bisnis agar keputusan rapat langsung dapat dieksekusi.",
            ]
        )

        if flow_marker:
            lines.extend(["", flow_marker])

        return "\n".join(lines)

    def _run_generation_pass(self, report_context, notes, analysis_context, macro_osint, active_sections, include_visuals, label):
        prompt = self._build_report_prompt(
            report_context,
            notes,
            analysis_context,
            macro_osint,
            active_sections,
            include_visuals,
        )
        user_instruction = self._build_user_instruction(notes, active_sections)
        response = self.ollama.chat(
            model=LLM_MODEL,
            messages=[
                {"role": "system", "content": prompt},
                {"role": "user", "content": user_instruction},
            ],
            options={
                "num_ctx": REPORT_NUM_CTX,
                "num_predict": REPORT_NUM_PREDICT,
                "temperature": REPORT_TEMPERATURE,
                "top_p": REPORT_TOP_P,
                "repeat_penalty": REPORT_REPEAT_PENALTY,
            },
        )
        logger.info(
            "Generation pass %s completed with done_reason=%s, eval_count=%s.",
            label,
            response.get("done_reason"),
            response.get("eval_count"),
        )
        return response["message"]["content"]

    def run(self, notes="", analysis_context=""):
        logger.info("Starting cash-in intelligence report generation.")

        global_osint_future = self.io_pool.submit(Researcher.get_macro_finance_trends, notes)
        report_context = self.kb.get_report_context(notes)

        try:
            macro_osint = global_osint_future.result(timeout=45)
        except Exception:
            macro_osint = "Tidak ada tren finansial eksternal yang tersedia."

        fallback_used = False
        generated_sections = []
        for section_pass in self.SECTION_PASSES:
            generated_sections.append(
                self._run_generation_pass(
                    report_context,
                    notes,
                    analysis_context,
                    macro_osint,
                    section_pass["sections"],
                    section_pass["include_visuals"],
                    section_pass["label"],
                ).strip()
            )

        generated_content = "\n\n".join(section for section in generated_sections if section).strip()
        generated_content = self._finalize_report_content(generated_content, report_context, macro_osint)
        completeness_result = self._score_report_completeness(generated_content)
        logger.info(
            "Report completeness score %.1f/100 before fallback.",
            completeness_result["score"],
        )
        if not completeness_result["passed"]:
            logger.warning("Generated report failed quality gate. Falling back to deterministic management draft.")
            fallback_used = True
            generated_content = self._build_fallback_report(report_context, notes, analysis_context, macro_osint)
            generated_content = self._finalize_report_content(generated_content, report_context, macro_osint)
            completeness_result = self._score_report_completeness(generated_content)
            logger.info(
                "Report completeness score %.1f/100 after fallback.",
                completeness_result["score"],
            )

        document = Document()
        DocumentBuilder.create_cover(document, DEFAULT_COLOR)
        DocumentBuilder.add_table_of_contents(document)
        DocumentBuilder.process_content(
            document,
            generated_content,
            DEFAULT_COLOR,
        )

        run_metadata = {
            "fallback_used": fallback_used,
            "quality_gate_passed": completeness_result["passed"],
            "completeness_score": completeness_result["score"],
            "completeness_missing": completeness_result["missing"],
            "osint_available": bool(
                macro_osint
                and "tidak tersedia" not in macro_osint.lower()
                and "tidak ada data osint" not in macro_osint.lower()
            ),
            "visuals_included": "[[CHART:" in generated_content and "[[FLOW:" in generated_content,
            "report_length": len(generated_content),
        }

        return document, "Inixindo_Cash_In_Intelligence_Report", run_metadata
