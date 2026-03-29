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
from urllib.parse import urljoin, urlparse

import chromadb
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
from ollama import Client
from sqlalchemy import create_engine

from config import (
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
    REPORT_NUM_PREDICT,
    REPORT_REPEAT_PENALTY,
    REPORT_SECTION_SEQUENCE,
    REPORT_TEMPERATURE,
    REPORT_TOP_P,
    SERPER_API_KEY,
    WRITER_FIRM_NAME,
)

matplotlib.use("Agg")
logger = logging.getLogger(__name__)


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
                self.report_context_cache = FinancialAnalyzer.build_report_context(self.df)

        context = dict(self.report_context_cache)
        notes = (notes or "").strip()
        if notes:
            focused_evidence = self.query(notes, max_results=10) or context["evidence"]
            context["evidence"] = FinancialAnalyzer.normalize_evidence_text(focused_evidence)
        return context


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
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return 0
        digits = re.sub(r"[^\d]", "", str(value))
        return int(digits) if digits else 0

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
    def normalize_evidence_text(raw_text):
        lines = []
        chunks = [chunk.strip() for chunk in str(raw_text or "").split("\n---\n") if chunk.strip()]

        if len(chunks) > 1 or any("Periode Laporan:" in chunk for chunk in chunks):
            for chunk in chunks[:10]:
                single_line = re.sub(r"\s+", " ", chunk).strip()
                lines.append(f"- {single_line}")
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
    def build_report_context(cls, df):
        if df is None or df.empty:
            return {
                "financial_summary": "Tidak ada data finansial internal yang tersedia.",
                "evidence": "Tidak ada catatan historis yang tersedia.",
                "management_brief": "Tidak ada management brief yang dapat disusun dari data kosong.",
                "executive_facts": "- Tidak ada fakta eksekutif yang tersedia.",
                "scenario_table": "| Skenario | Estimasi Realisasi Cash In | Gap terhadap Total Invoice | Narasi Manajemen |\n|---|---:|---:|---|\n| Base Case | Rp 0 | Rp 0 | Data kosong. |",
                "priority_table": "| Prioritas | Fokus | Isu Utama | Aksi 30 Hari | Dampak yang Diharapkan |\n|---:|---|---|---|---|\n| 1 | Tidak ada data | - | Lengkapi data terlebih dahulu. | Memberi dasar analisis yang layak. |",
                "meeting_agenda": "1. Pastikan data internal tersedia sebelum rapat dilanjutkan.",
                "visual_prompt": "Do not force visuals.",
            }

        working_df = df.copy()
        period_column = cls._find_column(working_df, "period")
        partner_column = cls._find_column(working_df, "partner")
        service_column = cls._find_column(working_df, "service")
        payment_class_column = cls._find_column(working_df, "payment_class")
        invoice_value_column = cls._find_column(working_df, "invoice_value")
        notes_column = cls._find_column(working_df, "notes")

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
                f"- {row['__period']} | {row['__partner']} | {row['__service']} | {row['__payment_class']} | {cls._format_currency(int(row['__invoice_value']))} | {trimmed_note}"
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
            "| Prioritas | Fokus | Isu Utama | Aksi 30 Hari | Dampak yang Diharapkan |",
            "|---:|---|---|---|---|",
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
                f"| {item['priority']} | {item['focus']} | {item['issue']} | {item['action']} | {item['impact']} |"
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
            "management_brief": "\n".join(management_brief_lines),
            "executive_facts": "\n".join(executive_fact_lines),
            "scenario_table": "\n".join(scenario_lines),
            "priority_table": "\n".join(priority_table_lines),
            "meeting_agenda": "\n".join(agenda_lines),
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

    _cache = {}

    @staticmethod
    def _is_serper_available():
        return bool(
            SERPER_API_KEY
            and SERPER_API_KEY.strip()
            and SERPER_API_KEY != "masukkan_api_key_serper_anda_disini"
        )

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
    def get_macro_finance_trends(cls, extra_context=""):
        if not cls._is_serper_available():
            return "Data OSINT eksternal tidak tersedia (SERPER_API_KEY belum dikonfigurasi)."

        context_snippet = (extra_context or "").strip()
        cache_key = context_snippet.lower()
        if cache_key in cls._cache:
            return cls._cache[cache_key]

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

        blocks = []
        for topic_config in cls._OSINT_TOPICS:
            topic_name = topic_config["topic"]
            unique_entries = cls._deduplicate(topic_results.get(topic_name, []))
            blocks.append(cls._format_topic(topic_name, unique_entries))

        combined = "\n\n".join(blocks).strip()
        if not combined:
            combined = "Tidak ada data OSINT eksternal yang dapat dipakai."

        cls._cache[cache_key] = combined
        return combined

    @classmethod
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
            "sections": REPORT_SECTION_SEQUENCE[:3],
            "include_visuals": True,
            "label": "executive_overview",
        },
        {
            "sections": REPORT_SECTION_SEQUENCE[3:],
            "include_visuals": False,
            "label": "forward_actions",
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

    def _build_report_prompt(self, report_context, notes, macro_osint, active_sections, include_visuals):
        persona = PERSONAS.get("default", "Chief Financial Officer")
        section_scope, section_headings = self._build_section_scope(active_sections)
        return FINANCE_SYSTEM_PROMPT.format(
            persona=persona,
            financial_summary=report_context["financial_summary"],
            management_brief=report_context["management_brief"],
            internal_evidence=report_context["evidence"],
            industry_trends=macro_osint,
            user_focus=(notes or "Tidak ada fokus tambahan."),
            section_scope=section_scope,
            section_headings=section_headings,
            visual_prompt=report_context["visual_prompt"] if include_visuals else "",
        )

    @staticmethod
    def _is_acceptable_report(raw_text):
        if not raw_text or len(raw_text.strip()) < 3200:
            return False

        required_headings = [f"# {section}" for section in REPORT_SECTION_SEQUENCE]
        if any(heading not in raw_text for heading in required_headings):
            return False

        table_header_pattern = re.compile(
            r"\|\s*Prioritas\s*\|\s*Fokus\s*\|\s*Isu Utama\s*\|\s*Aksi 30 Hari\s*\|\s*Dampak yang Diharapkan\s*\|"
        )
        if not table_header_pattern.search(raw_text):
            return False

        return True

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

    def _build_fallback_report(self, report_context, notes, macro_osint):
        chart_marker, flow_marker = self._extract_visual_markers(report_context.get("visual_prompt", ""))
        focus_block = notes.strip() if notes and notes.strip() else "Tidak ada fokus tambahan dari pengguna."

        lines = [
            "# Ringkasan Eksekutif",
            "### Kesimpulan Utama",
            "- Laporan ini menempatkan data invoice internal dan catatan penagihan sebagai sumber utama untuk membaca kualitas cash in, tingkat keterlambatan, dan urgensi tindak lanjut.",
            "- Fokus pengguna tetap dijaga dalam interpretasi, namun angka dan prioritas diturunkan dari bukti internal dan pola historis yang tersedia.",
            "",
            "## Fakta Eksekutif yang Wajib Dijaga Konsisten",
            report_context["executive_facts"],
            "",
            "## Agenda Diskusi Manajemen",
            report_context["meeting_agenda"],
        ]

        if chart_marker:
            lines.extend(["", chart_marker])

        lines.extend(
            [
                "",
                "# Analisis Deskriptif Cash In",
                "### Snapshot Portofolio dan Konsentrasi Risiko",
                report_context["financial_summary"],
                "",
                "### Fokus Pengguna",
                f"- {focus_block}",
                "",
                "# Analisis Diagnostik",
                "### Bukti Internal yang Mewakili",
                report_context["evidence"],
                "",
                "### Konteks OSINT Pendukung",
                macro_osint or "Tidak ada tren eksternal yang tersedia.",
                "",
                "### Implikasi Diagnostik",
                "- Hambatan penagihan perlu dibaca per segmen partner, bukan diperlakukan sebagai satu masalah yang sama untuk seluruh portofolio.",
                "- Invoice berisiko tinggi perlu dipisahkan menurut akar masalah dominan: siklus anggaran, approval, administrasi, likuiditas, atau sengketa.",
                "",
                "# Analisis Prediktif",
                "### Dasar Proyeksi",
                "- Proyeksi menggunakan pendekatan risk-adjusted berdasarkan campuran kelas pembayaran historis, sehingga hasil harus dibaca sebagai skenario manajemen, bukan kepastian kas masuk.",
                "- Base case mewakili perilaku penagihan yang paling mungkin terjadi bila pola historis bertahan, sedangkan upside dan downside menunjukkan ruang perbaikan atau penurunan.",
                "",
                "### Skenario 1-2 Kuartal",
                report_context["scenario_table"],
                "",
                "### Trigger Peringatan Dini",
                "1. Kenaikan porsi invoice Kelas D/E pada partner dengan nilai invoice besar.",
                "2. Penumpukan hambatan dokumen, approval, atau pencairan anggaran pada periode terbaru.",
                "3. Munculnya bukti likuiditas pelanggan yang semakin lemah atau dispute yang belum ditutup.",
                "",
                "# Rekomendasi Preskriptif",
                "### Prinsip Tindakan",
                "1. Dahulukan invoice bernilai besar dengan skor risiko tinggi dan penyebab yang masih bisa dipulihkan dalam 30 hari.",
                "2. Pisahkan treatment untuk isu anggaran, approval, administrasi, likuiditas, dan sengketa agar collection effort tidak tersebar terlalu tipis.",
                "3. Gunakan bukti internal dan jadwal tindak lanjut yang terdokumentasi agar eskalasi ke manajemen klien lebih kuat.",
                "",
                "### Rekomendasi Detail",
                report_context["executive_facts"],
                "",
                "### Agenda Keputusan Internal",
                report_context["meeting_agenda"],
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
            ]
        )

        if flow_marker:
            lines.extend(["", flow_marker])

        return "\n".join(lines)

    def _run_generation_pass(self, report_context, notes, macro_osint, active_sections, include_visuals, label):
        prompt = self._build_report_prompt(
            report_context,
            notes,
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

    def run(self, notes=""):
        logger.info("Starting cash-in intelligence report generation.")

        global_osint_future = self.io_pool.submit(Researcher.get_macro_finance_trends, notes)
        report_context = self.kb.get_report_context(notes)

        try:
            macro_osint = global_osint_future.result(timeout=25)
        except Exception:
            macro_osint = "Tidak ada tren finansial eksternal yang tersedia."

        generated_sections = []
        for section_pass in self.SECTION_PASSES:
            generated_sections.append(
                self._run_generation_pass(
                    report_context,
                    notes,
                    macro_osint,
                    section_pass["sections"],
                    section_pass["include_visuals"],
                    section_pass["label"],
                ).strip()
            )

        generated_content = "\n\n".join(section for section in generated_sections if section).strip()
        if not self._is_acceptable_report(generated_content):
            logger.warning("Generated report failed quality gate. Falling back to deterministic management draft.")
            generated_content = self._build_fallback_report(report_context, notes, macro_osint)

        document = Document()
        DocumentBuilder.create_cover(document, DEFAULT_COLOR)
        DocumentBuilder.add_table_of_contents(document)
        DocumentBuilder.process_content(
            document,
            generated_content,
            DEFAULT_COLOR,
        )

        return document, "Inixindo_Cash_In_Intelligence_Report"
