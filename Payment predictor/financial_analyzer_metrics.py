import re

from forecast_engine import parse_idr_amount


class FinancialAnalyzerMetricsMixin:
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
