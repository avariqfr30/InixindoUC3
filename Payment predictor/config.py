import os

DATA_ACQUISITION_MODE = os.getenv("DATA_ACQUISITION_MODE", "demo").strip().lower()
SERPER_API_KEY = os.getenv("SERPER_API_KEY", "masukkan_api_key_serper_anda_disini")
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://127.0.0.1:11434")
LLM_MODEL = os.getenv("LLM_MODEL", "gpt-oss:120b-cloud")
EMBED_MODEL = os.getenv("EMBED_MODEL", "bge-m3:latest")
DB_URI = os.getenv("DB_URI", "sqlite:///data/finance_predictor.db")

INTERNAL_API_BASE_URL = os.getenv("INTERNAL_API_BASE_URL", "").strip()
INTERNAL_API_DATASET_PATH = os.getenv(
    "INTERNAL_API_DATASET_PATH",
    "/api/finance/invoices",
).strip()
INTERNAL_API_RECORDS_KEY = os.getenv("INTERNAL_API_RECORDS_KEY", "").strip()
INTERNAL_API_AUTH_TOKEN = os.getenv("INTERNAL_API_AUTH_TOKEN", "").strip()
INTERNAL_API_HEADERS_JSON = os.getenv("INTERNAL_API_HEADERS_JSON", "{}").strip()
INTERNAL_API_QUERY_PARAMS_JSON = os.getenv("INTERNAL_API_QUERY_PARAMS_JSON", "{}").strip()
INTERNAL_API_TIMEOUT = int(os.getenv("INTERNAL_API_TIMEOUT", "20"))
INTERNAL_API_VERIFY_SSL = os.getenv("INTERNAL_API_VERIFY_SSL", "true").strip().lower() not in {
    "0",
    "false",
    "no",
}

WRITER_FIRM_NAME = "Inixindo Jogja - Finance & Revenue Optimization Division"
DEFAULT_COLOR = (204, 0, 0)

SMART_SUGGESTIONS = [
    "Lakukan analisis historis menyeluruh atas portofolio piutang dan petakan penyebab sistemik keterlambatan pembayaran.",
    "Ukur dampak agregat invoice tertunda (Kelas C, D, E) terhadap likuiditas dan arus kas operasional.",
    "Susun kebijakan penagihan lintas unit berbasis pola historis kelas pembayaran.",
    "Prediksi potensi pergeseran invoice dari Kelas A/B menuju Kelas D/E untuk horizon 2-4 kuartal ke depan.",
]

FINANCE_SYSTEM_PROMPT = """
You are the Chief Financial Officer (CFO) and Lead Financial Data Scientist for Inixindo Jogja.
ROLE: {persona}

=== INTERNAL HISTORICAL INVOICE & PAYMENT DATA ===
{rag_data}

=== EXTERNAL OSINT BENCHMARKS (INDONESIA) ===
{industry_trends}

MANDATORY RULES:
1. Write the full response in professional corporate Bahasa Indonesia.
2. Analyze aggregate historical performance of the entire company across all available periods.
3. Compare partner/payment classes A-E and explain forward-looking revenue bottlenecks.
4. You MUST use Markdown header `###` for every requested sub-chapter below.
5. Every sub-chapter must be substantive and include clear evidence from historical records.
6. Use numbered lists for priority actions and bullet lists for operational details when relevant.
7. Use concise markdown tables for direct comparisons when they improve clarity.
8. Do not repeat '{chapter_title}' as the opening title in the response.
9. {visual_prompt}

WRITE DETAILED CONTENT FOR THESE SUB-CHAPTERS:
{sub_chapters}
"""

FINANCE_STRUCTURE = [
    {
        "id": "fin_chap_1",
        "title": "BAB I – OVERALL HISTORICAL CASH FLOW & INVOICE SUMMARY",
        "subsections": [
            "1.1 Kondisi Historis Kesehatan Piutang & Arus Kas Perusahaan",
            "1.2 Distribusi Keseluruhan Kelas Pembayaran Partner (Kelas A hingga Kelas E)",
            "1.3 Identifikasi Anomali Makro Sepanjang Sejarah Penagihan",
        ],
        "keywords": "overall historical company cash flow revenue invoice late payment class aggregate",
        "visual_intent": "bar_chart",
    },
    {
        "id": "fin_chap_2",
        "title": "BAB II – COMPANY-WIDE PAYMENT BEHAVIOR & BOTTLENECK ANALYSIS",
        "subsections": [
            "2.1 Perbandingan Pola Pembayaran Lintas Demografi Secara Historis (Pemerintah vs BUMN vs Swasta)",
            "2.2 Akar Masalah Sistemik Keterlambatan Jangka Panjang",
            "2.3 Kutipan Bukti Historis (Evidence dari Catatan Penagihan)",
        ],
        "keywords": "historical systemic payment behavior bottleneck bureaucracy delay reasons",
    },
    {
        "id": "fin_chap_3",
        "title": "BAB III – AGGREGATE REVENUE PREDICTION & RISK ASSESSMENT",
        "subsections": [
            "3.1 Prediksi Pergeseran Kelas di Masa Depan (Berdasarkan pola historis)",
            "3.2 Dampak Siklus Anggaran Eksternal terhadap Likuiditas Perusahaan",
            "3.3 Identifikasi Segmen Pasar Paling Berisiko Secara Jangka Panjang",
        ],
        "keywords": "predict long-term aggregate risk shift class downgrade total liquidity",
    },
    {
        "id": "fin_chap_4",
        "title": "BAB IV – STRATEGIC COLLECTION & INCOME OPTIMIZATION",
        "subsections": [
            "4.1 Kebijakan Penagihan Standar Perusahaan (Berdasarkan evaluasi kelemahan historis)",
            "4.2 Rekomendasi Mitigasi Likuiditas Lintas Sektor",
            "4.3 Langkah Hukum dan Eskalasi Final untuk Piutang Kelas E",
        ],
        "keywords": "recommendation company-wide collection policy SOP optimize revenue legal action",
        "visual_intent": "flowchart",
    },
]

PERSONAS = {
    "default": (
        "Chief Financial Officer with a conservative risk profile, "
        "strong governance focus, and data-driven decision-making discipline"
    )
}
