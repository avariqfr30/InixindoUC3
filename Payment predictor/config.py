import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

DATA_ACQUISITION_MODE = os.getenv("DATA_ACQUISITION_MODE", "demo").strip().lower()
APP_SERVER = os.getenv("APP_SERVER", "flask").strip().lower()
APP_HOST = os.getenv("APP_HOST", "127.0.0.1").strip()
APP_PORT = int(os.getenv("APP_PORT", "5000"))
APP_DEBUG = os.getenv("APP_DEBUG", "false").strip().lower() in {"1", "true", "yes"}
WAITRESS_THREADS = int(os.getenv("WAITRESS_THREADS", "12"))
WAITRESS_CONNECTION_LIMIT = int(os.getenv("WAITRESS_CONNECTION_LIMIT", "100"))
WAITRESS_CHANNEL_TIMEOUT = int(os.getenv("WAITRESS_CHANNEL_TIMEOUT", "120"))
SERPER_API_KEY = os.getenv("SERPER_API_KEY", "masukkan_api_key_serper_anda_disini")
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://127.0.0.1:11434")
LLM_MODEL = os.getenv("LLM_MODEL", "gpt-oss:120b-cloud")
EMBED_MODEL = os.getenv("EMBED_MODEL", "bge-m3:latest")
DB_URI = os.getenv("DB_URI", f"sqlite:///{os.path.join(DATA_DIR, 'finance_predictor.db')}")
DEMO_CSV_PATH = os.getenv("DEMO_CSV_PATH", os.path.join(DATA_DIR, "db.csv"))
REPORT_MAX_CONCURRENT_JOBS = int(os.getenv("REPORT_MAX_CONCURRENT_JOBS", "4"))
REPORT_JOB_RETENTION_SECONDS = int(os.getenv("REPORT_JOB_RETENTION_SECONDS", "1800"))
REPORT_STATUS_POLL_INTERVAL_MS = int(os.getenv("REPORT_STATUS_POLL_INTERVAL_MS", "1500"))
REPORT_NUM_CTX = int(os.getenv("REPORT_NUM_CTX", "24576"))
REPORT_NUM_PREDICT = int(os.getenv("REPORT_NUM_PREDICT", "2200"))
REPORT_TEMPERATURE = float(os.getenv("REPORT_TEMPERATURE", "0.2"))
REPORT_TOP_P = float(os.getenv("REPORT_TOP_P", "0.85"))
REPORT_REPEAT_PENALTY = float(os.getenv("REPORT_REPEAT_PENALTY", "1.1"))

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
    "Jelaskan kondisi cash in saat ini dari karakter penagihan partner dan faktor yang paling memengaruhi keterlambatan.",
    "Diagnosa akar masalah cash in berdasarkan pola kelas pembayaran, jenis partner, dan catatan historis penagihan.",
    "Prediksi risiko penurunan cash in 1-2 kuartal ke depan dan tandai segmen partner yang perlu diawasi.",
    "Berikan rekomendasi tindakan prioritas untuk mempercepat cash in dan menurunkan invoice berisiko.",
]

FINANCE_SYSTEM_PROMPT = """
You are the Chief Financial Officer (CFO) and Lead Financial Data Scientist for Inixindo Jogja.
ROLE: {persona}

OBJECTIVE:
Create one practical cash-in intelligence report that helps business users understand collection behavior,
diagnose causes of delayed cash-in, predict short-term risk, and decide what to do next.

=== INTERNAL CASH-IN SUMMARY ===
{financial_summary}

=== INTERNAL EVIDENCE NOTES ===
{internal_evidence}

=== EXTERNAL OSINT CONTEXT (INDONESIA) ===
{industry_trends}

=== USER FOCUS ===
{user_focus}

MANDATORY RULES:
1. Write the full response in professional but easy-to-understand Bahasa Indonesia.
2. Keep the analysis focused on cash-in behavior, collection patterns, and invoice realization risk.
3. Use these exact top-level Markdown headings in order:
   # Ringkasan Eksekutif
   # Analisis Deskriptif Cash In
   # Analisis Diagnostik
   # Analisis Prediktif
   # Rekomendasi Preskriptif
   # Prioritas Tindakan 30 Hari
4. Use `###` sub-headings inside sections when needed.
5. Use numbered lists for action priorities and bullet lists for operational details.
6. Use concise Markdown tables only when they make comparison clearer.
7. Cite internal evidence naturally by paraphrasing or quoting short anonymized note fragments.
8. Treat OSINT as supporting context, never as the source of truth for internal financial facts.
9. Include the provided visual markers exactly as supplied when they are present.
10. Do not add an introduction before `# Ringkasan Eksekutif`.

VISUAL MARKERS:
{visual_prompt}
"""

REPORT_SECTION_SEQUENCE = [
    "Ringkasan Eksekutif",
    "Analisis Deskriptif Cash In",
    "Analisis Diagnostik",
    "Analisis Prediktif",
    "Rekomendasi Preskriptif",
    "Prioritas Tindakan 30 Hari",
]

PERSONAS = {
    "default": (
        "Chief Financial Officer with a conservative risk profile, "
        "strong governance focus, and data-driven decision-making discipline"
    )
}
