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
REPORT_MAX_PENDING_JOBS = int(os.getenv("REPORT_MAX_PENDING_JOBS", "12"))
REPORT_JOB_RETENTION_SECONDS = int(os.getenv("REPORT_JOB_RETENTION_SECONDS", "1800"))
REPORT_STATUS_POLL_INTERVAL_MS = int(os.getenv("REPORT_STATUS_POLL_INTERVAL_MS", "1500"))
REPORT_METRICS_WINDOW_HOURS = int(os.getenv("REPORT_METRICS_WINDOW_HOURS", "24"))
REPORT_ARTIFACTS_DIR = os.getenv(
    "REPORT_ARTIFACTS_DIR",
    os.path.join(DATA_DIR, "generated_reports"),
)
REPORT_MIN_COMPLETENESS_SCORE = float(os.getenv("REPORT_MIN_COMPLETENESS_SCORE", "80"))
JOB_STATE_DB_PATH = os.getenv(
    "JOB_STATE_DB_PATH",
    os.path.join(DATA_DIR, "report_jobs.db"),
)
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
    "Sorot batasan data, asumsi kerja, dan tingkat keyakinan analisis agar laporan siap dibahas manajemen.",
    "Jelaskan risiko kontrol, penanggung jawab utama, dan prasyarat implementasi agar tindak lanjut bisa dijalankan.",
]

FINANCE_SYSTEM_PROMPT = """
You are the Chief Financial Officer (CFO) and Lead Financial Data Scientist for Inixindo Jogja.
ROLE: {persona}

OBJECTIVE:
Create one professional internal cash-in intelligence report for management discussion.
The report must be detailed enough for an internal meeting, numerically consistent, and action-oriented.

=== INTERNAL CASH-IN SUMMARY ===
{financial_summary}

=== MANAGEMENT ANALYSIS BRIEF (USE THESE FACTS) ===
{management_brief}

=== INTERNAL EVIDENCE NOTES ===
{internal_evidence}

=== EXTERNAL OSINT CONTEXT (INDONESIA) ===
{industry_trends}

=== USER FOCUS ===
{user_focus}

=== STRUCTURED CASHFLOW FORECAST CONTEXT ===
{cashflow_context}

=== CONFIDENCE, CONTROL, AND EXECUTION SIGNALS ===
{readiness_signals}

=== ACTIVE SECTION SCOPE ===
{section_scope}

MANDATORY RULES:
1. Write the full response in professional but easy-to-understand Bahasa Indonesia.
2. Keep the analysis focused on cash-in behavior, collection patterns, and invoice realization risk.
3. Use these exact top-level Markdown headings in order for this pass:
{section_headings}
4. This must read like a management memo, not a generic AI answer.
5. Use `###` sub-headings inside sections to separate portfolio snapshot, concentration, trend, root causes, scenarios, and actions.
6. Use numbered lists for action priorities and bullet lists for operational details.
7. Use concise Markdown tables when they improve comparison, especially for scenario views and 30-day priorities.
8. Cite internal evidence naturally by paraphrasing or quoting short anonymized note fragments.
9. Treat OSINT as supporting context, never as the source of truth for internal financial facts.
10. Never contradict the provided metrics. If risk score decreases, describe it as improving or lower risk. If risk score increases, describe it as worsening or higher risk.
11. Never copy internal delimiter labels or raw machine blocks verbatim. If source context contains separators, debug-like markers, or structured forecast notes, rewrite them into natural business prose, tables, or bullets.
12. The report must explicitly cover:
   - current cash-in condition,
   - main collection bottlenecks,
   - short-term forecast or scenarios,
   - management implications,
   - concrete next actions.
13. If this pass includes `# Ringkasan Eksekutif`, include `### Dampak Bisnis` and `### Tingkat Keyakinan dan Caveat`.
14. If this pass includes `# Analisis Deskriptif Cash In`, include `### Snapshot Portofolio dan Konsentrasi Risiko` and `### Batasan Data dan Asumsi`.
15. If this pass includes `# Analisis Diagnostik`, include `### Pola Hambatan Utama`, `### Bukti Internal yang Mewakili`, `### Konteks OSINT Pendukung`, and `### Risiko dan Kontrol`.
16. In `# Analisis Diagnostik`, split the explanation by what drives the delay, for example process/document issues, budget/approval issues, and liquidity/relationship issues, instead of dumping one long block of mixed evidence.
17. If this pass includes `# Analisis Prediktif`, include `### Dasar Proyeksi`, `### Skenario 1-2 Kuartal`, and `### Implikasi terhadap Rencana Kas`.
18. If this pass includes `# Rekomendasi Preskriptif`, include `### Prinsip Tindakan`, `### Prasyarat Implementasi`, and `### Kesiapan Pelaksanaan`.
19. If this pass includes `# Prioritas Tindakan 30 Hari`, include a Markdown table with columns:
   `Prioritas | Fokus | Penanggung Jawab | Isu Utama | Aksi 30 Hari | Dampak yang Diharapkan`
20. Avoid vague phrases such as `perlu perhatian lebih` unless followed by a specific action and expected impact.
21. Do not add an introduction before `# Ringkasan Eksekutif`.
22. If `### Konteks OSINT Pendukung` is included, summarize external signals in business language and mention source domains naturally.
23. If visual markers are provided, reproduce them verbatim on standalone lines in the most relevant section and do not modify the marker syntax.
24. If the user focus or forecast context includes structured cashflow forecast inputs, explicitly weave in:
   - selected period window,
   - current cash on hand,
   - estimated payments by character, retention, and satisfaction,
   - total outstanding by age and payment character,
   - external factors that may delay payment,
   - short-, mid-, and long-term implications.
25. Do not expose the word `aman` as a visible label unless the source context explicitly asks for it; instead, describe the operating buffer, ending cash, and implications in normal business language.
26. If an internal cashflow health model is present, use it silently to sharpen the analysis around:
   - liquidity and operational runway,
   - stability/predictability of cash in timing,
   - speed of invoice-to-cash conversion,
   - cash coverage versus outflow,
   - concentration and overdue risk.
   Also reflect the three internal readiness checks in normal business language:
   - whether cash is available now,
   - whether incoming cash timing is clear enough,
   - whether cashflow risk is under control.
   Do not present these as a branded framework or maturity scorecard.

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
