# config.py
import os

DEMO_MODE = True 

GOOGLE_API_KEY = "API_KEY"
GOOGLE_CX_ID = "CX_ID"

OLLAMA_HOST = "http://127.0.0.1:11434"

LLM_MODEL = "gpt-oss:120b-cloud" 
EMBED_MODEL = "bge-m3:latest"
DB_URI = "sqlite:///data/finance_predictor.db" 

WRITER_FIRM_NAME = "Inixindo Jogja - Finance & Revenue Optimization Division" 
DEFAULT_COLOR = (204, 0, 0) # Inixindo Jogja Red

# --- SMART SUGGESTIONS (Finance & Cash Flow Level) ---
SMART_SUGGESTIONS = [
    "Tolong analisis risiko arus kas (cash flow) spesifik dari klien Instansi Pemerintah di awal tahun.",
    "Berikan prediksi persentase invoice yang kemungkinan akan jatuh ke Kelas D atau E bulan depan.",
    "Susun rekomendasi skema penagihan agresif untuk klien di Kelas C agar tidak turun ke Kelas D.",
    "Bandingkan perilaku pembayaran (payment behavior) antara BUMN dengan perusahaan Swasta."
]

FINANCE_SYSTEM_PROMPT = """
You are the Chief Financial Officer (CFO) and Lead Financial Data Scientist for Inixindo Jogja.
ROLE: {persona}.

=== INVOICE & PAYMENT BEHAVIOR DATA (TIMEFRAME: {timeframe}) ===
{rag_data}

=== EXTERNAL OSINT BENCHMARKS (MACRO ECONOMIC TRENDS) ===
Corporate & Gov Budget Cycles in Indonesia: {industry_trends}

MANDATORY RULES:
1. STRICT LANGUAGE: Write the entire response strictly in professional, corporate Bahasa Indonesia.
2. HOLISTIC FINANCIAL SYNTHESIS: You must analyze the payment behaviors of partners. They are classed as: Class A (On time), Class B (1-2 Weeks Late), Class C (1-2 Months Late), Class D (3-6 Months Late), Class E (>6 Months Late).
3. STRICT SUB-CHAPTER ENFORCEMENT: You MUST use Markdown Headers (###) for EVERY single sub-chapter listed below. You are FORBIDDEN from leaving any sub-chapter empty. Write at least 150 words per sub-chapter.
4. EVIDENCE: Quote anonymized historical notes from the data to explain WHY payments are late.
5. NO TITLE REPETITION: Do NOT write '{chapter_title}' at the start of your response.
6. {visual_prompt}

WRITE DETAILED CONTENT FOR THE FOLLOWING SUB-CHAPTERS:
{sub_chapters}
"""

FINANCE_STRUCTURE = [
    {
        "id": "fin_chap_1", "title": "BAB I – EXECUTIVE CASH FLOW & INVOICE SUMMARY",
        "subs": [
            "1.1 Kondisi Kesehatan Piutang & Arus Kas Secara Keseluruhan", 
            "1.2 Distribusi Kelas Pembayaran Partner (Kelas A hingga Kelas E)", 
            "1.3 Identifikasi Anomali atau Lonjakan Keterlambatan Pembayaran"
        ],
        "keywords": "overall cash flow revenue invoice late payment class A B C D E",
        "visual_intent": "bar_chart",
        "length_intent": "Highly concise, data-driven executive summary."
    },
    {
        "id": "fin_chap_2", "title": "BAB II – PAYMENT BEHAVIOR & BOTTLENECK ANALYSIS",
        "subs": [
            "2.1 Analisis Perilaku Pembayaran Berdasarkan Demografi (Pemerintah vs BUMN vs Swasta)", 
            "2.2 Akar Masalah Utama Keterlambatan (Birokrasi, Transisi Sistem, Masalah Internal Klien)",
            "2.3 Kutipan Bukti Historis (Evidence dari Catatan Penagihan)"
        ],
        "keywords": "payment behavior bottleneck bureaucracy budget cycle delay reasons",
        "length_intent": "Detailed root-cause analysis. Quote the data explicitly."
    },
    {
        "id": "fin_chap_3", "title": "BAB III – REVENUE PREDICTION & RISK ASSESSMENT",
        "subs": [
            "3.1 Prediksi Pergeseran Kelas (Risiko partner Kelas B/C turun menjadi Kelas D/E)", 
            "3.2 Dampak Siklus Anggaran Eksternal (OSINT Tren Ekonomi & APBN/APBD)", 
            "3.3 Identifikasi Segmen Paling Berisiko Terhadap Likuiditas Perusahaan"
        ],
        "keywords": "predict risk shift class downgrade liquidity budget trend",
        "length_intent": "Objective, highly analytical, predictive."
    },
    {
        "id": "fin_chap_4", "title": "BAB IV – STRATEGIC COLLECTION & INCOME OPTIMIZATION",
        "subs": [
            "4.1 Strategi Optimalisasi Penagihan (Diskon pelunasan awal, pengetatan SLA, denda)", 
            "4.2 Rekomendasi Mitigasi Khusus untuk Klien Pemerintah & BUMN", 
            "4.3 Langkah Hukum atau Eskalasi untuk Klien Kelas E (>6 Bulan)"
        ],
        "keywords": "recommendation collection strategy optimize revenue legal action SLA penalty",
        "visual_intent": "flowchart",
        "length_intent": "Actionable, clear, and structured using bullet points."
    }
]

PERSONAS = {
    "default": "Chief Financial Officer (Highly analytical, risk-averse, strategic, numbers-driven)"
}