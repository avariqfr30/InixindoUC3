# config.py
import os

DEMO_MODE = True 

SERPER_API_KEY = "masukkan_api_key_serper_anda_disini"

OLLAMA_HOST = "http://127.0.0.1:11434"

LLM_MODEL = "gpt-oss:120b-cloud" 
EMBED_MODEL = "bge-m3:latest"
DB_URI = "sqlite:///data/finance_predictor.db" 

WRITER_FIRM_NAME = "Inixindo Jogja - Finance & Revenue Optimization Division" 
DEFAULT_COLOR = (204, 0, 0) # Inixindo Jogja Red

SMART_SUGGESTIONS = [
    "Lakukan analisis historis komprehensif terhadap seluruh portofolio piutang perusahaan dan identifikasi tren sistemik penyebab keterlambatan secara makro.",
    "Evaluasi dampak agregat jangka panjang dari seluruh invoice yang tertunda (Kelas C, D, E) terhadap likuiditas Inixindo.",
    "Susun rancangan kebijakan penagihan (Company-Wide Collection Policy) yang terstandardisasi berdasarkan pola historis klien.",
    "Berdasarkan seluruh data historis, prediksi rasio pergeseran invoice dari metrik lancar (Kelas A/B) menjadi berisiko (Kelas D/E) untuk masa depan."
]

FINANCE_SYSTEM_PROMPT = """
You are the Chief Financial Officer (CFO) and Lead Financial Data Scientist for Inixindo Jogja.
ROLE: {persona}.

=== ALL-TIME HISTORICAL INVOICE & PAYMENT DATA ===
{rag_data}

=== EXTERNAL OSINT BENCHMARKS (MACRO ECONOMIC TRENDS) ===
Corporate & Gov Budget Cycles in Indonesia: {industry_trends}

MANDATORY RULES:
1. STRICT LANGUAGE: Write the entire response strictly in professional, corporate Bahasa Indonesia.
2. HISTORICAL & OVERALL SYNTHESIS: You must analyze the aggregate historical financial health of the ENTIRE company across ALL time periods provided. Look at all partner classes (Class A to E) to predict future revenue bottlenecks.
3. STRICT SUB-CHAPTER ENFORCEMENT: You MUST use Markdown Headers (###) for EVERY single sub-chapter listed below. You are FORBIDDEN from leaving any sub-chapter empty. Write at least 150 words per sub-chapter.
4. EVIDENCE: Quote anonymized historical notes from the data to explain systemic bottlenecks.
5. NO TITLE REPETITION: Do NOT write '{chapter_title}' at the start of your response.
6. {visual_prompt}

WRITE DETAILED CONTENT FOR THE FOLLOWING SUB-CHAPTERS:
{sub_chapters}
"""

FINANCE_STRUCTURE = [
    {
        "id": "fin_chap_1", "title": "BAB I – OVERALL HISTORICAL CASH FLOW & INVOICE SUMMARY",
        "subs": [
            "1.1 Kondisi Historis Kesehatan Piutang & Arus Kas Perusahaan", 
            "1.2 Distribusi Keseluruhan Kelas Pembayaran Partner (Kelas A hingga Kelas E)", 
            "1.3 Identifikasi Anomali Makro Sepanjang Sejarah Penagihan"
        ],
        "keywords": "overall historical company cash flow revenue invoice late payment class aggregate",
        "visual_intent": "bar_chart",
        "length_intent": "Highly concise, data-driven executive summary."
    },
    {
        "id": "fin_chap_2", "title": "BAB II – COMPANY-WIDE PAYMENT BEHAVIOR & BOTTLENECK ANALYSIS",
        "subs": [
            "2.1 Perbandingan Pola Pembayaran Lintas Demografi Secara Historis (Pemerintah vs BUMN vs Swasta)", 
            "2.2 Akar Masalah Sistemik Keterlambatan Jangka Panjang",
            "2.3 Kutipan Bukti Historis (Evidence dari Catatan Penagihan)"
        ],
        "keywords": "historical systemic payment behavior bottleneck bureaucracy delay reasons",
        "length_intent": "Detailed root-cause analysis. Quote the data explicitly."
    },
    {
        "id": "fin_chap_3", "title": "BAB III – AGGREGATE REVENUE PREDICTION & RISK ASSESSMENT",
        "subs": [
            "3.1 Prediksi Pergeseran Kelas di Masa Depan (Berdasarkan pola historis)", 
            "3.2 Dampak Siklus Anggaran Eksternal Terhadap Likuiditas Perusahaan", 
            "3.3 Identifikasi Segmen Pasar Paling Berisiko Secara Jangka Panjang"
        ],
        "keywords": "predict long-term aggregate risk shift class downgrade total liquidity",
        "length_intent": "Objective, highly analytical, predictive."
    },
    {
        "id": "fin_chap_4", "title": "BAB IV – STRATEGIC COLLECTION & INCOME OPTIMIZATION",
        "subs": [
            "4.1 Kebijakan Penagihan Standar Perusahaan (Berdasarkan evaluasi kelemahan historis)", 
            "4.2 Rekomendasi Mitigasi Likuiditas Lintas Sektor", 
            "4.3 Langkah Hukum dan Eskalasi Final untuk Piutang Kelas E"
        ],
        "keywords": "recommendation company-wide collection policy SOP optimize revenue legal action",
        "visual_intent": "flowchart",
        "length_intent": "Actionable, clear, and structured using bullet points."
    }
]

PERSONAS = {
    "default": "Chief Financial Officer (Highly analytical, risk-averse, strategic, focusing on company-wide macro metrics and historical patterns)"
}