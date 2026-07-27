"""Bounded feedback policy for UC3 report generation."""
import hashlib
import hmac
import json

from adaptive_feedback import FeedbackStore, build_feedback_policy
from config import LEARNING_FEEDBACK_DB_PATH

UC3_FEEDBACK_POLICY = build_feedback_policy("uc3_cashflow", {
    "hasil_membantu": {"label": "Laporan membantu", "guidance": "", "mode": "positive"},
    "ringkasan_kurang_jelas": {"label": "Ringkasan kurang jelas", "guidance": "Buat ringkasan risiko kas dan implikasi keputusan lebih langsung dan mudah dipindai.", "mode": "adapt"},
    "bahasa_terlalu_teknis": {"label": "Bahasa terlalu teknis", "guidance": "Gunakan Bahasa Indonesia yang lebih sederhana tanpa mengubah angka, formula, bukti, atau makna finansial.", "mode": "adapt"},
    "tindakan_kurang_praktis": {"label": "Tindakan kurang praktis", "guidance": "Jelaskan tindakan, pemilik, waktu, dan dampak kas yang diharapkan secara lebih operasional.", "mode": "adapt"},
    "fokus_risiko_kurang_sesuai": {"label": "Fokus risiko kurang sesuai", "guidance": "Prioritaskan risiko kas yang paling relevan dengan konteks pengguna tanpa mengubah perhitungan atau sumber data.", "mode": "adapt"},
    "angka_perlu_diperiksa": {"label": "Data, angka, atau formula perlu diperiksa", "guidance": "", "mode": "review"},
})

def create_feedback_store():
    return FeedbackStore(LEARNING_FEEDBACK_DB_PATH, UC3_FEEDBACK_POLICY)


def dashboard_snapshot_id(owner_key, sync_status, cash_on_hand, start_date, monthly_cost, secret_key):
    financial = (sync_status or {}).get("financialData") or {}
    cash_out = (sync_status or {}).get("cashOutSource") or {}
    payload = {
        "owner": str(owner_key or "").strip().casefold(),
        "financial_data_version": str(financial.get("dataVersion") or ""),
        "cash_out_version": str(cash_out.get("version") or ""),
        "cash_on_hand": int(cash_on_hand),
        "start_date": str(start_date or ""),
        "monthly_cost": int(monthly_cost),
    }
    encoded = json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode("utf-8")
    digest = hmac.new(str(secret_key or "").encode("utf-8"), encoded, hashlib.sha256).hexdigest()
    return f"dashboard:{digest}"
