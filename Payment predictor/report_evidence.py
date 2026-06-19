import re

from editorial_intelligence import DATASET_ROLES, payment_voice_rules
from reader_safe_text import reader_safe_text


HIDDEN_TERMS = [
    "Invoice Evidence Analyst",
    "Control Reviewer",
    "Executive Editor",
    "agent",
    "workflow",
    "endpoint",
    "Internal API",
    "APIDog",
]


class PaymentEvidenceBuilder:
    CONFIDENCE_PHRASES = {
        "high": "Dasar bukti kuat",
        "tinggi": "Dasar bukti kuat",
        "medium": "Dasar bukti cukup",
        "sedang": "Dasar bukti cukup",
        "low": "Dasar bukti terbatas",
        "rendah": "Dasar bukti terbatas",
    }

    @staticmethod
    def clean(value, max_words=28):
        text = reader_safe_text(str(value or ""))
        for term in HIDDEN_TERMS:
            text = re.sub(re.escape(term), "", text, flags=re.IGNORECASE)
        text = re.sub(r"https?://\S+", "", text)
        text = re.sub(r"\s+", " ", text).strip(" -;,.")
        words = text.split()
        if len(words) > max_words:
            text = " ".join(words[:max_words]).rstrip(" ,;:") + "."
        return text

    @classmethod
    def insight_cards(cls, ledger, financial_summary="", management_brief="", osint_dossier=None, limit=6):
        cards = []
        for item in ledger or []:
            claim = cls.clean(item.get("claim") or item.get("detail") or item.get("finding"), max_words=24)
            allowed = cls.clean(item.get("allowed_use") or item.get("allowed_usage") or "", max_words=20)
            confidence = str(item.get("confidence") or "sedang").strip().lower()
            if not claim:
                continue
            cards.append({
                "observation": claim,
                "implication": allowed or "Bukti ini perlu dibaca sebagai sinyal prioritas cashflow.",
                "recommended_angle": "Mulai dari angka, jelaskan risiko kas, lalu tutup dengan tindakan koleksi atau kontrol.",
                "confidence": confidence,
            })
            if len(cards) >= limit:
                break
        summary = cls.clean(financial_summary or management_brief, max_words=30)
        if summary:
            cards.insert(0, {
                "observation": summary,
                "implication": "Ringkasan dashboard harus menjadi pusat cerita laporan.",
                "recommended_angle": "Jadikan dasbor sebagai sumber keputusan hari ini, bukan lampiran narasi.",
                "confidence": "high",
                "dataset_roles": DATASET_ROLES,
                "voice_rules": payment_voice_rules(),
            })
        dossier = osint_dossier if isinstance(osint_dossier, dict) else {}
        for card in (dossier.get("evidence_cards") or [])[:2]:
            if not isinstance(card, dict):
                continue
            claim = cls.clean(card.get("claim"), max_words=24)
            why = cls.clean(card.get("why_it_matters"), max_words=20)
            if claim:
                cards.append({
                    "observation": claim,
                    "implication": why or "OSINT hanya dipakai sebagai konteks eksternal pendukung.",
                    "recommended_angle": "Gunakan sebagai konteks waktu atau tekanan eksternal, bukan sebagai fakta invoice.",
                    "confidence": "medium",
                })
        return cards[:limit]

    @classmethod
    def to_markdown(cls, ledger, limit=4):
        rows = []
        for item in ledger or []:
            claim = cls.clean(item.get("claim") or item.get("detail") or item.get("finding"))
            allowed_use = cls.clean(item.get("allowed_use") or item.get("allowed_usage") or "Dipakai sebagai bukti pendukung.")
            confidence = cls.CONFIDENCE_PHRASES.get(str(item.get("confidence") or "Sedang").strip().lower(), "Dasar bukti cukup")
            if claim:
                rows.append(f"- {claim}. {allowed_use}. {confidence}.")
            if len(rows) >= limit:
                break
        if not rows:
            return ""
        return "### Bukti yang Dipakai\n" + "\n".join(rows)
