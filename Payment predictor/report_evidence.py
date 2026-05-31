import re

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
