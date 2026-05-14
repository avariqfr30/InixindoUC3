import re

_REPLACEMENTS = (
    (r"API internal perusahaan", "data operasional yang tersedia"),
    (r"Internal API", "data operasional yang tersedia"),
    (r"dataset demo lokal", "dataset simulasi"),
    (r"source-of-truth internal", "sistem data utama"),
    (r"source-of-truth", "sistem data utama"),
    (r"sync status", "status kesiapan data"),
    (r"record aktif", "catatan aktif"),
)
_FORBIDDEN_TECH = r"\b(endpoint|schema|Waitress|queue|thread|runtime)\b"


def reader_safe_text(raw_text):
    text = str(raw_text or "")
    for pattern, replacement in _REPLACEMENTS:
        text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)
    text = re.sub(_FORBIDDEN_TECH, "kesiapan operasional", text, flags=re.IGNORECASE)
    return "\n".join(re.sub(r"[ \t]+", " ", line).rstrip() for line in text.splitlines()).strip()
