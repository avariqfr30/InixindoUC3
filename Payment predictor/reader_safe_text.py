import re

_REPLACEMENTS = (
    (r"\bNama\s+Perusahaan\s+Klien\s*:", "catatan klien:"),
    (r"\bReferenceAccount\s+mencatat\b", "catatan klien menunjukkan"),
    (r"\bsource\s*=\s*(?:https?://\S+|/api/[A-Za-z0-9_./-]+)?", ""),
    (r"\bdataset[_\s-]*code\s*=\s*ConsultantProjectExpertHistory\b", "riwayat pengalaman konsultan"),
    (r"\bConsultantProjectExpertHistory\b", "riwayat pengalaman konsultan"),
    (r"\bDirangkum\s+dari\s+sumber[^:]*:\s*", "Berdasarkan catatan pendukung yang sudah dipadatkan: "),
    (r"\bProblem\s*,\s*Opportunity\s*,\s*Directive\b", "kebutuhan prioritas yang perlu dipertegas"),
    (r"\bReferenceAccount\b", "catatan klien"),
    (r"\bPain\s+Points\b", "titik masalah"),
    (r"\bExecutive Summary\b", "Ringkasan Eksekutif"),
    (r"\bBLUF\b", "Inti Keputusan"),
    (r"\bKey Findings\b", "Temuan Utama"),
    (r"\bRecommendation\b", "Rekomendasi"),
    (r"\bRecommendations\b", "Rekomendasi"),
    (r"\bHeadline\b", "Sorotan"),
    (r"\bHeadlines\b", "Sorotan"),
    (r"\bInsight\b", "Wawasan"),
    (r"\bInsights\b", "Wawasan"),
    (r"\bCaveat\b", "Catatan Batasan"),
    (r"\bCaveats\b", "Catatan Batasan"),
    (r"\bDriver\b", "Faktor"),
    (r"\bDrivers\b", "Faktor"),
    (r"\bTiming\b", "Jadwal"),
    (r"\bForecast\b", "Proyeksi"),
    (r"\bOwner\b", "Penanggung Jawab"),
    (r"\bReview\b", "Tinjauan"),
    (r"\bCash discipline\b", "Disiplin kas"),
    (r"\bcash discipline\b", "disiplin kas"),
    (r"\bcash out\b", "arus kas keluar"),
    (r"\bending cash\b", "saldo kas akhir"),
    (r"\brunway\b", "ketahanan kas"),
    (r"\bcoverage ratio\b", "rasio cakupan"),
    (r"\bDeterministic cashflow projection\b", "Proyeksi arus kas deterministik"),
    (r"\bnot a statistical guarantee\b", "bukan jaminan statistik"),
    (r"\bChallenge Check\b", "Uji Kewajaran"),
    (r"\bSensitivity\b", "Sensitivitas"),
    (r"\binvoice\b", "tagihan"),
    (r"\binvoices\b", "tagihan"),
    (r"Visual Dashboard Snapshot", "Cuplikan Dasbor Operasional"),
    (r"Snapshot Dashboard Operasional", "Cuplikan Dasbor Operasional"),
    (r"Dashboard Snapshot", "Cuplikan Dasbor"),
    (r"(?<!\[\[)\bDashboard\b(?!:)", "Dasbor"),
    (r"API internal perusahaan", "data operasional yang tersedia"),
    (r"API internal", "data operasional yang tersedia"),
    (r"Internal API", "data operasional yang tersedia"),
    (r"APIDog", "sistem data operasional"),
    (r"dataset demo lokal", "dataset simulasi"),
    (r"demo mode", "mode simulasi"),
    (r"source-of-truth internal", "sistem data utama"),
    (r"source-of-truth", "sistem data utama"),
    (r"evidence ledger", "ringkasan bukti"),
    (r"Invoice Evidence Analyst", "penelaah bukti invoice"),
    (r"Control Reviewer", "penelaah kontrol"),
    (r"Executive Editor", "penyunting eksekutif"),
    (r"sync status", "status kesiapan data"),
    (r"record aktif", "catatan aktif"),
    (r"\bagent\b", "tim analisis"),
    (r"\bdesk\b", "proses analisis"),
    (r"\bworkflow\b", "alur kerja"),
)
_FORBIDDEN_TECH = r"\b(endpoint|schema|Waitress|queue|thread|runtime)\b"
_MARKER_RE = re.compile(r"\[\[.*?\]\]", re.DOTALL)


def reader_safe_text(raw_text):
    text = str(raw_text or "")

    # Preserve [[CHART:...]] and [[FLOW:...]] markers from regex mangling
    markers = []
    def _preserve(match):
        markers.append(match.group(0))
        return f"__PRESERVED_MARKER_{len(markers) - 1}__"
    text = _MARKER_RE.sub(_preserve, text)

    for pattern, replacement in _REPLACEMENTS:
        text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)
    text = re.sub(_FORBIDDEN_TECH, "kesiapan operasional", text, flags=re.IGNORECASE)

    # Restore preserved markers
    for i, marker in enumerate(markers):
        text = text.replace(f"__PRESERVED_MARKER_{i}__", marker)

    return "\n".join(re.sub(r"[ \t]+", " ", line).rstrip() for line in text.splitlines()).strip()
