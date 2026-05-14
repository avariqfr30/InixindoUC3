from dataclasses import dataclass


@dataclass(frozen=True)
class ReportStructure:
    section_sequence: tuple
    section_passes: tuple
    required_subheadings: tuple
    required_tables: dict


REPORT_STRUCTURE = ReportStructure(
    section_sequence=(
        "Ringkasan Eksekutif",
        "Analisis Deskriptif Cashflow",
        "Analisis Diagnostik Cashflow",
        "Analisis Prediktif Cashflow",
        "Rekomendasi Preskriptif",
        "Prioritas Tindakan 30 Hari",
    ),
    section_passes=(
        {
            "sections": ("Ringkasan Eksekutif",),
            "include_visuals": False,
            "label": "executive_confidence",
        },
        {
            "sections": (
                "Analisis Deskriptif Cashflow",
                "Analisis Diagnostik Cashflow",
            ),
            "include_visuals": True,
            "label": "diagnostic_evidence",
        },
        {
            "sections": (
                "Analisis Prediktif Cashflow",
                "Rekomendasi Preskriptif",
                "Prioritas Tindakan 30 Hari",
            ),
            "include_visuals": False,
            "label": "actions_readiness",
        },
    ),
    required_subheadings=(
        "### Dampak Bisnis",
        "### Tingkat Keyakinan dan Caveat",
        "### Batasan Data dan Asumsi",
        "### Konteks OSINT Pendukung",
        "### Risiko dan Kontrol",
        "### Prasyarat Implementasi",
        "### Kesiapan Pelaksanaan",
    ),
    required_tables={
        "scenario_cash_in": (
            r"\|\s*Skenario\s*\|\s*Estimasi Arus Kas Masuk\s*\|"
            r"\s*Gap terhadap Total Invoice\s*\|"
        ),
        "priority_30_day": (
            r"\|\s*Prioritas\s*\|\s*Fokus\s*\|\s*Penanggung Jawab\s*\|"
            r"\s*Isu Utama\s*\|\s*Aksi 30 Hari\s*\|\s*Dampak yang Diharapkan\s*\|"
        ),
    },
)
