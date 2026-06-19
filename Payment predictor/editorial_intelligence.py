"""Payment-specific dashboard voice and editorial quality helpers."""
from __future__ import annotations

import re
from collections import Counter
from typing import Any


EXCLUDED_DATASETS = {"FinanceInvoice", "ProjectStandards"}

DATASET_ROLES = {
    "InvoiceTraining": "cash-in training invoice behavior",
    "InvoiceConsultant": "cash-in consulting invoice behavior",
    "ReferenceAccount": "partner/account context",
    "BankDisbursement": "cash-out dashboard context",
}


def compact_finance_text(value: Any, max_words: int = 14) -> str:
    words = re.sub(r"\s+", " ", str(value or "").strip()).split()
    if len(words) <= max_words:
        return " ".join(words)
    return " ".join(words[:max_words]).rstrip(".,;:") + "."


def payment_voice_rules() -> list[str]:
    return [
        "Tulis seperti memo CFO untuk menindaklanjuti dashboard: langsung pada angka, risiko, keputusan, dan owner.",
        "Jangan mengulang formula dashboard sebagai paragraf panjang.",
        "Jika data tidak menunjukkan dominasi jelas, katakan tidak dominan; jangan memaksa nama layanan.",
        "Tabel prioritas hanya memuat keputusan cepat; penjelasan diletakkan di bawah tabel.",
        "Jika OSINT tidak tersedia, laporan tetap kuat karena invoice dan dashboard adalah sumber utama.",
    ]


def compact_repeated_finance_cells(rows: list[list[Any]] | None, max_cell_words: int = 14) -> list[list[str]]:
    counts: Counter[str] = Counter()
    output: list[list[str]] = []
    for row in rows or []:
        next_row: list[str] = []
        for cell in row:
            text = compact_finance_text(cell, max_cell_words)
            signature = text.lower()
            counts[signature] += 1
            if counts[signature] > 2 and len(text.split()) >= 3:
                text = ""
            next_row.append(text)
        output.append(next_row)
    return output


def assess_payment_style(text: Any) -> dict[str, Any]:
    normalized = re.sub(r"\s+", " ", str(text or "").lower())
    paragraphs = [part.strip() for part in re.split(r"\n\s*\n|(?<=[.!?])\s+", str(text or "")) if part.strip()]
    openings = Counter(" ".join(re.findall(r"[a-z0-9]+", part.lower())[:2]) for part in paragraphs)
    findings: list[str] = []
    if normalized.count("dashboard") >= 2 and normalized.count("dihitung") >= 2:
        findings.append("dashboard_restatement")
    if any(count >= 3 and opening for opening, count in openings.items()):
        findings.append("repeated_openings")
    return {"passed": not findings, "findings": findings}


PAYMENT_CONNECTOR_TERMS = (
    "melanjutkan", "menjadi dasar", "berangkat dari", "menghubungkan",
    "arah berikutnya", "diterjemahkan menjadi", "dibaca sebagai kelanjutan",
    "dari posisi", "rangkaian cashflow", "kaitan dengan", "dari sini",
    "pembahasan kemudian", "dasar tersebut",
)
PAYMENT_SECTION_ORDER = (
    "Ringkasan Eksekutif",
    "Analisis Deskriptif Cashflow",
    "Analisis Diagnostik Cashflow",
    "Analisis Prediktif Cashflow",
    "Rekomendasi Preskriptif",
)
PAYMENT_SECTION_CONTEXTS = {
    "Ringkasan Eksekutif": "Pada tingkat eksekutif",
    "Analisis Deskriptif Cashflow": "Dalam profil arus kas",
    "Analisis Diagnostik Cashflow": "Dari sisi penyebab",
    "Analisis Prediktif Cashflow": "Pada horizon proyeksi",
    "Rekomendasi Preskriptif": "Dalam rencana tindak lanjut",
}


def _plain_document_text(value: Any) -> str:
    text = re.sub(r"\[\[(?:CHART|PIE|FLOW|DASHBOARD):.*?\]\]", " ", str(value or ""))
    text = re.sub(r"[#*`>|_]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _split_payment_sections(raw_text: Any) -> list[dict[str, str]]:
    sections: list[dict[str, str]] = []
    current_title: str | None = None
    current_body: list[str] = []
    for line in str(raw_text or "").splitlines():
        match = re.match(r"^#\s+(.+?)\s*$", line)
        if match:
            if current_title is not None:
                sections.append({"title": current_title, "body": "\n".join(current_body).strip()})
            current_title = match.group(1).strip()
            current_body = []
        else:
            current_body.append(line)
    if current_title is not None:
        sections.append({"title": current_title, "body": "\n".join(current_body).strip()})
    return sections


def _join_payment_sections(sections: list[dict[str, str]]) -> str:
    return "\n\n".join(f"# {section['title']}\n{section['body'].strip()}".strip() for section in sections).strip()


def _has_connector(text: Any, *titles: Any) -> bool:
    lowered = _plain_document_text(text).lower()
    if any(term in lowered for term in PAYMENT_CONNECTOR_TERMS):
        return True
    return any(str(title or "").strip().lower() in lowered for title in titles if str(title or "").strip())


def _split_payment_blocks(text: Any) -> list[str]:
    return re.split(
        r"(^#{1,6}[^\n]*(?:\n|$)|\n\s*\n)",
        str(text or ""),
        flags=re.MULTILINE,
    )


def _openings(text: Any, width: int = 3) -> Counter[str]:
    paragraphs = [
        part.strip()
        for index, part in enumerate(_split_payment_blocks(text))
        if index % 2 == 0 and part.strip() and not part.lstrip().startswith(("|", "[["))
    ]
    signatures = []
    for part in paragraphs:
        words = re.findall(r"[a-z0-9]+", part.lower())[:width]
        if not words or all(word.isdigit() for word in words):
            continue
        signatures.append(" ".join(words))
    return Counter(signatures)


def _vary_repeated_openings(sections: list[dict[str, str]]) -> None:
    combined = Counter()
    for section in sections:
        combined.update(_openings(section.get("body", "")))
    repeated = {opening for opening, count in combined.items() if opening and count >= 3}
    if not repeated:
        return

    seen: Counter[str] = Counter()
    variants = (
        "Dalam pembacaan kas, ",
        "Dari sisi risiko, ",
        "Bagi keputusan manajemen, ",
        "Pada horizon ini, ",
        "Sebagai tindak lanjut, ",
        "Untuk kontrol berikutnya, ",
    )
    variant_index = 0
    for section in sections:
        blocks = _split_payment_blocks(section.get("body", ""))
        for index in range(0, len(blocks), 2):
            paragraph = blocks[index].strip()
            if not paragraph or paragraph.startswith(("|", "[[")):
                continue
            signature = " ".join(re.findall(r"[a-z0-9]+", paragraph.lower())[:3])
            if signature not in repeated:
                continue
            seen[signature] += 1
            if seen[signature] == 1:
                continue
            prefix = variants[variant_index % len(variants)]
            variant_index += 1
            leading = blocks[index][:len(blocks[index]) - len(blocks[index].lstrip())]
            if paragraph.startswith(("- ", "* ", "+ ")):
                varied = paragraph[:2] + prefix + paragraph[2:3].lower() + paragraph[3:]
            else:
                varied = prefix + paragraph[0].lower() + paragraph[1:]
            blocks[index] = leading + varied
        section["body"] = "".join(blocks).strip()


def evaluate_payment_document_spine(raw_text: Any) -> dict[str, Any]:
    categories: set[str] = set()
    findings: list[str] = []
    sections = [
        section for section in _split_payment_sections(raw_text)
        if not section["title"].strip().lower().startswith("lampiran")
    ]
    titles = [section["title"] for section in sections]
    for required in PAYMENT_SECTION_ORDER:
        if required not in titles:
            categories.add("missing_cashflow_stage")
            findings.append(f"Bagian {required} belum hadir dalam alur laporan.")
    combined_openings = Counter()
    for section in sections:
        combined_openings.update(_openings(section.get("body", "")))
    repeated_global = [opening for opening, count in combined_openings.items() if opening and count >= 4]
    if repeated_global:
        categories.add("global_repeated_opening_spine")
        findings.append("Laporan masih memakai pembuka paragraf berulang lintas bagian: " + ", ".join(repeated_global[:4]) + ".")

    for index, section in enumerate(sections):
        title = section["title"]
        body = section["body"]
        if index > 0 and not _has_connector(body[:900], sections[index - 1]["title"], title):
            categories.add("missing_previous_handoff")
            findings.append(f"{title} belum mengikat angka ke bagian sebelumnya.")
        if index < len(sections) - 1 and not _has_connector(body[-900:], title, sections[index + 1]["title"]):
            categories.add("missing_next_handoff")
            findings.append(f"{title} belum menyiapkan keputusan menuju bagian berikutnya.")
        if any(count >= 3 and opening for opening, count in _openings(body).items()):
            categories.add("repeated_opening_spine")
            findings.append(f"{title} masih membuka paragraf dengan pola yang berulang.")
    return {"passes": not categories, "categories": sorted(categories), "findings": findings}


def repair_payment_document_spine(raw_text: Any) -> str:
    sections = _split_payment_sections(raw_text)
    openers = (
        "Melanjutkan {previous}, {current} membaca konsekuensi angka kas sebelumnya terhadap keputusan manajemen.",
        "Berangkat dari {previous}, {current} mempersempit sinyal cashflow menjadi risiko dan prioritas yang perlu dipilih.",
        "Setelah bagian {previous}, {current} menjaga alur agar forecast dan aksi tidak berdiri sendiri.",
        "Dari posisi {previous}, {current} menjelaskan sebab finansial yang perlu dipahami sebelum skenario dibaca.",
        "Rangkaian cashflow dari {previous} berlanjut ke {current} agar dashboard berubah menjadi keputusan.",
        "Kaitan dengan {previous} membuat {current} menjadi penajaman risiko, bukan blok laporan yang terpisah.",
    )
    closers = (
        "Temuan ini menjadi dasar untuk {next}, sehingga pembaca melihat perpindahan dari angka ke keputusan berikutnya.",
        "Arah berikutnya masuk ke {next}, tempat konsekuensi bagian ini diterjemahkan menjadi rencana cashflow yang lebih operasional.",
        "Implikasi bagian ini dibaca lebih lanjut pada {next}, bukan sebagai blok dashboard yang terpisah.",
        "Dari sini, {next} mengambil alih pembahasan agar risiko berubah menjadi pilihan tindakan.",
        "Pembahasan kemudian bergerak ke {next}, sehingga alur laporan tetap mengikuti kas, risiko, dan eksekusi.",
        "Dasar tersebut mengantar pembaca ke {next}, tempat prioritas cashflow diuji dari sisi owner dan kontrol.",
    )
    for index, section in enumerate(sections):
        body = str(section.get("body") or "").strip()
        before: list[str] = []
        after: list[str] = []
        if index > 0:
            previous = sections[index - 1]["title"]
            if not _has_connector(body[:900], previous, section["title"]):
                before.append(openers[(index - 1) % len(openers)].format(previous=previous, current=section["title"]))
        if index < len(sections) - 1:
            next_title = sections[index + 1]["title"]
            if not _has_connector(body[-900:], section["title"], next_title):
                after.append(closers[index % len(closers)].format(next=next_title))
        section["body"] = "\n\n".join([*before, body, *after]).strip()
    _vary_repeated_openings(sections)
    return _join_payment_sections(sections)
