import re


class FinancialAnalyzerEvidenceMixin:
    @classmethod
    def _extract_delay_themes(cls, notes_series):
        counts = {theme: 0 for theme in cls.DELAY_THEME_KEYWORDS}
        for note in notes_series.dropna().astype(str):
            lowered_note = note.lower()
            for theme, keywords in cls.DELAY_THEME_KEYWORDS.items():
                if any(keyword in lowered_note for keyword in keywords):
                    counts[theme] += 1

        ranked = [(theme, count) for theme, count in counts.items() if count > 0]
        ranked.sort(key=lambda item: item[1], reverse=True)
        return ranked[:4]

    @classmethod
    def _detect_note_themes(cls, note):
        matched_themes = []
        lowered_note = str(note or "").lower()
        for theme, keywords in cls.DELAY_THEME_KEYWORDS.items():
            if any(keyword in lowered_note for keyword in keywords):
                matched_themes.append(theme)
        return matched_themes

    @classmethod
    def _get_action_plan(cls, note):
        themes = cls._detect_note_themes(note)
        selected_theme = themes[0] if themes else "Follow-up umum"
        action_plan = cls.THEME_ACTION_MAP.get(selected_theme, cls.THEME_ACTION_MAP["Follow-up umum"])
        return selected_theme, action_plan

    @staticmethod
    def _format_evidence_chunk(chunk):
        normalized = re.sub(r"\s+", " ", str(chunk or "")).strip()
        if not normalized:
            return ""

        pattern = re.compile(
            r"Periode Laporan:\s*(?P<period>.*?)\s*\|\s*"
            r"Tipe Partner:\s*(?P<partner>.*?)\s*\|\s*"
            r"Layanan:\s*(?P<service>.*?)\s*\|\s*"
            r"Kelas Pembayaran:\s*(?P<payment_class>.*?)\s*\|\s*"
            r"Nilai Invoice:\s*(?P<invoice_value>.*?)\s*\|\s*"
            r"Catatan Historis Keterlambatan:\s*(?P<note>.*)$"
        )
        match = pattern.match(normalized)
        if not match:
            return f"- {normalized}"

        parts = match.groupdict()
        return "\n".join(
            [
                f"- {parts['period']} | {parts['partner']} | {parts['service']}",
                f"  - Kelas pembayaran: {parts['payment_class']}",
                f"  - Nilai invoice: {parts['invoice_value']}",
                f"  - Catatan utama: {parts['note']}",
            ]
        )

    @classmethod
    def normalize_evidence_text(cls, raw_text):
        lines = []
        chunks = [chunk.strip() for chunk in str(raw_text or "").split("\n---\n") if chunk.strip()]

        if len(chunks) > 1 or any("Periode Laporan:" in chunk for chunk in chunks):
            for chunk in chunks[:10]:
                lines.append(cls._format_evidence_chunk(chunk))
            return "\n".join(lines) if lines else "- Tidak ada catatan historis yang cukup untuk dikutip."

        for raw_line in str(raw_text or "").splitlines():
            cleaned_line = raw_line.strip()
            if not cleaned_line or cleaned_line == "---":
                continue
            if cleaned_line.startswith("- ") or re.match(r"^\d+\.", cleaned_line):
                lines.append(cleaned_line)
            else:
                lines.append(f"- {cleaned_line}")

        return "\n".join(lines) if lines else "- Tidak ada catatan historis yang cukup untuk dikutip."
