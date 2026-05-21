class ExecutiveSummaryBuilder:
    @staticmethod
    def _first_bullet(text, fallback):
        for raw_line in str(text or "").splitlines():
            line = raw_line.strip().lstrip("- ").strip()
            if line:
                return line
        return fallback

    @staticmethod
    def _section_digest(peer_sections, max_sections=4, max_words=22):
        rows = []
        for section in peer_sections or []:
            title = str((section or {}).get("title") or "").strip()
            body = str((section or {}).get("body") or "").strip()
            if not title or not body:
                continue
            cleaned = body.replace("[[", " ").replace("]]", " ")
            cleaned = " ".join(cleaned.split())
            if not cleaned:
                continue
            words = cleaned.split()
            brief = cleaned if len(words) <= max_words else " ".join(words[:max_words]).rstrip(" ,;:") + "."
            rows.append(f"- {title}: {brief}")
            if len(rows) >= max_sections:
                break
        return "\n".join(rows)

    @classmethod
    def build(cls, report_context, existing_body="", peer_sections=None):
        context = report_context or {}
        headlines = str(context.get("executive_headlines") or "").strip()
        decision_position = cls._first_bullet(
            headlines or context.get("executive_facts"),
            "Manajemen perlu menjaga disiplin kas dan memprioritaskan akun yang paling memengaruhi saldo kas akhir.",
        )
        confidence = str(context.get("confidence_summary") or "Keyakinan sedang berdasarkan bukti operasional yang tersedia.").strip()
        drivers = str(context.get("executive_facts") or context.get("executive_headlines") or "").strip()
        if not drivers:
            drivers = "- Prioritas utama adalah menjaga arus kas masuk, mengendalikan arus kas keluar, dan memperjelas tindak lanjut penagihan."
        highlights = headlines or "- Sorotan utama manajemen mengikuti faktor utama karena belum ada headline prioritas terpisah."
        if highlights.strip() == drivers.strip():
            highlights = "- Sorotan utama manajemen adalah mengubah faktor risiko tersebut menjadi keputusan penagihan, batas arus kas keluar, dan penanggung jawab mingguan."
        section_digest = cls._section_digest(peer_sections)
        digest_block = ["### Ringkasan Isi Laporan\n" + section_digest] if section_digest else []
        return "\n\n".join([
            "### Posisi Keputusan dan Risiko Kas\n" + decision_position,
            "### Faktor Utama\n" + drivers,
            "### Sorotan Utama untuk Manajemen\n" + highlights,
            *digest_block,
            "### Jadwal Aksi Manajemen\n- Minggu ini: eskalasi akun prioritas bernilai besar dan konfirmasi ulang komitmen pembayaran.\n- Dalam 30 hari: kunci daftar tagihan yang masih bisa dipulihkan dan batasi arus kas keluar yang tidak mendukung stabilitas kas.\n- Tinjauan mingguan: ukur perubahan saldo kas akhir, ketahanan kas, dan rasio cakupan sebelum menaikkan asumsi proyeksi.",
            "### Keputusan yang Dibutuhkan\n- Akun mana yang harus diprioritaskan untuk penagihan senior.\n- Batas toleransi arus kas keluar yang masih aman untuk periode berjalan.\n- Penanggung jawab tindak lanjut dan jadwal tinjauan mingguan.",
            "### Asumsi Proyeksi dan Catatan Batasan\n" + confidence,
        ])
