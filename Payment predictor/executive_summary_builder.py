class ExecutiveSummaryBuilder:
    @staticmethod
    def _first_bullet(text, fallback):
        for raw_line in str(text or "").splitlines():
            line = raw_line.strip().lstrip("- ").strip()
            if line:
                return line
        return fallback

    @classmethod
    def build(cls, report_context, existing_body=""):
        context = report_context or {}
        headline = cls._first_bullet(
            context.get("executive_headlines") or context.get("executive_facts"),
            "Manajemen perlu menjaga cash discipline dan memprioritaskan akun yang paling memengaruhi ending cash.",
        )
        confidence = str(context.get("confidence_summary") or "Keyakinan sedang berdasarkan bukti operasional yang tersedia.").strip()
        facts = str(context.get("executive_facts") or context.get("executive_headlines") or "").strip()
        if not facts:
            facts = "- Prioritas utama adalah menjaga arus kas masuk, mengendalikan cash out, dan memperjelas tindak lanjut penagihan."
        return "\n\n".join([
            "### Headline Keputusan\n" + headline,
            "### Headline Utama untuk Manajemen\n" + facts,
            "### Apa Artinya untuk Manajemen\n" + "Laporan ini menempatkan risiko kas, konsentrasi invoice, dan prioritas aksi dalam bahasa keputusan agar pembaca eksekutif dapat langsung menentukan fokus 30 hari.",
            "### 3 Temuan Terpenting\n" + facts,
            "### Keputusan yang Dibutuhkan\n- Akun mana yang harus diprioritaskan untuk penagihan senior.\n- Batas toleransi cash out yang masih aman untuk periode berjalan.\n- Owner tindak lanjut dan jadwal review mingguan.",
            "### Aksi 30 Hari\n- Jalankan daftar prioritas penagihan.\n- Review komitmen pembayaran akun utama.\n- Kunci pengeluaran yang tidak mendukung stabilitas kas jangka pendek.",
            "### Tingkat Keyakinan dan Caveat\n" + confidence,
        ])
