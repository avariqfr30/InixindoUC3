import re

from management_interpretation import CashflowManagementInterpreter


class ExecutiveSummaryBuilder:
    @staticmethod
    def _summarize_section_body(body, max_words=22):
        cleaned = str(body or "").replace("[[", " ").replace("]]", " ")
        cleaned = cleaned.replace("|", " ")
        cleaned = " ".join(cleaned.split())
        if not cleaned:
            return ""
        sentences = [part.strip(" -;,.") for part in re.split(r"(?<=[.!?])\s+|;\s+", cleaned) if part.strip(" -;,.")]
        if sentences:
            sentences.sort(
                key=lambda sentence: (
                    any(term in sentence.lower() for term in ("kas", "invoice", "risiko", "pembayaran", "tagihan", "prioritas")),
                    bool(re.search(r"\b\d+(?:[,.]\d+)?%?\b|Rp\s*\d+", sentence)),
                    len(sentence),
                ),
                reverse=True,
            )
            cleaned = sentences[0]
        words = cleaned.split()
        return cleaned if len(words) <= max_words else " ".join(words[:max_words]).rstrip(" ,;:") + "."

    @staticmethod
    def _profile_values(context):
        profile = (context or {}).get("base_profile") or {}
        partners = [str(item).strip() for item in profile.get("top_risk_partners", []) if str(item).strip()]
        services = [str(item).strip() for item in profile.get("top_risk_services", []) if str(item).strip()]
        return partners, services

    @staticmethod
    def _first_bullet(text, fallback):
        for raw_line in str(text or "").splitlines():
            line = raw_line.strip().lstrip("- ").strip()
            if line:
                return line
        return fallback

    @classmethod
    def _section_digest(cls, peer_sections, max_sections=4, max_words=22):
        rows = []
        for section in peer_sections or []:
            title = str((section or {}).get("title") or "").strip()
            body = str((section or {}).get("body") or "").strip()
            if not title or not body:
                continue
            cleaned = cls._summarize_section_body(body, max_words=max_words)
            if not cleaned:
                continue
            rows.append(f"- {title}: {cleaned}")
            if len(rows) >= max_sections:
                break
        return "\n".join(rows)

    @staticmethod
    def _fallback_management_interpretation(context, partner, service):
        profile = (context or {}).get("base_profile") or {}
        expected_gap = profile.get("expected_gap_base")
        gap_text = f"Rp{int(expected_gap):,}" if isinstance(expected_gap, (int, float)) else "gap arus kas yang masih perlu dipersempit"
        return {
            "signal": str(context.get("executive_facts") or context.get("executive_headlines") or "Cash in dan cash out perlu dibaca bersama sebelum keputusan dibuat.").splitlines()[0].lstrip("- ").strip(),
            "meaning": f"Risiko utama adalah kontrol waktu pada {partner} dan layanan {service}, bukan hanya nominal tagihan.",
            "decision": f"Manajemen perlu menentukan apakah {partner} masuk eskalasi senior minggu ini.",
            "actions": [
                f"Kunci komitmen pembayaran untuk {partner}.",
                f"Gunakan {gap_text} sebagai batas risiko yang harus diperkecil dalam 30 hari.",
            ],
            "confidence": str(context.get("confidence_summary") or "Sedang - data operasional cukup, tetapi status komitmen bayar perlu validasi."),
        }

    @classmethod
    def build(cls, report_context, existing_body="", peer_sections=None):
        context = report_context or {}
        partners, services = cls._profile_values(context)
        partner = partners[0] if partners else "akun prioritas bernilai besar"
        service = services[0] if services else "layanan dengan eksposur terbesar"
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
        interpretation = context.get("management_interpretation") or cls._fallback_management_interpretation(context, partner, service)
        interpretation_block = CashflowManagementInterpreter.to_markdown_table(interpretation)
        timeline = (
            f"- Minggu ini: eskalasi {partner} dan konfirmasi ulang komitmen pembayaran yang masih bisa dipulihkan.\n"
            f"- Dalam 30 hari: kunci daftar tagihan pada {service} dan batasi arus kas keluar yang tidak mendukung stabilitas kas.\n"
            "- Tinjauan mingguan: ukur perubahan saldo kas akhir, ketahanan kas, dan rasio cakupan sebelum menaikkan asumsi proyeksi."
        )
        decisions = (
            f"- Apakah {partner} perlu masuk eskalasi senior pada minggu berjalan.\n"
            f"- Seberapa besar ruang pengendalian arus kas keluar yang masih aman sambil memulihkan tagihan {service}.\n"
            "- Siapa penanggung jawab tindak lanjut dan kapan komitmen pembayaran berikutnya harus dikonfirmasi."
        )
        return "\n\n".join([
            "### Posisi Keputusan dan Risiko Kas\n" + decision_position,
            "### Faktor Utama\n" + drivers,
            "### Sorotan Utama untuk Manajemen\n" + highlights,
            *digest_block,
            "### Interpretasi Manajemen\n" + interpretation_block,
            "### Jadwal Aksi Manajemen\n" + timeline,
            "### Keputusan yang Dibutuhkan\n" + decisions,
            "### Asumsi Proyeksi dan Catatan Batasan\n" + confidence,
        ])
