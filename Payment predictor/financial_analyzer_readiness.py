from config import APP_SERVER, REPORT_MAX_CONCURRENT_JOBS, WAITRESS_THREADS


class FinancialAnalyzerReadinessMixin:
    @staticmethod
    def _clamp_score(score):
        return max(1, min(5, int(round(score))))

    @classmethod
    def _score_to_confidence(cls, score):
        return cls.CONFIDENCE_LABELS.get(cls._clamp_score(score), "menengah")

    @staticmethod
    def _format_list_as_sentence(items):
        filtered_items = [str(item).strip() for item in items if str(item).strip()]
        if not filtered_items:
            return "-"
        if len(filtered_items) == 1:
            return filtered_items[0]
        if len(filtered_items) == 2:
            return f"{filtered_items[0]} dan {filtered_items[1]}"
        return f"{', '.join(filtered_items[:-1])}, dan {filtered_items[-1]}"

    @classmethod
    def _count_keyword_hits(cls, raw_text, keywords):
        lowered_text = str(raw_text or "").lower()
        return sum(1 for keyword in keywords if keyword in lowered_text)

    @classmethod
    def _build_hidden_dimensions(cls, base_profile, notes):
        notes = (notes or "").strip()
        missing_core_fields = len(base_profile.get("missing_core_fields", []))
        total_invoices = base_profile.get("total_invoices", 0)
        data_mode = base_profile.get("data_mode", "demo")
        owner_hits = cls._count_keyword_hits(notes, cls.OWNERSHIP_KEYWORDS)
        adoption_hits = cls._count_keyword_hits(notes, cls.ADOPTION_KEYWORDS)

        business_value_score = 4 if total_invoices > 0 and base_profile.get("high_risk_invoices", 0) > 0 else 3
        if notes:
            business_value_score += 1

        data_model_score = 4 if missing_core_fields <= 1 and total_invoices >= 25 else 3
        if data_mode == "demo":
            data_model_score -= 1

        infrastructure_score = 4 if APP_SERVER == "waitress" else 2
        if REPORT_MAX_CONCURRENT_JOBS >= 4:
            infrastructure_score += 1
        if APP_SERVER == "waitress" and WAITRESS_THREADS < 8:
            infrastructure_score -= 1

        people_score = 2
        if owner_hits >= 3:
            people_score = 4
        elif owner_hits >= 1:
            people_score = 3

        governance_score = 4
        if data_mode == "demo":
            governance_score -= 1
        if missing_core_fields >= 2:
            governance_score -= 1

        adoption_score = 2
        if adoption_hits >= 3:
            adoption_score = 4
        elif adoption_hits >= 1 or owner_hits >= 2:
            adoption_score = 3

        dimensions = {
            "business_value_clarity": cls._clamp_score(business_value_score),
            "data_model_readiness": cls._clamp_score(data_model_score),
            "infrastructure_readiness": cls._clamp_score(infrastructure_score),
            "people_ownership_readiness": cls._clamp_score(people_score),
            "governance_control_readiness": cls._clamp_score(governance_score),
            "organizational_adoption_readiness": cls._clamp_score(adoption_score),
        }
        note_profile = {
            "owner_hits": owner_hits,
            "adoption_hits": adoption_hits,
            "has_caution_prompt": bool(cls._count_keyword_hits(notes, cls.CAUTION_KEYWORDS)),
        }
        return dimensions, note_profile

    @classmethod
    def _build_readiness_outputs(cls, base_profile, hidden_dimensions, note_profile):
        data_mode = base_profile.get("data_mode", "demo")
        total_invoices = base_profile.get("total_invoices", 0)
        data_source_label = "dataset demo lokal" if data_mode == "demo" else "API internal perusahaan"
        core_field_summary = (
            f"{base_profile.get('core_fields_available', 0)}/{base_profile.get('core_fields_expected', 6)} atribut inti tersedia"
        )
        missing_fields_sentence = cls._format_list_as_sentence(base_profile.get("missing_core_fields", []))
        partner_focus_sentence = cls._format_list_as_sentence(base_profile.get("top_risk_partners", []))

        data_confidence = cls._score_to_confidence(hidden_dimensions["data_model_readiness"])
        deployment_confidence = cls._score_to_confidence(hidden_dimensions["infrastructure_readiness"])
        ownership_confidence = cls._score_to_confidence(hidden_dimensions["people_ownership_readiness"])
        control_confidence = cls._score_to_confidence(hidden_dimensions["governance_control_readiness"])
        adoption_confidence = cls._score_to_confidence(hidden_dimensions["organizational_adoption_readiness"])
        expected_gap_base = base_profile.get("expected_gap_base", 0)

        if data_mode == "demo":
            data_mode_line = (
                "Data masih berasal dari dataset demo lokal, sehingga laporan cocok untuk simulasi diskusi internal "
                "dan pengujian alur, bukan untuk komitmen operasional final."
            )
        else:
            data_mode_line = (
                "Data sudah ditarik dari API internal, sehingga laporan lebih layak dipakai sebagai dasar prioritas operasional "
                "selama sinkronisasi dan validasi sumber tetap terjaga."
            )

        readiness_signals = [
            f"- Business value clarity: {hidden_dimensions['business_value_clarity']}/5. Use case cashflow jelas dan langsung terkait percepatan realisasi invoice, pengendalian cash out, pengurangan risiko keterlambatan, dan prioritas follow-up.",
            f"- Data/model readiness: {hidden_dimensions['data_model_readiness']}/5. Sumber data saat ini adalah {data_source_label} dengan {total_invoices} invoice dan {core_field_summary}.",
            f"- Infrastructure/deployment readiness: {hidden_dimensions['infrastructure_readiness']}/5. Runtime aktif adalah {APP_SERVER} dengan queue {REPORT_MAX_CONCURRENT_JOBS} job dan thread Waitress {WAITRESS_THREADS}.",
            f"- People/ownership readiness: {hidden_dimensions['people_ownership_readiness']}/5. Sinyal owner atau sponsor eksplisit dari catatan pengguna = {note_profile['owner_hits']}.",
            f"- Governance/risk control: {hidden_dimensions['governance_control_readiness']}/5. Internal data tetap menjadi sumber fakta utama, OSINT hanya pendukung, dan fallback menjaga konsistensi struktur laporan.",
            f"- Organizational adoption readiness: {hidden_dimensions['organizational_adoption_readiness']}/5. Sinyal kesiapan adopsi atau pilot dari catatan pengguna = {note_profile['adoption_hits']}.",
            "- Gunakan sinyal ini untuk mengatur tingkat keyakinan, caveat, risiko kontrol, kepemilikan tindakan, dan prasyarat implementasi secara halus tanpa menyebut kerangka internal apa pun.",
        ]

        confidence_lines = [
            f"- Tingkat keyakinan data saat ini {data_confidence} karena laporan memakai {data_source_label} dengan {core_field_summary}.",
            f"- Kesiapan operasional aplikasi berada pada tingkat {deployment_confidence}; jalur deployment saat ini {APP_SERVER} dengan dukungan antrean {REPORT_MAX_CONCURRENT_JOBS} pekerjaan paralel.",
            f"- Kepastian penanggung jawab lintas fungsi berada pada tingkat {ownership_confidence}; sinyal owner eksplisit yang tertangkap dari catatan pengguna = {note_profile['owner_hits']}.",
            data_mode_line,
        ]

        assumption_lines = [
            f"- Analisis ini mengasumsikan {total_invoices} invoice yang tersedia sudah mewakili pola penagihan utama pada portofolio yang dibahas.",
            f"- Atribut inti yang terbaca saat ini adalah {core_field_summary}; atribut yang belum kuat atau belum tersedia: {missing_fields_sentence}.",
            "- Proyeksi arus kas masuk dibaca sebagai skenario manajemen berbasis histori kelas pembayaran, bukan kepastian realisasi kas.",
        ]
        if data_mode == "demo":
            assumption_lines.append("- Karena masih demo mode, angka dan narasi diposisikan sebagai bahan kalibrasi diskusi sebelum integrasi source-of-truth internal.")
        if note_profile["owner_hits"] == 0:
            assumption_lines.append("- Catatan pengguna belum menyebut penanggung jawab spesifik, sehingga penetapan owner tindakan masih perlu divalidasi saat rapat.")

        control_lines = [
            f"- Postur kontrol saat ini berada pada tingkat {control_confidence}; invoice prioritas tetap memerlukan verifikasi manual sebelum komitmen eskalasi atau forecast dinaikkan.",
            f"- Fokus kontrol utama saat ini berada pada konsentrasi risiko di {partner_focus_sentence} dan invoice Kelas D/E yang bernilai besar.",
            "- Sebelum eskalasi ke klien, verifikasi ulang invoice prioritas, kelengkapan dokumen, dan status komitmen bayar terbaru.",
            "- OSINT hanya dipakai sebagai konteks eksternal; fakta invoice, nilai, dan prioritas tetap harus mengikuti data internal yang tersedia.",
        ]
        if data_mode == "demo":
            control_lines.append("- Hindari menjadikan hasil demo sebagai dasar komitmen eksternal atau forecast final tanpa verifikasi terhadap sistem internal.")
        if hidden_dimensions["governance_control_readiness"] <= 2:
            control_lines.append("- Risiko salah tafsir naik bila data tambahan dan kontrol review manual tidak disiapkan sebelum sesi tindak lanjut.")

        implementation_lines = [
            f"- Untuk penggunaan operasional yang lebih kuat, pertahankan minimal {REPORT_MAX_CONCURRENT_JOBS} slot antrean dan gunakan runtime {APP_SERVER if APP_SERVER == 'waitress' else 'Waitress pada uji bersama'} untuk akses internal bersama.",
            "- Pastikan ritme refresh data, sumber invoice prioritas, dan jalur eskalasi ke account owner diputuskan sebelum laporan dipakai sebagai dasar action plan mingguan.",
        ]
        if data_mode == "demo":
            implementation_lines.append("- Langkah implementasi terdekat adalah memetakan endpoint API internal, autentikasi, dan struktur data source-of-truth agar prioritas penagihan tidak lagi bergantung pada dataset simulasi.")
        else:
            implementation_lines.append("- Pertahankan monitoring sinkronisasi API, validasi schema, dan fallback dokumen agar kualitas laporan tetap stabil saat dipakai banyak pengguna.")

        organizational_lines = [
            f"- Kesiapan pelaksanaan saat ini berada pada tingkat {adoption_confidence}; laporan akan lebih mudah dieksekusi bila sponsor bisnis, finance collection, dan account owner duduk pada forum review yang sama.",
            "- Gunakan laporan ini sebagai bahan keputusan internal: apa yang harus ditagih lebih dulu, siapa yang memimpin eskalasi, dan kontrol apa yang wajib ditutup sebelum follow-up berikutnya.",
        ]
        if note_profile["owner_hits"] == 0:
            organizational_lines.append("- Tetapkan minimal satu owner bisnis dan satu owner operasional untuk setiap cluster invoice prioritas agar keputusan rapat langsung bisa dijalankan.")
        if note_profile["adoption_hits"] == 0:
            organizational_lines.append("- Siapkan ritme pilot atau review berkala agar penggunaan laporan tidak berhenti di tahap analisis saja.")

        review_context = {
            "dataSource": "Demo dataset lokal" if data_mode == "demo" else "API internal perusahaan",
            "dataStatus": f"{total_invoices} invoice siap dianalisis",
            "operationalScope": "Analisis cashflow (arus kas masuk dan arus kas keluar), risiko realisasi invoice, dan prioritas tindak lanjut 30 hari.",
            "reportPurpose": "Bahan diskusi internal manajemen untuk keputusan penagihan, kontrol risiko, dan kesiapan pelaksanaan.",
            "readinessCaveat": data_mode_line,
            "controlNote": "Fakta internal tetap menjadi sumber utama; konteks eksternal hanya dipakai untuk memperkaya pembacaan risiko.",
        }
        cash_plan_implications = [
            "- Base case sebaiknya dipakai sebagai jangkar pembacaan rencana kas jangka pendek, sedangkan upside dan downside dipakai untuk menguji kebutuhan eskalasi dan ruang koreksi target.",
            "- Semakin besar eksposur Kelas D/E pada partner bernilai tinggi, semakin besar kebutuhan buffer keputusan, ritme follow-up, dan verifikasi dokumen sebelum asumsi arus kas masuk dinaikkan.",
        ]
        if data_mode == "demo":
            cash_plan_implications.append("- Karena masih demo mode, implikasi rencana kas diposisikan sebagai arah diskusi internal, bukan angka forecast final.")
        if expected_gap_base > 0:
            cash_plan_implications.append("- Gap arus kas masuk pada base case harus dibaca sebagai ruang risiko yang perlu diperkecil lewat penagihan prioritas, bukan langsung diasumsikan akan pulih otomatis.")
        cash_plan_implications.append("- Tekanan cash out harus dibaca bersama ending cash, runway, dan coverage ratio; kenaikan arus kas masuk saja tidak cukup bila kewajiban jatuh tempo menumpuk pada horizon yang sama.")

        return {
            "readiness_signals": "\n".join(readiness_signals),
            "confidence_summary": "\n".join(confidence_lines),
            "assumptions": "\n".join(assumption_lines),
            "controls": "\n".join(control_lines),
            "implementation_prerequisites": "\n".join(implementation_lines),
            "organizational_readiness": "\n".join(organizational_lines),
            "cash_plan_implications": "\n".join(cash_plan_implications),
            "review_context": review_context,
        }

    @classmethod
    def _build_visual_prompt(cls, class_distribution):
        labels = []
        for payment_class, metrics in class_distribution.items():
            labels.append(f"{payment_class},{round(metrics['share'], 1)}")
        chart_marker = (
            "[[CHART: Distribusi Historis Kelas Pembayaran | Persentase Invoice | "
            + "; ".join(labels)
            + "]]"
        )
        flow_marker = (
            "[[FLOW: Prioritisasi Invoice Risiko Tinggi -> Penagihan Berbasis Bukti -> "
            "Eskalasi Manajemen -> Pemulihan Cashflow]]"
        )
        return f"{chart_marker}\n{flow_marker}"

    @classmethod
    def apply_silent_assessment(cls, context, notes=""):
        base_profile = context.get("base_profile", {})
        hidden_dimensions, note_profile = cls._build_hidden_dimensions(base_profile, notes)
        visible_outputs = cls._build_readiness_outputs(base_profile, hidden_dimensions, note_profile)
        return {
            **visible_outputs,
            "hidden_dimensions": hidden_dimensions,
            "note_profile": note_profile,
        }
