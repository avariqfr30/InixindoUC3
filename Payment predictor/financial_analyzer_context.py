import statistics

import pandas as pd


class FinancialAnalyzerContextMixin:
    @classmethod
    def build_report_context(cls, df, data_mode="demo"):
        if df is None or df.empty:
            return {
                "financial_summary": "Tidak ada data finansial internal yang tersedia.",
                "evidence": "Tidak ada catatan historis yang tersedia.",
                "diagnostic_breakdown": "- Belum ada pola hambatan yang dapat dijelaskan karena data masih kosong.",
                "management_brief": "Tidak ada management brief yang dapat disusun dari data kosong.",
                "executive_facts": "- Tidak ada fakta eksekutif yang tersedia.",
                "scenario_table": "| Skenario | Estimasi Arus Kas Masuk | Gap terhadap Total Invoice | Narasi Manajemen |\n|---|---:|---:|---|\n| Base Case | Rp 0 | Rp 0 | Data kosong. |",
                "priority_table": "| Prioritas | Fokus | Penanggung Jawab | Isu Utama | Aksi 30 Hari | Dampak yang Diharapkan |\n|---:|---|---|---|---|---|\n| 1 | Tidak ada data | Finance Collection | - | Lengkapi data terlebih dahulu. | Memberi dasar analisis yang layak. |",
                "meeting_agenda": "1. Pastikan data internal tersedia sebelum rapat dilanjutkan.",
                "base_profile": {
                    "data_mode": data_mode,
                    "total_invoices": 0,
                    "total_invoice_value": 0,
                    "delayed_invoice_value": 0,
                    "high_risk_invoices": 0,
                    "high_risk_invoice_value": 0,
                    "expected_realization_base": 0,
                    "expected_gap_base": 0,
                    "core_fields_available": 0,
                    "core_fields_expected": 6,
                    "missing_core_fields": [
                        "periode",
                        "partner",
                        "layanan",
                        "kelas pembayaran",
                        "nilai invoice",
                        "catatan keterlambatan",
                    ],
                    "top_risk_partners": [],
                },
                "visual_prompt": "Do not force visuals.",
            }

        working_df = df.copy()
        period_column = cls._find_column(working_df, "period")
        partner_column = cls._find_column(working_df, "partner")
        service_column = cls._find_column(working_df, "service")
        payment_class_column = cls._find_column(working_df, "payment_class")
        invoice_value_column = cls._find_column(working_df, "invoice_value")
        notes_column = cls._find_column(working_df, "notes")
        core_field_map = {
            "periode": period_column,
            "partner": partner_column,
            "layanan": service_column,
            "kelas pembayaran": payment_class_column,
            "nilai invoice": invoice_value_column,
            "catatan keterlambatan": notes_column,
        }
        missing_core_fields = [
            label for label, column in core_field_map.items() if not column
        ]

        working_df["__period"] = (
            working_df[period_column].astype(str).fillna("Tidak Diketahui")
            if period_column
            else "Tidak Diketahui"
        )
        working_df["__partner"] = (
            working_df[partner_column].astype(str).fillna("Tidak Diketahui")
            if partner_column
            else "Tidak Diketahui"
        )
        working_df["__service"] = (
            working_df[service_column].astype(str).fillna("Tidak Diketahui")
            if service_column
            else "Tidak Diketahui"
        )
        working_df["__note"] = (
            working_df[notes_column].astype(str).fillna("")
            if notes_column
            else ""
        )
        working_df["__invoice_value"] = (
            working_df[invoice_value_column].apply(cls._parse_currency)
            if invoice_value_column
            else 0
        )
        class_labels = []
        class_scores = []
        source_series = working_df[payment_class_column] if payment_class_column else pd.Series([""] * len(working_df))
        for raw_value in source_series:
            payment_class, score = cls._detect_payment_class(raw_value)
            class_labels.append(payment_class)
            class_scores.append(score)
        working_df["__payment_class"] = class_labels
        working_df["__payment_score"] = class_scores
        working_df["__base_realization"] = working_df.apply(
            lambda row: row["__invoice_value"] * cls.REALIZATION_RATE_MAP.get(row["__payment_class"], 0.65),
            axis=1,
        )
        working_df["__upside_realization"] = working_df.apply(
            lambda row: row["__invoice_value"] * cls.UPSIDE_RATE_MAP.get(row["__payment_class"], 0.72),
            axis=1,
        )
        working_df["__downside_realization"] = working_df.apply(
            lambda row: row["__invoice_value"] * cls.DOWNSIDE_RATE_MAP.get(row["__payment_class"], 0.50),
            axis=1,
        )

        total_invoices = len(working_df)
        total_invoice_value = int(working_df["__invoice_value"].sum())
        delayed_invoices = int((working_df["__payment_score"] > 1).sum())
        high_risk_invoices = int((working_df["__payment_score"] >= 4).sum())
        delayed_invoice_value = int(working_df.loc[working_df["__payment_score"] > 1, "__invoice_value"].sum())
        high_risk_invoice_value = int(working_df.loc[working_df["__payment_score"] >= 4, "__invoice_value"].sum())
        weighted_risk_score = statistics.fmean(working_df["__payment_score"]) if total_invoices else 0
        expected_realization_base = int(round(working_df["__base_realization"].sum()))
        expected_realization_upside = int(round(working_df["__upside_realization"].sum()))
        expected_realization_downside = int(round(working_df["__downside_realization"].sum()))
        expected_gap_base = max(total_invoice_value - expected_realization_base, 0)
        expected_gap_upside = max(total_invoice_value - expected_realization_upside, 0)
        expected_gap_downside = max(total_invoice_value - expected_realization_downside, 0)

        class_summary_df = (
            working_df.groupby("__payment_class", dropna=False)
            .agg(
                invoice_count=("__payment_class", "size"),
                invoice_value=("__invoice_value", "sum"),
                risk_score=("__payment_score", "mean"),
            )
        )
        class_summary_df["__sort_key"] = [
            cls.PAYMENT_CLASS_ORDER.get(index_value, 99) for index_value in class_summary_df.index
        ]
        class_summary_df = class_summary_df.sort_values("__sort_key")
        class_distribution = {}
        for payment_class, row in class_summary_df.iterrows():
            class_distribution[payment_class] = {
                "count": int(row["invoice_count"]),
                "value": int(row["invoice_value"]),
                "share": (row["invoice_count"] / total_invoices) * 100 if total_invoices else 0,
            }

        partner_summary_df = (
            working_df.groupby("__partner", dropna=False)
            .agg(
                invoice_count=("__partner", "size"),
                invoice_value=("__invoice_value", "sum"),
                avg_risk_score=("__payment_score", "mean"),
            )
            .sort_values(["avg_risk_score", "invoice_value"], ascending=[False, False])
            .head(5)
        )
        service_summary_df = (
            working_df.groupby("__service", dropna=False)
            .agg(
                invoice_count=("__service", "size"),
                invoice_value=("__invoice_value", "sum"),
                avg_risk_score=("__payment_score", "mean"),
            )
            .sort_values(["avg_risk_score", "invoice_value"], ascending=[False, False])
            .head(5)
        )
        period_summary_df = (
            working_df.groupby("__period", dropna=False)
            .agg(
                invoice_count=("__period", "size"),
                invoice_value=("__invoice_value", "sum"),
                avg_risk_score=("__payment_score", "mean"),
            )
            .reset_index()
        )
        period_summary_df["__sort_key"] = period_summary_df["__period"].apply(cls._parse_period_sort_key)
        period_summary_df = period_summary_df.sort_values("__sort_key").tail(6)

        high_risk_subset_df = working_df[working_df["__payment_score"] >= 4].copy()
        high_risk_partner_df = (
            high_risk_subset_df.groupby("__partner", dropna=False)
            .agg(
                invoice_count=("__partner", "size"),
                invoice_value=("__invoice_value", "sum"),
                avg_risk_score=("__payment_score", "mean"),
            )
            .sort_values(["invoice_value", "invoice_count"], ascending=[False, False])
            .head(5)
            if not high_risk_subset_df.empty
            else pd.DataFrame(columns=["invoice_count", "invoice_value", "avg_risk_score"])
        )
        high_risk_service_df = (
            high_risk_subset_df.groupby("__service", dropna=False)
            .agg(
                invoice_count=("__service", "size"),
                invoice_value=("__invoice_value", "sum"),
                avg_risk_score=("__payment_score", "mean"),
            )
            .sort_values(["invoice_value", "invoice_count"], ascending=[False, False])
            .head(5)
            if not high_risk_subset_df.empty
            else pd.DataFrame(columns=["invoice_count", "invoice_value", "avg_risk_score"])
        )

        top_themes = cls._extract_delay_themes(working_df["__note"])
        evidence_rows = working_df.sort_values(
            ["__payment_score", "__invoice_value"],
            ascending=[False, False],
        ).head(8)
        priority_rows = []
        for row_index, (_, row) in enumerate(evidence_rows.iterrows(), start=1):
            theme, action_plan = cls._get_action_plan(row["__note"])
            priority_rows.append(
                {
                    "priority": row_index,
                    "focus": f"{row['__partner']} / {row['__service']}",
                    "issue": theme,
                    "payment_class": row["__payment_class"],
                    "invoice_value": int(row["__invoice_value"]),
                    "action": action_plan["action"],
                    "impact": action_plan["impact"],
                    "owner": action_plan["owner"],
                }
            )

        latest_period_summary = period_summary_df.iloc[-1] if not period_summary_df.empty else None
        previous_period_summary = period_summary_df.iloc[-2] if len(period_summary_df) >= 2 else None

        recent_trend_line = "- Belum ada cukup periode untuk membaca perubahan terbaru."
        recent_value_change_line = "- Perubahan nilai invoice belum dapat dibandingkan antarperiode."
        recent_risk_change_line = "- Perubahan skor risiko belum dapat dibandingkan antarperiode."
        recent_period_label = latest_period_summary["__period"] if latest_period_summary is not None else "periode terbaru"

        if latest_period_summary is not None and previous_period_summary is not None:
            value_delta = int(latest_period_summary["invoice_value"] - previous_period_summary["invoice_value"])
            prior_value = int(previous_period_summary["invoice_value"])
            value_delta_pct = (value_delta / prior_value) * 100 if prior_value else 0
            risk_delta = float(latest_period_summary["avg_risk_score"] - previous_period_summary["avg_risk_score"])
            count_delta = int(latest_period_summary["invoice_count"] - previous_period_summary["invoice_count"])

            value_direction = "naik" if value_delta > 0 else "turun" if value_delta < 0 else "relatif stabil"
            risk_direction = "naik" if risk_delta > 0.05 else "turun" if risk_delta < -0.05 else "relatif stabil"
            risk_interpretation = (
                "memburuk"
                if risk_delta > 0.05
                else "membaik"
                if risk_delta < -0.05
                else "stabil"
            )
            recent_trend_line = (
                f"- Periode terbaru yang diamati adalah {previous_period_summary['__period']} ke {latest_period_summary['__period']} "
                f"dengan jumlah invoice berubah {count_delta:+d} dan nilai invoice {value_direction} "
                f"{abs(value_delta_pct):.1f}%."
            )
            recent_value_change_line = (
                f"- Nilai invoice {previous_period_summary['__period']} = {cls._format_currency(int(previous_period_summary['invoice_value']))}; "
                f"{latest_period_summary['__period']} = {cls._format_currency(int(latest_period_summary['invoice_value']))}."
            )
            recent_risk_change_line = (
                f"- Skor risiko rata-rata {risk_direction} {abs(risk_delta):.2f} poin dari "
                f"{previous_period_summary['avg_risk_score']:.2f} ke {latest_period_summary['avg_risk_score']:.2f}, "
                f"yang berarti kondisi penagihan {risk_interpretation}."
            )

        top_risk_partner_names = ", ".join(high_risk_partner_df.index.tolist()[:3]) if not high_risk_partner_df.empty else "-"
        top_risk_service_names = ", ".join(high_risk_service_df.index.tolist()[:3]) if not high_risk_service_df.empty else "-"
        top_theme_map = {theme: count for theme, count in top_themes}
        process_issue_count = top_theme_map.get("Dokumen dan administrasi", 0) + top_theme_map.get("Sengketa atau klarifikasi", 0)
        budget_issue_count = top_theme_map.get("Siklus anggaran", 0) + top_theme_map.get("Persetujuan internal klien", 0)
        liquidity_issue_count = top_theme_map.get("Likuiditas pelanggan", 0)

        diagnostic_breakdown_lines = []
        if process_issue_count:
            diagnostic_breakdown_lines.append(
                f"1. Hambatan proses, dokumen, dan klarifikasi masih dominan dengan {process_issue_count} sinyal historis; ini biasanya menahan invoice yang sebetulnya sudah siap ditagih tetapi belum lolos kelengkapan atau sign-off."
            )
        if budget_issue_count:
            diagnostic_breakdown_lines.append(
                f"2. Hambatan anggaran dan persetujuan internal klien muncul pada {budget_issue_count} catatan; dampaknya paling terasa pada akun pemerintah, BUMN, dan partner dengan approval berlapis."
            )
        if liquidity_issue_count:
            diagnostic_breakdown_lines.append(
                f"3. Tekanan likuiditas pelanggan muncul pada {liquidity_issue_count} catatan; risiko ini perlu dibedakan dari isu administratif karena membutuhkan pola negosiasi dan komitmen bayar yang lebih aktif."
            )
        if top_risk_partner_names != "-":
            diagnostic_breakdown_lines.append(
                f"4. Eksposur dampak terbesar saat ini terkonsentrasi pada {top_risk_partner_names}, sehingga setiap bottleneck di segmen tersebut memberi pengaruh paling besar ke arus kas masuk dan kualitas ending cash."
            )
        if not diagnostic_breakdown_lines:
            diagnostic_breakdown_lines.append("1. Belum ada pola hambatan dominan yang cukup kuat untuk dipisahkan dari catatan historis.")

        financial_summary_lines = [
            "## Snapshot Arus Kas Masuk",
            f"- Total invoice dianalisis: {total_invoices}",
            f"- Total nilai invoice: {cls._format_currency(total_invoice_value)}",
            f"- Porsi invoice terlambat: {cls._format_percentage((delayed_invoices / total_invoices) * 100 if total_invoices else 0)}",
            f"- Nilai invoice terlambat: {cls._format_currency(delayed_invoice_value)}",
            f"- Porsi invoice risiko tinggi (Kelas D/E): {cls._format_percentage((high_risk_invoices / total_invoices) * 100 if total_invoices else 0)}",
            f"- Nilai invoice risiko tinggi (Kelas D/E): {cls._format_currency(high_risk_invoice_value)}",
            f"- Skor risiko penagihan rata-rata: {weighted_risk_score:.2f} dari 5.00",
            f"- Estimasi arus kas masuk risk-adjusted (base case): {cls._format_currency(expected_realization_base)} atau {cls._format_percentage((expected_realization_base / total_invoice_value) * 100 if total_invoice_value else 0)} dari total nilai invoice",
            f"- Gap arus kas masuk pada base case: {cls._format_currency(expected_gap_base)}",
            "",
            "## Pergerakan Periode Terbaru",
            recent_trend_line,
            recent_value_change_line,
            recent_risk_change_line,
            "",
            "## Distribusi Kelas Pembayaran",
            "| Kelas | Jumlah Invoice | Nilai Invoice | Porsi Invoice |",
            "|---|---:|---:|---:|",
        ]
        for payment_class, metrics in class_distribution.items():
            financial_summary_lines.append(
                f"| {payment_class} | {metrics['count']} | {cls._format_currency(metrics['value'])} | {cls._format_percentage(metrics['share'])} |"
            )

        financial_summary_lines.extend(
            [
                "",
                "## Segmentasi Risiko per Tipe Partner",
                "| Tipe Partner | Jumlah Invoice | Nilai Invoice | Skor Risiko Rata-rata |",
                "|---|---:|---:|---:|",
            ]
        )
        for partner, row in partner_summary_df.iterrows():
            financial_summary_lines.append(
                f"| {partner} | {int(row['invoice_count'])} | {cls._format_currency(int(row['invoice_value']))} | {row['avg_risk_score']:.2f} |"
            )

        financial_summary_lines.extend(
            [
                "",
                "## Segmentasi Risiko per Layanan",
                "| Layanan | Jumlah Invoice | Nilai Invoice | Skor Risiko Rata-rata |",
                "|---|---:|---:|---:|",
            ]
        )
        for service, row in service_summary_df.iterrows():
            financial_summary_lines.append(
                f"| {service} | {int(row['invoice_count'])} | {cls._format_currency(int(row['invoice_value']))} | {row['avg_risk_score']:.2f} |"
            )

        financial_summary_lines.extend(
            [
                "",
                "## Tren Ringkas per Periode",
                "| Periode | Jumlah Invoice | Nilai Invoice | Skor Risiko Rata-rata |",
                "|---|---:|---:|---:|",
            ]
        )
        for _, row in period_summary_df.iterrows():
            financial_summary_lines.append(
                f"| {row['__period']} | {int(row['invoice_count'])} | {cls._format_currency(int(row['invoice_value']))} | {row['avg_risk_score']:.2f} |"
            )

        financial_summary_lines.extend(
            [
                "",
                "## Konsentrasi Eksposur Risiko Tinggi",
                "| Tipe Partner | Jumlah Invoice D/E | Nilai Invoice D/E | Skor Risiko Rata-rata |",
                "|---|---:|---:|---:|",
            ]
        )
        if not high_risk_partner_df.empty:
            for partner, row in high_risk_partner_df.iterrows():
                financial_summary_lines.append(
                    f"| {partner} | {int(row['invoice_count'])} | {cls._format_currency(int(row['invoice_value']))} | {row['avg_risk_score']:.2f} |"
                )
        else:
            financial_summary_lines.append("| Tidak ada konsentrasi D/E | 0 | Rp 0 | 0.00 |")

        financial_summary_lines.extend(["", "## Sinyal Diagnostik Utama"])
        if top_themes:
            for index, (theme, count) in enumerate(top_themes, start=1):
                financial_summary_lines.append(f"{index}. {theme} muncul pada {count} catatan historis.")
        else:
            financial_summary_lines.append("1. Belum ada tema dominan yang dapat diambil dari catatan historis.")

        evidence_lines = []
        for _, row in evidence_rows.iterrows():
            note = str(row["__note"]).strip()
            if not note:
                continue
            trimmed_note = cls._trim_note_for_report(note, max_length=220)
            evidence_lines.append(
                "\n".join(
                    [
                        f"- {row['__period']} | {row['__partner']} | {row['__service']}",
                        f"  - Kelas pembayaran: {row['__payment_class']}",
                        f"  - Nilai invoice: {cls._format_currency(int(row['__invoice_value']))}",
                        f"  - Catatan utama: {trimmed_note}",
                    ]
                )
            )
        if not evidence_lines:
            evidence_lines.append("- Tidak ada catatan historis yang cukup untuk dikutip.")

        executive_fact_lines = [
            f"- Portofolio yang dianalisis mencakup {total_invoices} invoice dengan total nilai {cls._format_currency(total_invoice_value)}.",
            f"- Invoice terlambat mencapai {delayed_invoices} kasus atau {cls._format_percentage((delayed_invoices / total_invoices) * 100 if total_invoices else 0)} dari populasi, dengan nilai tertunda {cls._format_currency(delayed_invoice_value)}.",
            f"- Eksposur risiko tinggi Kelas D/E mencapai {high_risk_invoices} invoice senilai {cls._format_currency(high_risk_invoice_value)} dan terkonsentrasi pada {top_risk_partner_names}.",
            f"- Estimasi arus kas masuk risk-adjusted base case adalah {cls._format_currency(expected_realization_base)} dengan gap {cls._format_currency(expected_gap_base)} terhadap total nilai invoice.",
            recent_trend_line,
            recent_risk_change_line,
            f"- Layanan dengan eksposur risiko tinggi paling dominan saat ini: {top_risk_service_names}.",
        ]
        scenario_lines = [
            "| Skenario | Estimasi Arus Kas Masuk | Gap terhadap Total Invoice | Narasi Manajemen |",
            "|---|---:|---:|---|",
            f"| Upside | {cls._format_currency(expected_realization_upside)} | {cls._format_currency(expected_gap_upside)} | Terjadi bila perbaikan approval, kelengkapan dokumen, dan penagihan pada akun prioritas dijalankan cepat. |",
            f"| Base Case | {cls._format_currency(expected_realization_base)} | {cls._format_currency(expected_gap_base)} | Menggambarkan realisasi yang paling realistis bila perilaku penagihan tetap mengikuti pola historis saat ini. |",
            f"| Downside | {cls._format_currency(expected_realization_downside)} | {cls._format_currency(expected_gap_downside)} | Terjadi bila siklus anggaran, likuiditas pelanggan, atau dispute memburuk sehingga semakin banyak invoice bergeser ke kelas risiko tinggi. |",
        ]
        priority_table_lines = [
            "| Prioritas | Fokus | Penanggung Jawab | Isu Utama | Aksi 30 Hari | Dampak yang Diharapkan |",
            "|---:|---|---|---|---|---|",
        ]
        management_priority_lines = [
            "| Prioritas | Fokus | Kelas | Nilai Invoice | Isu Utama | Aksi Awal | Fungsi Utama |",
            "|---:|---|---|---:|---|---|---|",
        ]
        for item in priority_rows[:6]:
            management_priority_lines.append(
                f"| {item['priority']} | {item['focus']} | {item['payment_class']} | {cls._format_currency(item['invoice_value'])} | {item['issue']} | {item['action']} | {item['owner']} |"
            )
            priority_table_lines.append(
                f"| {item['priority']} | {item['focus']} | {item['owner']} | {item['issue']} | {item['action']} | {item['impact']} |"
            )
        meeting_questions = [
            "Segmen partner mana yang paling layak mendapat eskalasi manajemen karena menggabungkan nilai invoice besar dan skor risiko tinggi?",
            "Hambatan apa yang paling sering berulang: anggaran, approval, dokumen, likuiditas, atau sengketa ruang lingkup?",
            f"Apakah strategi 30 hari ke depan harus difokuskan pada {recent_period_label} dan akun-akun D/E agar gap arus kas masuk dapat ditekan tanpa memperburuk tekanan cash out?",
        ]
        agenda_lines = [f"{index}. {question}" for index, question in enumerate(meeting_questions, start=1)]
        management_brief_lines = (
            ["## Fakta Eksekutif yang Wajib Dijaga Konsisten"]
            + executive_fact_lines
            + ["", "## Skenario 1-2 Kuartal"]
            + scenario_lines
            + ["", "## Prioritas Penagihan untuk Pembahasan Internal"]
            + management_priority_lines
            + ["", "## Agenda Diskusi Manajemen"]
            + agenda_lines
        )

        return {
            "financial_summary": "\n".join(financial_summary_lines),
            "evidence": "\n".join(evidence_lines),
            "diagnostic_breakdown": "\n".join(diagnostic_breakdown_lines),
            "management_brief": "\n".join(management_brief_lines),
            "executive_facts": "\n".join(executive_fact_lines),
            "scenario_table": "\n".join(scenario_lines),
            "priority_table": "\n".join(priority_table_lines),
            "meeting_agenda": "\n".join(agenda_lines),
            "base_profile": {
                "data_mode": data_mode,
                "total_invoices": total_invoices,
                "total_invoice_value": total_invoice_value,
                "delayed_invoices": delayed_invoices,
                "delayed_invoice_value": delayed_invoice_value,
                "high_risk_invoices": high_risk_invoices,
                "high_risk_invoice_value": high_risk_invoice_value,
                "expected_realization_base": expected_realization_base,
                "expected_gap_base": expected_gap_base,
                "core_fields_available": len(core_field_map) - len(missing_core_fields),
                "core_fields_expected": len(core_field_map),
                "missing_core_fields": missing_core_fields,
                "top_risk_partners": high_risk_partner_df.index.tolist()[:3],
                "top_risk_services": high_risk_service_df.index.tolist()[:3],
            },
            "visual_prompt": cls._build_visual_prompt(class_distribution),
        }
