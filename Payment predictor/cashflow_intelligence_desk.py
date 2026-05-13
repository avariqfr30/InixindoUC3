class CashflowIntelligenceDesk:
    """Builds a hidden, source-bound analyst brief for report generation."""

    ROLE_ORDER = (
        "Invoice Evidence Analyst",
        "Collection Risk Analyst",
        "Forecast Analyst",
        "Control Reviewer",
        "Executive Editor",
    )

    @classmethod
    def empty_context(cls):
        return {
            "agent_evidence_ledger": [],
            "agent_evidence_brief": (
                "## Control Reviewer\n"
                "- Jangan klaim pola cashflow, penyebab keterlambatan, atau prioritas penagihan karena data invoice internal masih kosong."
            ),
            "agent_rejected_claims": [
                "Pola historis tidak boleh disimpulkan tanpa record invoice.",
                "Prioritas akun tidak boleh dibuat dari asumsi kosong.",
            ],
        }

    @classmethod
    def build_context(
        cls,
        working_df,
        base_profile,
        priority_rows,
        top_themes,
        format_currency,
        format_percentage,
    ):
        if working_df is None or working_df.empty:
            return cls.empty_context()

        ledger = []
        total_invoices = int(base_profile.get("total_invoices") or 0)
        total_invoice_value = int(base_profile.get("total_invoice_value") or 0)
        delayed_value = int(base_profile.get("delayed_invoice_value") or 0)
        high_risk_value = int(base_profile.get("high_risk_invoice_value") or 0)
        expected_gap = int(base_profile.get("expected_gap_base") or 0)
        top_partners = [str(item) for item in base_profile.get("top_risk_partners", []) if str(item).strip()]
        top_services = [str(item) for item in base_profile.get("top_risk_services", []) if str(item).strip()]
        missing_fields = [str(item) for item in base_profile.get("missing_core_fields", []) if str(item).strip()]

        delayed_share = (delayed_value / total_invoice_value) * 100 if total_invoice_value else 0
        high_risk_share = (high_risk_value / total_invoice_value) * 100 if total_invoice_value else 0

        ledger.append(
            cls._card(
                role="Invoice Evidence Analyst",
                claim=(
                    f"Portofolio berisi {total_invoices} invoice senilai {format_currency(total_invoice_value)}; "
                    f"{format_currency(delayed_value)} atau {format_percentage(delayed_share)} sedang tertahan pada invoice terlambat."
                ),
                source_detail="Hasil agregasi nilai invoice dan skor kelas pembayaran internal.",
                confidence="high" if total_invoices and total_invoice_value else "low",
                allowed_use="Headline eksekutif dan snapshot portofolio.",
            )
        )
        ledger.append(
            cls._card(
                role="Collection Risk Analyst",
                claim=(
                    f"Eksposur risiko tinggi Kelas D/E bernilai {format_currency(high_risk_value)} "
                    f"atau {format_percentage(high_risk_share)} dari total invoice."
                ),
                source_detail="Subset invoice dengan skor pembayaran tinggi berdasarkan kelas pembayaran historis.",
                confidence="high" if total_invoices else "low",
                allowed_use="Konsentrasi risiko, caveat penagihan, dan prioritas eskalasi.",
                must_caveat=not bool(high_risk_value),
            )
        )
        ledger.append(
            cls._card(
                role="Forecast Analyst",
                claim=(
                    f"Base case menyisakan gap arus kas masuk {format_currency(expected_gap)} terhadap total invoice, "
                    "sehingga rencana 30 hari harus fokus pada invoice yang masih bisa dipulihkan."
                ),
                source_detail="Perbandingan total nilai invoice terhadap estimasi realisasi risk-adjusted.",
                confidence="medium" if total_invoices else "low",
                allowed_use="Skenario, implikasi cash in, dan prioritas tindakan.",
            )
        )

        for item in (priority_rows or [])[:3]:
            ledger.append(
                cls._card(
                    role="Collection Risk Analyst",
                    claim=(
                        f"Prioritas {item.get('priority')}: {item.get('focus')} bernilai "
                        f"{format_currency(int(item.get('invoice_value') or 0))} dengan isu {item.get('issue')}."
                    ),
                    source_detail="Urutan invoice prioritas dari kombinasi skor risiko dan nilai invoice.",
                    confidence="medium",
                    allowed_use="Tabel prioritas 30 hari dan agenda follow-up.",
                )
            )

        theme_lines = []
        for theme, count in (top_themes or [])[:3]:
            theme_lines.append(f"{theme} ({count} sinyal)")
        if theme_lines:
            ledger.append(
                cls._card(
                    role="Invoice Evidence Analyst",
                    claim="Tema keterlambatan paling sering: " + ", ".join(theme_lines) + ".",
                    source_detail="Klasifikasi kata kunci dari catatan keterlambatan internal.",
                    confidence="medium",
                    allowed_use="Pola hambatan utama dan rekomendasi treatment.",
                )
            )

        if top_partners or top_services:
            concentration = []
            if top_partners:
                concentration.append("partner " + ", ".join(top_partners[:3]))
            if top_services:
                concentration.append("layanan " + ", ".join(top_services[:3]))
            ledger.append(
                cls._card(
                    role="Executive Editor",
                    claim="Konsentrasi perhatian manajemen diarahkan ke " + " dan ".join(concentration) + ".",
                    source_detail="Top segment berisiko tinggi dari agregasi partner dan layanan.",
                    confidence="medium",
                    allowed_use="Headline eksekutif yang langsung menunjukkan titik keputusan.",
                )
            )

        rejected_claims = cls._build_rejected_claims(missing_fields, high_risk_value, top_partners)
        ledger.append(
            cls._card(
                role="Control Reviewer",
                claim="Narasi boleh memakai fakta invoice internal, tetapi tidak boleh menambah penyebab eksternal tanpa dukungan OSINT pembanding.",
                source_detail="Aturan kontrol penulisan laporan internal.",
                confidence="high",
                allowed_use="Caveat, asumsi, dan risiko kontrol.",
            )
        )

        return {
            "agent_evidence_ledger": ledger,
            "agent_evidence_brief": cls._format_brief(ledger, rejected_claims),
            "agent_rejected_claims": rejected_claims,
        }

    @staticmethod
    def _card(role, claim, source_detail, confidence, allowed_use, must_caveat=False):
        return {
            "agent": role,
            "claim": str(claim),
            "source_type": "internal_finance_dataset",
            "source_detail": str(source_detail),
            "confidence": confidence,
            "allowed_use": str(allowed_use),
            "must_caveat": bool(must_caveat),
        }

    @staticmethod
    def _build_rejected_claims(missing_fields, high_risk_value, top_partners):
        rejected = [
            "Jangan klaim penyebab eksternal sebagai fakta internal bila hanya OSINT atau catatan umum yang mendukung.",
            "Jangan mengubah angka invoice, gap, atau nilai risiko di luar angka yang tersedia pada konteks.",
            "Jangan menampilkan istilah agent, desk, atau workflow internal pada laporan pengguna.",
        ]
        if missing_fields:
            rejected.append(
                "Jangan membuat kesimpulan terlalu presisi untuk atribut yang belum lengkap: "
                + ", ".join(missing_fields)
                + "."
            )
        if not high_risk_value:
            rejected.append("Jangan menyatakan ada eksposur Kelas D/E bernilai besar bila nilai risiko tinggi terbaca Rp 0.")
        if not top_partners:
            rejected.append("Jangan menyebut nama partner prioritas bila konsentrasi partner risiko tinggi belum terbaca.")
        return rejected

    @classmethod
    def _format_brief(cls, ledger, rejected_claims):
        lines = []
        for role in cls.ROLE_ORDER:
            role_cards = [item for item in ledger if item["agent"] == role]
            if not role_cards:
                continue
            lines.append(f"## {role}")
            for item in role_cards:
                caveat = " Caveat wajib." if item.get("must_caveat") else ""
                lines.append(
                    f"- Klaim: {item['claim']} Sumber: {item['source_detail']} "
                    f"Confidence: {item['confidence']}. Pakai untuk: {item['allowed_use']}.{caveat}"
                )
            lines.append("")

        if lines and lines[-1] != "":
            lines.append("")
        if "## Control Reviewer" not in lines:
            lines.append("## Control Reviewer")
        for claim in rejected_claims:
            lines.append(f"- {claim}")
        return "\n".join(lines).strip()
