class CashflowManagementInterpreter:
    """Executive-facing interpretation shared by UC3 dashboard and reports."""

    STATUS_MAP = {
        "aman": "AMAN",
        "waspada": "WASPADA",
        "bahaya": "BAHAYA",
        "kritis": "BAHAYA",
    }

    @classmethod
    def display_status(cls, raw_status):
        return cls.STATUS_MAP.get(str(raw_status or "").strip().lower(), "WASPADA")

    @classmethod
    def build_dashboard_reaction(
        cls,
        raw_status,
        ending_cash,
        total_cash_in,
        total_cash_out,
        top_overdue_accounts=None,
        weakest_dimensions=None,
    ):
        status = cls.display_status(raw_status)
        top_account = (top_overdue_accounts or [{}])[0].get("name") or "akun prioritas"
        weakest = ", ".join(weakest_dimensions or []) or "likuiditas dan konversi invoice"

        if status == "AMAN":
            headline = "Kas berada dalam zona aman karena cash tersedia, jadwal masuk kas terlihat, dan risiko utama terkendali."
            meaning = "Prioritas manajemen adalah menjaga ritme penagihan, memantau jadwal uang masuk, dan mencegah konsentrasi risiko baru."
            decision = "Pertahankan pola follow-up saat ini sambil memantau akun dengan overdue terbesar."
            actions = [
                "Jaga cadence penagihan mingguan pada akun prioritas.",
                "Pastikan jadwal uang masuk tetap tercatat dan dikonfirmasi.",
                "Pantau perubahan cash-out agar buffer tidak turun tanpa sinyal awal.",
            ]
            confidence = "Kuat - buffer dan coverage masih memadai untuk horizon aktif."
        elif status == "WASPADA":
            headline = "Kas masih manageable, tetapi konversi invoice perlu dipercepat."
            meaning = f"Risiko utama berada pada {weakest}; keterlambatan {top_account} dapat menekan ending cash."
            decision = "Manajemen perlu memilih invoice yang dieskalasi minggu ini dan mengunci komitmen pembayaran."
            actions = [
                f"Eskalasi {top_account} ke penanggung jawab bisnis dan keuangan.",
                "Konfirmasi tanggal bayar tertulis sebelum memperluas eskalasi ke akun lain.",
            ]
            confidence = "Sedang - sinyal risiko jelas, tetapi komitmen bayar terbaru tetap perlu validasi."
        else:
            headline = "Kas akhir masuk zona bahaya jika invoice prioritas tidak segera terkonversi."
            meaning = f"Tekanan cash-out lebih cepat dari cash-in; {top_account} menjadi titik keputusan paling mendesak."
            decision = "Manajemen perlu menjalankan eskalasi senior dan meninjau cash-out non-prioritas."
            actions = [
                f"Eskalasi senior untuk {top_account} hari ini atau pada forum manajemen terdekat.",
                "Tahan atau review cash-out non-prioritas sampai komitmen pembayaran lebih jelas.",
                "Pantau saldo akhir secara mingguan sampai status keluar dari zona bahaya.",
            ]
            confidence = "Cukup kuat - gap kas terlihat pada horizon aktif, tetapi timing realisasi masih perlu validasi operasional."

        return {
            "status": status,
            "headline": headline,
            "signal": f"Cash in terproyeksi Rp{int(total_cash_in):,}; cash out terproyeksi Rp{int(total_cash_out):,}; ending cash Rp{int(ending_cash):,}.",
            "meaning": meaning,
            "decision": decision,
            "actions": actions,
            "confidence": confidence,
            "minto_pyramid": {
                "main_answer": f"{headline} {decision}",
                "supporting_arguments": [
                    meaning,
                    f"Cash bridge menunjukkan cash in Rp{int(total_cash_in):,}, cash out Rp{int(total_cash_out):,}, dan ending cash Rp{int(ending_cash):,}.",
                    f"Akun prioritas yang perlu dibaca lebih dulu adalah {top_account}.",
                ],
                "evidence": [
                    f"Status dashboard: {status}.",
                    f"Dimensi terlemah: {weakest}.",
                    f"Confidence: {confidence}",
                ],
            },
        }

    @classmethod
    def build_decision_queue(cls, top_overdue_accounts=None):
        rows = []
        for index, account in enumerate(top_overdue_accounts or [], start=1):
            name = account.get("name") or "Akun prioritas"
            days = int(account.get("days_overdue") or 0)
            if days >= 120:
                action = "Eskalasi senior dan minta komitmen pembayaran tertulis."
            elif days >= 45:
                action = "Konfirmasi dokumen, owner approval, dan tanggal bayar."
            else:
                action = "Jaga follow-up rutin dan pantau perubahan status."
            rows.append(
                {
                    "priority": index,
                    "name": name,
                    "amount": int(account.get("amount") or 0),
                    "days_overdue": days,
                    "action": action,
                }
            )
        return rows

    @staticmethod
    def _reader_phrase(value):
        text = str(value or "-")
        replacements = {
            "timing-control": "kontrol waktu",
            "Timing-control": "Kontrol waktu",
            "cash-in": "cash in",
            "cash-out": "cash out",
        }
        for source, target in replacements.items():
            text = text.replace(source, target)
        return text

    @classmethod
    def to_markdown_table(cls, interpretation):
        row = interpretation or {}
        actions = row.get("actions", row.get("action"))
        action_text = "; ".join(actions) if isinstance(actions, list) else str(actions or "-")
        signal = cls._reader_phrase(row.get("signal", "-"))
        meaning = cls._reader_phrase(row.get("meaning", "-"))
        decision = cls._reader_phrase(row.get("decision", "-"))
        action_text = cls._reader_phrase(action_text)
        confidence = cls._reader_phrase(row.get("confidence", "-"))
        return "\n".join(
            [
                "| Sinyal | Makna | Keputusan | Aksi | Keyakinan |",
                "| --- | --- | --- | --- | --- |",
                (
                    f"| {signal} | {meaning} | "
                    f"{decision} | {action_text} | {confidence} |"
                ),
            ]
        )
