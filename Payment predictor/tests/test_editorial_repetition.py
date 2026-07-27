import unittest
import re

from editorial_intelligence import evaluate_payment_document_spine, repair_payment_document_spine


class RepeatedOpeningRepairTest(unittest.TestCase):
    def test_structural_headings_and_tables_are_not_counted_as_repeated_prose(self):
        sections = (
            "Ringkasan Eksekutif",
            "Analisis Deskriptif Cashflow",
            "Analisis Diagnostik Cashflow",
            "Analisis Prediktif Cashflow",
            "Rekomendasi Preskriptif",
        )
        descriptions = (
            "Ringkasan manajemen menjelaskan keputusan finansial utama pada bagian ini.",
            "Profil arus kas merangkum posisi angka yang perlu diperhatikan.",
            "Diagnosis hambatan menguraikan penyebab keterlambatan yang terbaca.",
            "Proyeksi periode menilai konsekuensi risiko pada horizon aktif.",
            "Rencana tindak mengikat rekomendasi dengan pelaksanaan yang terukur.",
        )
        handoffs = (
            "Temuan ini menjadi dasar untuk Analisis Deskriptif Cashflow.",
            "Arah berikutnya masuk ke Analisis Diagnostik Cashflow.",
            "Implikasi bagian ini dibaca lebih lanjut pada Analisis Prediktif Cashflow.",
            "Dari sini, Rekomendasi Preskriptif mengambil alih pembahasan.",
            "",
        )
        bodies = []
        for index, title in enumerate(sections):
            connector = "" if index == 0 else f"Melanjutkan {sections[index - 1]}, bagian ini menjaga alur keputusan.\n\n"
            closing = f"\n\n{handoffs[index]}" if handoffs[index] else ""
            synthesis = "\n\n### Ringkasan Isi Laporan\nRingkasan mengikat seluruh keputusan." if index == 0 else ""
            bodies.append(
                f"# {title}\n{connector}{descriptions[index]}"
                "\n\n### Cuplikan Dasbor Operasional"
                "\n| Prioritas | Penanggung Jawab |"
                "\n| --- | --- |"
                "\n| 1 | Finance Collection |"
                f"{synthesis}{closing}"
            )
        source = "\n\n".join(bodies)

        result = evaluate_payment_document_spine(source)

        self.assertNotIn("global_repeated_opening_spine", result["categories"])
        self.assertNotIn("repeated_opening_spine", result["categories"])

    def test_repair_varies_cross_section_openings_without_losing_evidence(self):
        repeated = "Eksposur risiko tinggi tetap Rp 1.250.000.000 dan memerlukan keputusan owner."
        sections = (
            "Ringkasan Eksekutif",
            "Analisis Deskriptif Cashflow",
            "Analisis Diagnostik Cashflow",
            "Analisis Prediktif Cashflow",
            "Rekomendasi Preskriptif",
        )
        bodies = []
        for index, title in enumerate(sections):
            connector = "" if index == 0 else f"Melanjutkan {sections[index - 1]}, angka ini memperjelas tahap berikutnya.\n\n"
            closing = "" if index == len(sections) - 1 else f"\n\nArah berikutnya masuk ke {sections[index + 1]}."
            synthesis = "\n\n### Ringkasan Isi Laporan\nRingkasan menjaga keputusan, angka, dan owner tetap terhubung." if index == 0 else ""
            marker = "\n\n[[CHART:aging|Distribusi Aging]]" if index == 1 else ""
            bodies.append(f"# {title}\n{connector}{repeated}{synthesis}{marker}{closing}")
        source = "\n\n".join(bodies)

        self.assertIn("global_repeated_opening_spine", evaluate_payment_document_spine(source)["categories"])

        repaired = repair_payment_document_spine(source)

        self.assertNotIn("global_repeated_opening_spine", evaluate_payment_document_spine(repaired)["categories"])
        self.assertEqual(repaired.count("Rp 1.250.000.000"), len(sections))
        self.assertEqual(repaired.lower().count("eksposur risiko tinggi"), len(sections))
        self.assertIn("[[CHART:aging|Distribusi Aging]]", repaired)
        for title in sections:
            self.assertIn(f"# {title}", repaired)

    def test_repair_varies_a_three_time_opening_before_it_reaches_the_qa_limit(self):
        sections = (
            "Ringkasan Eksekutif",
            "Analisis Deskriptif Cashflow",
            "Analisis Diagnostik Cashflow",
            "Analisis Prediktif Cashflow",
            "Rekomendasi Preskriptif",
        )
        bodies = []
        for index, title in enumerate(sections):
            connector = "" if index == 0 else f"Melanjutkan {sections[index - 1]}, angka ini memperjelas tahap berikutnya.\n\n"
            sentence = (
                "Eksposur risiko tinggi tetap Rp 1.250.000.000 dan memerlukan keputusan owner."
                if index < 3 else
                f"Bagian {title} memakai bukti yang berbeda untuk menjaga kesinambungan keputusan."
            )
            closing = "" if index == len(sections) - 1 else f"\n\nArah berikutnya masuk ke {sections[index + 1]}."
            bodies.append(f"# {title}\n{connector}{sentence}{closing}")
        source = "\n\n".join(bodies)

        repaired = repair_payment_document_spine(source)
        repeated_starts = sum(
            part.strip().lower().startswith("eksposur risiko tinggi")
            for part in re.split(r"\n\s*\n|(?<=[.!?])\s+", repaired)
        )

        self.assertLessEqual(repeated_starts, 1)
        self.assertEqual(repaired.count("Rp 1.250.000.000"), 3)


if __name__ == "__main__":
    unittest.main()
