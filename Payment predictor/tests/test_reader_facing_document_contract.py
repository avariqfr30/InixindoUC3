import sys
import unittest
from pathlib import Path

from docx import Document

PROJECT_DIR = Path(__file__).resolve().parents[1]
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

from docx_rendering import DocumentBuilder
from report_finalization import ReportFinalizer


class ReaderFacingDocumentContractTests(unittest.TestCase):
    def test_reader_safe_text_removes_source_mode_terms(self):
        raw = "API internal perusahaan memakai endpoint source-of-truth dengan Waitress queue thread dan sync status record aktif."
        clean = DocumentBuilder.reader_safe_text(raw)
        for token in ["API internal", "endpoint", "source-of-truth", "Waitress", "queue", "thread", "sync status", "record aktif"]:
            self.assertNotIn(token, clean)
        self.assertIn("data operasional yang tersedia", clean)

    def test_toc_has_static_fallback_without_update_instruction(self):
        doc = Document()
        DocumentBuilder.add_table_of_contents(doc)
        text = "\n".join(paragraph.text for paragraph in doc.paragraphs)
        self.assertIn("Daftar Isi", text)
        self.assertIn("Ringkasan Eksekutif", text)
        self.assertIn("Analisis Prediktif Cashflow", text)
        self.assertNotIn("Update Field", text)

    def test_executive_summary_finalization_is_decision_first_and_source_neutral(self):
        finalizer = ReportFinalizer()
        raw = "# Ringkasan Eksekutif\n### Tingkat Keyakinan dan Caveat\nCatatan teknis dari API internal perusahaan dan endpoint."
        context = {
            "executive_headlines": "- Risiko kas terkonsentrasi pada keterlambatan pembayaran akun utama.",
            "executive_facts": "- Ending cash perlu dijaga.",
            "confidence_summary": "Keyakinan sedang berdasarkan bukti operasional.",
        }
        result = finalizer.finalize(raw, context, "", analysis_payload={})
        self.assertIn("# Ringkasan Eksekutif", result)
        self.assertIn("### Headline Keputusan", result)
        self.assertIn("### Keputusan yang Dibutuhkan", result)
        for token in ["API internal", "endpoint", "source-of-truth", "Waitress", "queue", "thread", "sync status"]:
            self.assertNotIn(token, result)


if __name__ == "__main__":
    unittest.main()
