import os
import sys
import tempfile
import unittest
from pathlib import Path

from sqlalchemy import create_engine, text

WORKSPACE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WORKSPACE))


class DemoSourceFallbackTest(unittest.TestCase):
    def test_empty_cached_demo_table_falls_back_to_csv(self):
        from cashflow_analysis import KnowledgeBase

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = os.path.join(tmpdir, 'db.csv')
            with open(csv_path, 'w', encoding='utf-8') as handle:
                handle.write(
                    'Periode Laporan,Tipe Partner,Layanan,Kelas Pembayaran,Nilai Invoice,Catatan Historis Keterlambatan\n'
                    'Q2 2025,Instansi Pemerintah,Audit SPBE,Kelas C (Telat 1-2 Bulan),Rp 180.000.000,Menunggu revisi DIPA.\n'
                )
            engine = create_engine(f"sqlite:///{os.path.join(tmpdir, 'finance.db')}")
            with engine.begin() as connection:
                connection.execute(text('CREATE TABLE invoices_demo ("Periode Laporan" TEXT)'))

            knowledge_base = KnowledgeBase.__new__(KnowledgeBase)
            knowledge_base.engine = engine
            knowledge_base.table_name = 'invoices_demo'
            knowledge_base.source_profile = {'path': csv_path}

            data_frame, summary = knowledge_base._load_demo_data()

        self.assertEqual(len(data_frame), 1)
        self.assertTrue(summary['isReady'])


if __name__ == '__main__':
    unittest.main()
