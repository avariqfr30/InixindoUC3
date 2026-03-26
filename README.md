# Historical Revenue Predictor (Internal Tool)

Sistem *Enterprise AI* berbasis web yang dirancang khusus untuk divisi Finance dan Executive Board (CFO) Inixindo Jogja. Aplikasi ini menelan **seluruh histori data penagihan (All-Time)** tanpa batasan kuartal, untuk memprediksi risiko arus kas, membedah pola keterlambatan pembayaran lintas demografi, dan menghasilkan strategi mitigasi likuiditas secara otomatis.

Berbeda dengan sistem analitik tradisional, AI ini tidak hanya menghitung angka, tetapi membaca *konteks* dari catatan penagihan dan membandingkannya dengan tren siklus anggaran makro di Indonesia (Pemerintah, BUMN, dan Swasta) untuk memprediksi pergeseran **Kelas Pembayaran (Kelas A hingga Kelas E)**.

## Fitur Utama

* **All-Time Macro Analysis**: Menarik dan mensintesis 100% data historis perusahaan secara serentak untuk menemukan pola tunda bayar (*bottlenecks*) jangka panjang.
* **Payment Class Profiling**: Secara cerdas membedah pergeseran perilaku klien dari Kelas A (Tepat Waktu) hingga Kelas E (Macet > 6 Bulan).
* **OSINT Budget Cycle Context**: Menggunakan *Serper API* (multi-query `search` + `news`) untuk memperkaya konteks tren anggaran, perilaku pembayaran, dan sinyal risiko likuiditas di Indonesia.
* **CFO-Level Auto-Reporting**: Menghasilkan dokumen Microsoft Word (*Strictly Confidential*) berskala eksekutif dengan daftar isi, heading terstruktur, numbering/bullet bawaan Word, tabel, grafik *Bar Chart*, dan *Flowchart* mitigasi.
* **Smart Prompt Suggestions**: Menyediakan cip instruksi makro otomatis agar manajemen tidak perlu repot merangkai *prompt* analisis dari nol.

## Prasyarat Sistem

* **Python 3.9+** (Untuk *deployment* lokal).
* **Ollama**: Berjalan di *background* pada port `11434`.
* **Kredensial Serper**: `SERPER_API_KEY` aktif (bisa diisi lewat environment variable atau `config.py`).
* **Demo mode**: File CSV berisi data penagihan pada `data/db.csv`.
* **Internal API mode**: Endpoint API internal yang mengembalikan dataset finansial dalam format JSON.

## Instalasi & Persiapan (Local Deployment)

### 1. Struktur Folder
Pastikan struktur proyek Anda persis seperti ini sebelum dijalankan:
```text
/project_folder
 ├── data/
 │    └── db.csv            <-- File database finansial Anda
 ├── templates/
 │    └── index.html        <-- File antarmuka web
 ├── app.py
 ├── config.py
 ├── core.py
 └── requirements.txt
```

### 2. Instalasi Dependensi Python
Buka terminal/CMD di dalam folder proyek Anda, lalu jalankan:
```bash
pip install -r requirements.txt
```
*(Atau instal manual: `pip install flask flask-cors pandas chromadb ollama matplotlib python-docx markdown beautifulsoup4 requests sqlalchemy`)*

### 3. Menyiapkan Model Ollama (Wajib)
Pastikan Anda sudah mengunduh model LLM dan *Embedding* yang menjadi otak sistem ini:
```bash
ollama pull bge-m3:latest
ollama pull gpt-oss:120b-cloud
```
*(Catatan: Anda bisa mengganti nama model `gpt-oss:120b-cloud` di file `config.py` sesuai dengan model yang terinstal di mesin Anda, misalnya `llama3` atau `mistral`).*

### 4. Menjalankan Aplikasi
Setiap kali ada perubahan struktur pada `db.csv`, pastikan Anda **menghapus** file `finance_predictor.db` di dalam folder `data/` agar sistem melakukan sinkronisasi ulang dengan bersih.

Mode demo mempertahankan perilaku saat ini dan memakai SQLite/CSV lokal:
```bash
python3 app.py --data-mode demo
```

Mode internal API mengambil data finansial internal dari endpoint API, lalu memproses OSINT hanya sebagai konteks eksternal:
```bash
DATA_ACQUISITION_MODE=internal_api \
INTERNAL_API_BASE_URL=https://internal.example.com \
INTERNAL_API_DATASET_PATH=/api/finance/invoices \
INTERNAL_API_AUTH_TOKEN=your_token \
python3 app.py
```

Jika response API dibungkus object, Anda bisa menambahkan `INTERNAL_API_RECORDS_KEY`, misalnya `data.items`.

Akses *dashboard* melalui *browser* di **`http://127.0.0.1:5000`**.

---

## Troubleshooting Umum
* **"Error: Flask mati / Tidak terhubung"**: Pastikan Anda membuka melalui URL `http://127.0.0.1:5000`, BUKAN dengan melakukan *double-click* pada file `index.html`.
* **Ollama Connection Refused**: Pastikan aplikasi Ollama berjalan di latar belakang (cek ikon tray di Windows/Mac).
* **KeyError saat Generate**: Hapus file `.db` (SQLite) di folder `data/` dan *restart* `app.py`. Ini terjadi jika CSV Anda memiliki nama kolom yang berbeda dengan format lama.
* **Financial data unavailable**: Pastikan mode data sesuai, lalu cek `INTERNAL_API_BASE_URL`, `INTERNAL_API_DATASET_PATH`, token, dan bentuk JSON response bila menggunakan internal API.
