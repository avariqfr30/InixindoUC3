# Historical Revenue Predictor (Internal Tool)

Sistem *Enterprise AI* berbasis web yang dirancang khusus untuk divisi Finance dan Executive Board (CFO) Inixindo Jogja. Aplikasi ini menelan **seluruh histori data penagihan (All-Time)** tanpa batasan kuartal, untuk memprediksi risiko arus kas, membedah pola keterlambatan pembayaran lintas demografi, dan menghasilkan strategi mitigasi likuiditas secara otomatis.

Berbeda dengan sistem analitik tradisional, AI ini tidak hanya menghitung angka, tetapi membaca *konteks* dari catatan penagihan dan membandingkannya dengan tren siklus anggaran makro di Indonesia (Pemerintah, BUMN, dan Swasta) untuk memprediksi pergeseran **Kelas Pembayaran (Kelas A hingga Kelas E)**.

## Fitur Utama

* **All-Time Macro Analysis**: Menarik dan mensintesis 100% data historis perusahaan secara serentak untuk menemukan pola tunda bayar (*bottlenecks*) jangka panjang.
* **Payment Class Profiling**: Secara cerdas membedah pergeseran perilaku klien dari Kelas A (Tepat Waktu) hingga Kelas E (Macet > 6 Bulan).
* **OSINT Budget Cycle Context**: Menggunakan *Google Custom Search API* untuk mencari tahu tren siklus pencairan anggaran APBN/BUMN saat ini dan menggabungkannya dengan histori internal.
* **CFO-Level Auto-Reporting**: Menghasilkan dokumen Microsoft Word (*Strictly Confidential*) berskala eksekutif yang dilengkapi dengan grafik distribusi *Bar Chart*, *Flowchart* mitigasi, dan tipografi tata letak profesional rata kanan-kiri.
* **Smart Prompt Suggestions**: Menyediakan cip instruksi makro otomatis agar manajemen tidak perlu repot merangkai *prompt* analisis dari nol.

## Prasyarat Sistem

* **Python 3.9+** (Untuk *deployment* lokal).
* **Ollama**: Berjalan di *background* pada port `11434`.
* **Kredensial Google Custom Search**: `API_KEY` dan `CX_ID` aktif di `config.py`.
* **Database Historis**: File CSV berisi data penagihan.

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
*(Atau instal manual: `pip install flask flask-cors pandas chromadb ollama matplotlib python-docx markdown beautifulsoup4 requests Pillow sqlalchemy`)*

### 3. Menyiapkan Model Ollama (Wajib)
Pastikan Anda sudah mengunduh model LLM dan *Embedding* yang menjadi otak sistem ini:
```bash
ollama pull bge-m3:latest
ollama pull gpt-oss:120b-cloud
```
*(Catatan: Anda bisa mengganti nama model `gpt-oss:120b-cloud` di file `config.py` sesuai dengan model yang terinstal di mesin Anda, misalnya `llama3` atau `mistral`).*

### 4. Menjalankan Aplikasi
Setiap kali ada perubahan struktur pada `db.csv`, pastikan Anda **menghapus** file `finance_predictor.db` di dalam folder `data/` agar sistem melakukan sinkronisasi ulang dengan bersih.

Jalankan server Flask:
```bash
python app.py
```
Akses *dashboard* melalui *browser* di **`http://127.0.0.1:5000`**.

---

## Troubleshooting Umum
* **"Error: Flask mati / Tidak terhubung"**: Pastikan Anda membuka melalui URL `http://127.0.0.1:5000`, BUKAN dengan melakukan *double-click* pada file `index.html`.
* **Ollama Connection Refused**: Pastikan aplikasi Ollama berjalan di latar belakang (cek ikon tray di Windows/Mac).
* **KeyError saat Generate**: Hapus file `.db` (SQLite) di folder `data/` dan *restart* `app.py`. Ini terjadi jika CSV Anda memiliki nama kolom yang berbeda dengan format lama.
