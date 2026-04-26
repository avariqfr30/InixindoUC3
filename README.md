# Cash In Intelligence (Internal Tool)

Sistem *Enterprise AI* berbasis web yang dirancang untuk divisi Finance dan Executive Board (CFO) Inixindo Jogja. Aplikasi ini membaca histori data cash in dan karakter penagihan untuk menghasilkan analisis **deskriptif, diagnostik, prediktif, dan preskriptif** terhadap arus kas masuk.

Berbeda dengan sistem analitik tradisional, AI ini tidak hanya menghitung angka, tetapi membaca *konteks* dari catatan penagihan dan membandingkannya dengan tren siklus anggaran makro di Indonesia (Pemerintah, BUMN, dan Swasta) untuk memprediksi pergeseran **Kelas Pembayaran (Kelas A hingga Kelas E)**.

## Fitur Utama

* **Cash In Intelligence**: Mengubah histori invoice, kelas pembayaran, dan catatan penagihan menjadi laporan deskriptif, diagnostik, prediktif, dan preskriptif.
* **Payment Class Profiling**: Membedah perilaku klien dari Kelas A (Tepat Waktu) hingga Kelas E (Macet > 6 Bulan).
* **OSINT Budget Cycle Context**: Menggunakan *Serper API* (multi-query `search` + `news`) untuk memperkaya konteks tren anggaran, perilaku pembayaran, dan sinyal risiko likuiditas di Indonesia.
* **Shared Stress-Test Ready**: Mendukung job queue di sisi server agar beberapa pengguna bisa menjalankan generate report secara bersamaan dengan UI yang tetap sederhana.
* **VPS Simulation Hardening**: Metadata job disimpan di SQLite terpisah, file `.docx` hasil generate disimpan ke disk, dan antrean dapat dibatasi agar stress test gagal secara terkontrol saat kapasitas penuh.
* **CFO-Level Auto-Reporting**: Menghasilkan dokumen Microsoft Word (*Strictly Confidential*) dengan daftar isi, heading terstruktur, numbering/bullet bawaan Word, tabel, grafik *Bar Chart*, dan *Flowchart* mitigasi.
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
*(Atau instal manual: `pip install flask flask-cors pandas chromadb ollama matplotlib python-docx markdown beautifulsoup4 requests sqlalchemy waitress`)*

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

Mode internal API mengambil data finansial internal dari endpoint API, lalu memproses OSINT hanya sebagai konteks eksternal. Untuk handover, cara paling sederhana adalah memakai satu file profile produksi:
```bash
cp "Payment predictor/deployment/internal-api.production.example.json" /etc/payment-app.production.json
sudo nano /etc/payment-app.production.json
```

Isi bagian ini di JSON profile:
* `endpoint.url`: endpoint APIDog/API perusahaan
* `endpoint.method`: biasanya `POST`
* `endpoint.records_key`: path array record utama, misalnya `data.dataset_result`
* `auth.basic_username` dan `auth.basic_password`, atau `auth.bearer_token`
* `request.body`: body POST yang diminta backend, misalnya `{"dataset_code":"ClassReport"}`
* `field_map`: kosongkan dulu bila nama field API mudah ditebak; isi hanya jika validasi belum siap

Lalu arahkan service ke profile itu:
```bash
DATA_ACQUISITION_MODE=internal_api \
INTERNAL_API_CONFIG_FILE=/etc/payment-app.production.json \
python3 app.py
```

Demo mode tetap tersedia untuk testing/rollback:
```bash
DATA_ACQUISITION_MODE=demo python3 app.py
```

Mode env-var lama tetap didukung bila tidak ingin memakai file JSON:
```bash
DATA_ACQUISITION_MODE=internal_api \
INTERNAL_API_ENDPOINT_URL=https://internal.example.com/api/Resource/dataset \
INTERNAL_API_METHOD=POST \
INTERNAL_API_BASIC_USERNAME=your_username \
INTERNAL_API_BASIC_PASSWORD=your_password \
INTERNAL_API_BODY_JSON='{"dataset_code":"ClassReport"}' \
python3 app.py
```

Jika response API dibungkus object atau array utama tidak ada di root JSON, Anda bisa menambahkan `INTERNAL_API_RECORDS_KEY`, misalnya `data.items` atau `payload.data[0].rows`.

Jika nama field dari API internal berbeda dengan schema demo saat ini, Anda tidak perlu mengubah kode aplikasi. Cukup isi mapping field:
```bash
INTERNAL_API_FIELD_MAP_JSON='{"period":"report_period","partner_type":"customer_segment","service":"service_name","payment_class":"collection_bucket","invoice_value":"amount_idr","delay_note":"delay_reason"}'
```

Kalau tidak diisi, app sekarang akan mencoba menebak sendiri:
* array/object record mana yang paling relevan dari JSON response
* field mana yang tampaknya mewakili periode, partner, layanan, kelas pembayaran, nilai invoice, dan catatan keterlambatan

Dengan kata lain, untuk banyak endpoint internal nanti alurnya cukup:
1. edit satu file `internal-api.production.json`
2. set `DATA_ACQUISITION_MODE=internal_api` dan `INTERNAL_API_CONFIG_FILE=/path/profile.json`
3. login ke app lalu validasi source `production`
4. cek `GET /api/internal-data/contract`
5. tambahkan `endpoint.records_key` atau `field_map` hanya jika inference belum tepat
6. aktifkan source `production` setelah `isReady=true`

Cara termudah untuk tim internal nanti adalah mengikuti key canonical berikut langsung di response API:
* `period`
* `partner_type`
* `service`
* `payment_class`
* `invoice_value`
* `delay_note`

App juga sekarang menyediakan kontrak schema yang bisa dibuka setelah login:
* `GET /api/internal-data/contract`

Endpoint ini menampilkan:
* field yang wajib
* alias yang masih diterima
* contoh response
* template `INTERNAL_API_FIELD_MAP_JSON`
* template JSON profile produksi
* `fieldMapSuggestionJson` dari payload aktif
* checklist handover API internal
* endpoint env var yang bisa dipakai
* env var method/auth/body untuk endpoint non-GET
* ringkasan apakah dataset aktif saat ini sudah memenuhi kontrak atau belum
* path record JSON yang terdeteksi dari payload aktif

Untuk sesi uji bersama di jaringan internal perusahaan, gunakan Waitress dan bind aplikasi ke semua interface:
```bash
python3 app.py --data-mode demo --server waitress --host 0.0.0.0 --port 5000
```

Atau dengan internal API dan worker queue yang sedikit lebih agresif untuk stress test:
```bash
DATA_ACQUISITION_MODE=internal_api \
INTERNAL_API_BASE_URL=https://internal.example.com \
INTERNAL_API_DATASET_PATH=/api/finance/invoices \
INTERNAL_API_AUTH_TOKEN=your_token \
REPORT_MAX_CONCURRENT_JOBS=4 \
REPORT_MAX_PENDING_JOBS=12 \
WAITRESS_THREADS=12 \
python3 app.py --server waitress --host 0.0.0.0 --port 5000
```

Akses *dashboard* melalui *browser* di **`http://127.0.0.1:5000`**.

### Konfigurasi yang Berguna untuk Simulasi VPS
Anda bisa menyesuaikan perilaku antrean dan penyimpanan artefak dengan environment variable berikut:

```bash
REPORT_MAX_CONCURRENT_JOBS=4
REPORT_MAX_PENDING_JOBS=12
REPORT_JOB_RETENTION_SECONDS=3600
REPORT_METRICS_WINDOW_HOURS=24
REPORT_MIN_COMPLETENESS_SCORE=80
DATA_REFRESH_INTERVAL_SECONDS=300
FORECAST_CACHE_TTL_SECONDS=300
REPORT_ARTIFACTS_DIR=/var/tmp/inixindo-generated-reports
JOB_STATE_DB_PATH=/var/tmp/inixindo-report-jobs.db
```

Arti singkatnya:
* `REPORT_MAX_CONCURRENT_JOBS`: jumlah job generate yang boleh berjalan bersamaan.
* `REPORT_MAX_PENDING_JOBS`: batas total job aktif (`queued` + `running`). Di atas batas ini, app akan mengembalikan `429` agar load spike tidak membuat sistem tidak responsif.
* `REPORT_JOB_RETENTION_SECONDS`: berapa lama job selesai/error dan file hasilnya dipertahankan sebelum dibersihkan otomatis.
* `REPORT_METRICS_WINDOW_HOURS`: jendela waktu untuk metrik kesehatan terakhir pada endpoint `/health`.
* `REPORT_MIN_COMPLETENESS_SCORE`: ambang minimum kualitas dokumen. Job dianggap lolos bila struktur dan isi laporan mencapai skor ini.
* `DATA_REFRESH_INTERVAL_SECONDS`: interval refresh data live di background. Set `0` untuk tetap manual.
* `FORECAST_CACHE_TTL_SECONDS`: berapa lama snapshot forecast/dashboard disimpan agar dashboard tetap cepat dibuka ulang oleh beberapa user.
* `REPORT_ARTIFACTS_DIR`: direktori penyimpanan file `.docx` hasil generate.
* `JOB_STATE_DB_PATH`: SQLite kecil untuk status job, durasi, fallback, dan metrik operasional.

Jika sumber `cash out` aktual sudah tersedia, app sekarang juga bisa membaca feed kewajiban langsung dari endpoint terpisah:

```bash
CASH_OUT_API_ENDPOINT_URL=https://internal.example.com/api/finance/cash-out \
CASH_OUT_API_METHOD=POST \
CASH_OUT_API_BASIC_USERNAME=your_user \
CASH_OUT_API_BASIC_PASSWORD=your_pass \
CASH_OUT_API_BODY_JSON='{"resource":"cashout"}' \
CASH_OUT_API_RECORDS_KEY=data.items \
CASH_OUT_FIELD_MAP_JSON='{"amount":"amount_idr","due_date":"due_date","category":"expense_category","reference":"document_no","status":"status"}'
```

Kalau feed `cash out` belum diset, app tetap berjalan dengan model operating cost bulanan seperti sebelumnya.

Endpoint operasional yang sekarang berguna untuk dashboard live:
* `GET /health`: status job + freshness sinkronisasi data finansial dan feed cash out.
* `GET /get-config`: review context + status sinkronisasi yang dipakai UI.
* `POST /refresh-knowledge`: refresh manual data finansial dan feed cash out.
* `POST /api/forecast/drilldown/top-overdue`: daftar overdue utama untuk horizon aktif.
* `GET /api/forecast/drilldown/payment-class-trend`: distribusi nominal invoice per periode dan kelas pembayaran.
* `POST /api/forecast/drilldown/concentration`: konsentrasi exposure per partner dan layanan.

---

## Troubleshooting Umum
* **"Error: Flask mati / Tidak terhubung"**: Pastikan Anda membuka melalui URL `http://127.0.0.1:5000`, BUKAN dengan melakukan *double-click* pada file `index.html`.
* **Ollama Connection Refused**: Pastikan aplikasi Ollama berjalan di latar belakang (cek ikon tray di Windows/Mac).
* **KeyError saat Generate**: Hapus file `.db` (SQLite) di folder `data/` dan *restart* `app.py`. Ini terjadi jika CSV Anda memiliki nama kolom yang berbeda dengan format lama.
* **Financial data unavailable**: Pastikan mode data sesuai, lalu cek `INTERNAL_API_BASE_URL`, `INTERNAL_API_DATASET_PATH`, token, dan bentuk JSON response bila menggunakan internal API.
* **Internal API mengembalikan `data: null`**: artinya endpoint dan kredensial sudah benar, tetapi backend internal masih menunggu body POST yang spesifik. Isi `INTERNAL_API_BODY_JSON` sesuai payload yang diberikan tim backend.
* **Generate terasa lambat saat banyak user**: Turunkan ukuran model, kecilkan `REPORT_NUM_PREDICT`, atau sesuaikan `REPORT_MAX_CONCURRENT_JOBS` dengan kapasitas mesin yang menjalankan Ollama.
* **Queue penuh saat stress test**: Tingkatkan `REPORT_MAX_PENDING_JOBS` bila antrean memang ingin diperbolehkan lebih panjang, atau turunkan jumlah pengguna simultan bila target waktu 3-4 menit mulai meleset.
* **Artefak hasil report tidak ditemukan**: Pastikan `REPORT_ARTIFACTS_DIR` dapat ditulis oleh user proses aplikasi, dan `JOB_STATE_DB_PATH` mengarah ke lokasi yang persisten di VPS.
* **Waitress tidak jalan**: Pastikan dependensi terbaru sudah terpasang dengan `pip install -r requirements.txt`, lalu jalankan ulang dengan `--server waitress`.
