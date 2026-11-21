# Dokumentasi & Penjelasan Lengkap Proyek Telemedicine Demo
> **Mata Kuliah / Topik:** NoSQL for Big Data (Pola: *Ingestion → Processing → NoSQL Storage → Analytics*)  
> **Teknologi Utama:** Python 3.12, Apache Spark (PySpark 3.5), Neo4j 5.26 Community, Docker, PyVis

---

## 1. Ikhtisar & Latar Belakang Proyek

Proyek ini mendemonstrasikan implementasi arsitektur **Big Data Pipeline** untuk data telemedisin (percakapan konsultasi antara dokter dan pasien). 

### Mengapa Memadukan Dokumen JSON, Spark, dan Graph Database (Neo4j)?
1. **Transkrip Chat Mentah (Document Model / JSON):**
   Percakapan konsultasi berbentuk semi-terstruktur, memiliki struktur bersarang (*nested array* pesan), dan kaya teks narasi. Format ini ideal disimpan sebagai dokumen JSON (atau MongoDB di tingkat produksi).
2. **Pemrosesan Terdistribusi (Apache Spark):**
   Spark digunakan untuk *ingestion* dan pembersihan data skala besar secara paralel. Spark membedah pesan percakapan, memfilter pesan yang dikirim oleh pasien saja, dan mengekstrak entitas medis (gejala) menggunakan pemrosesan teks.
3. **Penyimpanan Hubungan (Graph Database / Neo4j):**
   Database relasional (RDBMS) membutuhkan banyak operasi `JOIN` yang berat untuk menelusuri hubungan bertingkat, sedangkan *document store* sulit menavigasi relasi antar entitas yang saling silang. Neo4j menyimpan data dalam bentuk graf (node & relasi), sehingga pertanyaan relasional seperti *"Gejala apa yang sering muncul bersamaan dengan demam?"* atau *"Pasien mana yang memiliki kemiripan gejala dengan Pasien X?"* dapat dijawab dengan sangat cepat dan elegan menggunakan Cypher query.

---

## 2. Arsitektur Alur Data (Data Pipeline)

```
[ data/chats.json ]
       │
       │  (1. Ingestion)
       ▼
[ Apache Spark / PySpark ] ──► Pemfilteran peran & Ekstraksi Gejala (NLP/Regex)
       │
       │  (2. Processing)
       ▼
[ DataFrame Spark ]
       │
       │  (3. Storage - via Neo4j Spark Connector / Python Driver)
       ▼
[ Neo4j Graph Database ]
       │
       │  (4. Analytics & Visualization)
       ├─► Analitik Cypher (Gejala Populer, Komorbiditas, Beban Dokter)
       ├─► Visualisasi Graf Interaktif (graph.html via PyVis)
       └─► Demo Operasi CRUD Cypher (Insert, Find, Update, Delete)
```

---

## 3. Desain Model Graf (Graph Data Model)

Neo4j **tidak** menyimpan transkrip chat yang panjang, melainkan hanya menyimpan entitas inti dan relasi hasil ekstraksi Spark.

### A. Node Labels & Properti
- **`Pasien`** : `{ id: "P001" }`
- **`Dokter`** : `{ id: "D01" }`
- **`Gejala`** : `{ nama: "demam" }`

### B. Relationships (Tepi Relasi)
- **`(:Pasien)-[:MENGALAMI {chat_id: "C001", waktu: "..."}]->(:Gejala)`**
  Menghubungkan pasien dengan gejala yang dialaminya pada sesi konsultasi tertentu.
- **`(:Pasien)-[:DIPERIKSA_OLEH]->(:Dokter)`**
  Menghubungkan pasien dengan dokter yang menangani konsultasinya.

```
       (:Dokter)
           ▲
           │ [DIPERIKSA_OLEH]
           │
       (:Pasien) ──[MENGALAMI {chat_id, waktu}]──► (:Gejala)
```

---

## 4. Struktur Direktori Proyek

```text
telemed-demo/
├── data/
│   └── chats.json              # Data mentah dummy konsultasi telemedisin
├── src/
│   ├── common.py               # Konfigurasi koneksi Neo4j, Spark, dan path
│   ├── symptoms.py             # Kamus gejala & logika ekstraksi regex Spark
│   ├── 01_test_neo4j.py        # Uji koneksi ke Neo4j & inisialisasi constraint
│   ├── 02_extract_symptoms.py  # Uji pengolahan Spark murni (tanpa Neo4j)
│   ├── 03_load_with_spark.py   # Spark memuat hasil olahan ke Neo4j via Spark Connector
│   ├── 03b_load_with_driver.py # Alternatif: menulis ke Neo4j via Python Driver
│   ├── 04_queries_and_viz.py   # Analisis query Cypher & pembuatan graph.html
│   └── 05_crud_demo.py         # Demonstrasi CRUD Cypher setara MongoDB Lab
├── docker-compose.yml          # Konfigurasi container database Neo4j 5.26
├── pyproject.toml              # Definisi dependensi Python proyek (uv)
├── graph.html                  # Hasil visualisasi graf interaktif HTML
├── README.md                   # Panduan cepat penggunaan
└── PENJELASAN_PROYEK.md        # Dokumen ini (penjelasan mendalam arsitektur)
```

---

## 5. Rincian Teknis Setiap Modul Script

### 1. `src/symptoms.py` (Logika Ekstraksi Gejala)
- Menggunakan 11 kategori gejala: *demam, batuk, sesak napas, pusing, mual, nyeri dada, sakit tenggorokan, diare, ruam, gatal, lemas*.
- **Penanganan Negasi (Negative Lookbehind):**
  Menggunakan regex Java `(?<!tidak )(?<!tidak ada )(?<!tanpa )(?<!belum ada )\b(?:gejala)`. 
  Contoh: Kalimat *"Saya demam tetapi tidak ada sesak"* hanya akan mendeteksi **demam**, sedangkan **sesak** diabaikan karena didahului frasa negasi.
- **Filter Peran Pesan:**
  Hanya pesan berlabel `sender: "patient"` yang dianalisis, mencegah pertanyaan dokter (misal: *"Apakah Anda mual?"*) disalahartikan sebagai gejala pasien.

### 2. `src/common.py` (Konfigurasi Terpusat)
- Mengatur URI, user, dan password Neo4j (default: `bolt://localhost:7687`, user `neo4j`, password `telemed123`).
- Menangani path otomatis Python driver Spark worker.
- **Dukungan Windows Otomatis:** Menyetel `HADOOP_HOME` dan `winutils.exe` ke `PATH` agar Spark di Windows tidak crash saat memproses JAR konektor.
- Mengatur *Unique Constraints* pada Neo4j (`Pasien.id`, `Dokter.id`, `Gejala.nama`) agar eksekusi `MERGE` tidak menghasilkan node duplikat.

### 3. `src/01_test_neo4j.py` (Verifikasi Lingkungan Neo4j)
- Memastikan port Bolt `7687` terbuka dan kredensial valid.
- Membuat *constraint* keunikan.
- Melakukan pengujian tulis-baca sementara pada label `UjiPasien` lalu menghapusnya kembali.

### 4. `src/02_extract_symptoms.py` (Uji Spark Standalone)
- Membaca `data/chats.json` dengan opsi `multiLine=True`.
- Menjalankan fungsi `extract_symptoms()` dan menampilkan DataFrame tabular per baris `(chat_id, patient_id, doctor_id, gejala)`.
- Menghitung agregasi frekuensi gejala langsung menggunakan Spark SQL tanpa melibatkan database.

### 5. `src/03_load_with_spark.py` & `src/03b_load_with_driver.py` (Ingestion ke Neo4j)
- **`03_load_with_spark.py`**: Menggunakan *Neo4j Connector for Apache Spark* (`org.neo4j:neo4j-connector-apache-spark_2.12:5.4.3_for_spark_3`). Spark menulis langsung secara terdistribusi menggunakan query Cypher `UNWIND $events AS event MERGE ...`.
- **`03b_load_with_driver.py`**: Skrip cadangan (*fallback*) jika konektor Spark mengalami kendala unduhan Maven. Menggunakan PySpark untuk transformasi, lalu `neo4j-python-driver` untuk menulis batch data ke Neo4j.

### 6. `src/04_queries_and_viz.py` (Analitik Graf & Visualisasi)
Menjalankan 4 query Cypher analitik utama:
1. **Top Gejala:** Menghitung gejala dengan frekuensi tertinggi di seluruh sesi konsultasi.
2. **Gejala Penyerta (Co-occurrence):** Mencari pola gejala yang muncul bersamaan dalam satu percakapan (misal: gejala apa saja yang sering menyertai demam).
3. **Pasien Mirip (Collaborative Graph Traversal):** Menemukan pasien lain yang memiliki irisan gejala yang sama dengan pasien target tanpa memerlukan `JOIN` SQL yang kompleks.
4. **Beban Dokter:** Menghitung jumlah pasien unik yang ditangani tiap dokter.
5. **Visualisasi PyVis (`graph.html`):** Membaca seluruh relasi dan merendernya ke graf interaktif HTML berbasis `vis.js` mandiri (*self-contained inline JS*).

### 7. `src/05_crud_demo.py` (Demonstrasi Operasi CRUD)
Memberikan perbandingan ekuivalensi operasi Cypher dengan operasi NoSQL Document (seperti Lab MongoDB):
- **Create:** `MERGE (p:Pasien {id: 'P999'}) ...` *(setara `insertOne` / `insertMany`)*
- **Read:** `MATCH (p:Pasien) WHERE p.usia > 25 ...` *(setara `find()` dengan operator `$gt`)*
- **Update:** `MATCH (p:Pasien {id: 'P999'}) SET p.usia = 31` *(setara `updateOne()` dengan `$set`)*
- **Delete:** `MATCH (p:Pasien {id: 'P999'}) DETACH DELETE p` *(setara `deleteOne()` dengan integritas relasi)*

---

## 6. Panduan Menjalankan Proyek Langkah demi Langkah

### Prasyarat
- **Docker Desktop** (sudah menyala)
- **Java 17** (terpasang di sistem dan `JAVA_HOME` aktif)
- **uv** (Package Manager Python)

### Eksekusi Lengkap:
```bash
# 1. Jalankan container database Neo4j di background
docker compose up -d

# 2. Uji koneksi dan buat constraint keunikan
uv run python src/01_test_neo4j.py

# 3. Uji ekstraksi gejala di Apache Spark
uv run python src/02_extract_symptoms.py

# 4. Tulis hasil ekstraksi Spark ke Neo4j
uv run python src/03_load_with_spark.py
# (atau jika konektor JAR bermasalah, gunakan: uv run python src/03b_load_with_driver.py)

# 5. Jalankan query analitik dan buat file visualisasi
uv run python src/04_queries_and_viz.py

# 6. Buka hasil visualisasi graf di browser
# Buka file graph.html langsung di browser Anda

# 7. Jalankan demo CRUD
uv run python src/05_crud_demo.py
```

Untuk mematikan container:
```bash
docker compose down
```

---

## 7. Catatan Teknis & Solusi Masalah Khusus Windows

- **Hadoop WinUtils Error:**  
  Spark di Windows memerlukan utilitas POSIX emulator `winutils.exe` dan `hadoop.dll` di folder `%HADOOP_HOME%\bin` untuk menangani izin file berkas JAR sementara. Proyek ini telah mengonfigurasi `C:\hadoop\bin` dan otomatis memuatnya melalui [src/common.py](file:///c:/Users/Chwyper/Downloads/telemed-demo/telemed-demo/src/common.py).
- **Graph Kosong di Skrip 04:**  
  Jika skrip 04 menghasilkan `(kosong)` dan `0 relasi`, hal ini menandakan database Neo4j belum terisi data. Pastikan langkah `03` atau `03b` dijalankan dengan sukses terlebih dahulu.

---

## 8. Poin Pembahasan Presentasi & Tanya-Jawab (Defense Q&A)

1. **Kenapa chat tidak disimpan langsung di Neo4j?**
   > Graph database dioptimalkan untuk relasi antar entitas, bukan untuk pencarian dokumen teks panjang (*document retrieval*). Menyimpan transkrip mentah di graf akan membebani memori tanpa memberi manfaat relasional. Teks mentah idealnya berada di Object Storage / Document DB (MongoDB), sedangkan representasi grafnya (Pasien - Gejala - Dokter) disimpan di Neo4j.
2. **Bagaimana skalabilitas ekstraksi gejala di Spark?**
   > Pendekatan saat ini menggunakan dictionary-based regex untuk keperluan demonstrasi. Untuk skala data jutaan percakapan, kamus kata diganti dengan model NLP bahasa Indonesia (seperti IndoBERT / HuggingFace Pipeline) yang dibungkus dalam Spark Pandas UDF (*User Defined Functions*) agar inferensi model berjalan terdistribusi di seluruh worker node Spark.
3. **Mengapa menggunakan `MERGE` bukan `CREATE`?**
   > `MERGE` bersifat idempoten (*idempotent*): node/relasi hanya dibuat jika belum ada di graf. Ini memastikan pipeline aman dijalankan berulang kali tanpa risiko duplikasi data.
