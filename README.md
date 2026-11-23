# Demo Telemedicine: Spark + Neo4j (Modul 9 NoSQL for Big Data)

Pipeline Big Data untuk chat konsultasi dokter-pasien, mengikuti pola di modul:
*ingestion → processing → NoSQL storage → analytics*.

```
data/chats.json  atau  data/synthetic/ (100 ribu chat)  atau  data/stream/ (chat masuk terus)
        │  ingestion
        ▼
  Spark: baca JSON, ambil pesan pasien, ekstrak gejala (kamus + negasi)
        │  processing
        ▼
  Neo4j (graph)   (Pasien)-[:MENGALAMI {chat_id, waktu}]->(Gejala)
        │           (Pasien)-[:DIPERIKSA_OLEH]->(Dokter)        NoSQL storage
        ▼
  Query Cypher + graph.html + demo CRUD                         analytics
```

Chat mentah **tidak** disimpan di Neo4j. Neo4j menyimpan *hubungan* hasil olahan. Alasannya:
graph unggul untuk pertanyaan relasi (gejala penyerta, pasien mirip), bukan untuk menyimpan
transkrip utuh. Jelaskan ini apa adanya saat presentasi.

## Isi proyek

| File | Fungsi |
|---|---|
| `data/chats.json` | 12 chat dummy (demo cepat dan uji kebenaran manual) |
| `src/generate_chats.py` | Generator chat sintetis: mode **Volume** (100 ribu) dan mode **Velocity** (stream) |
| `src/common.py` | Konfigurasi, SparkSession, driver Neo4j, `baca_chat()` |
| `src/symptoms.py` | Kamus gejala + ekstraksi dengan penanganan negasi |
| `src/01_test_neo4j.py` | Uji koneksi Neo4j + constraint |
| `src/02_extract_symptoms.py` | Spark saja (tanpa Neo4j), mencetak waktu dan jumlah partisi |
| `src/03_load_with_spark.py` | Spark → Neo4j lewat konektor, lalu baca balik |
| `src/03b_load_with_driver.py` | Cadangan: tulis lewat driver Python (per batch) |
| `src/04_queries_and_viz.py` | 4 query analitik + `graph.html` |
| `src/05_crud_demo.py` | CRUD Cypher (setara lab MongoDB di modul) |
| `src/06_evaluate_extraction.py` | **Veracity**: precision/recall ekstraksi terhadap label asli (data sintetis) |
| `src/07_stream_to_neo4j.py` | **Velocity**: Spark Structured Streaming → Neo4j per micro-batch |
| `PENJELASAN_PROYEK.md` | Penjelasan arsitektur lebih dalam dan bahan tanya-jawab |

## Prasyarat

| Kebutuhan | Versi | Cek |
|---|---|---|
| Docker Desktop | terbaru, harus sedang berjalan | `docker --version` |
| Java | **17** (disarankan, sesuai rencana kelompok) | `java -version` |
| uv | terbaru | `uv --version` |

Python 3.12 diatur otomatis oleh uv lewat `.python-version`. Python 3.14 di sistem tidak
masalah, proyek ini tidak memakainya (PySpark 3.5 dan konektor 5.4.3 hanya mendukung
Python 3.9 sampai 3.13). Jalankan semua skrip lewat `uv run`, bukan `python` langsung.

## Langkah menjalankan

### A. Demo dasar (12 chat)

```bash
uv sync                                   # 0. environment Python
docker compose up -d                      # 1. Neo4j; tunggu ~30 detik
docker compose logs -f neo4j              #    tunggu "Started." lalu Ctrl+C
uv run python src/01_test_neo4j.py        # 2. uji koneksi + constraint
uv run python src/02_extract_symptoms.py  # 3. uji Spark saja
uv run python src/03_load_with_spark.py   # 4. Spark -> Neo4j (run pertama mengunduh JAR)
#   kalau konektor bermasalah: uv run python src/03b_load_with_driver.py
uv run python src/04_queries_and_viz.py   # 5. query + graph.html (buka di browser)
uv run python src/05_crud_demo.py         # 6. demo CRUD
```

### B. Volume: 100 ribu chat

```bash
uv run python src/generate_chats.py --n 100000 --out data/synthetic   # beberapa detik, ~43 MB

# Semua skrip membaca sumber dari variabel CHATS_SOURCE (default: data/chats.json).
# Linux/macOS:
CHATS_SOURCE=data/synthetic uv run python src/02_extract_symptoms.py
CHATS_SOURCE=data/synthetic uv run python src/03_load_with_spark.py
# Windows PowerShell:
#   $env:CHATS_SOURCE="data/synthetic"; uv run python src/02_extract_symptoms.py
```

Sebelum memuat data besar, kosongkan Neo4j kalau perlu: `docker compose down -v` lalu `up -d` lagi.
Skrip `02`, `03`, dan `03b` mencetak waktu proses. **Catat angka dari laptop kalian sendiri**
untuk slide Volume; jangan memakai angka dari orang lain.

### C. Velocity: chat masuk terus

```bash
# Terminal 1: produsen, 500 chat baru tiap 5 detik
uv run python src/generate_chats.py --stream --batch 500 --interval 5 --out data/stream
# Terminal 2: konsumen
uv run python src/07_stream_to_neo4j.py             # menulis ke Neo4j
uv run python src/07_stream_to_neo4j.py --dry-run   # hanya menghitung, tanpa Neo4j
```

Saat berjalan, jalankan `MATCH ()-[m:MENGALAMI]->() RETURN count(m)` di Neo4j berulang kali:
angkanya naik. Hentikan dengan Ctrl+C. Ini streaming berbasis file (Spark memantau folder),
bukan Kafka.

### D. Veracity: ukur akurasi ekstraksi

```bash
CHATS_SOURCE=data/synthetic uv run python src/06_evaluate_extraction.py
```

Menghentikan Neo4j: `docker compose down` (data tetap). Mengosongkan semuanya: `docker compose down -v`.

## 5V di proyek ini

| V | Bagaimana ditunjukkan | Skrip | Batasan yang harus disebut |
|---|---|---|---|
| **Volume** | 100 ribu chat (±260 ribu pasangan chat-gejala), diproses Spark dalam banyak partisi | `generate_chats.py`, `02`, `03` | Data sintetis dan Spark mode lokal (1 mesin), bukan klaster. Ini bukan petabyte |
| **Velocity** | Chat datang terus, graph ter-update per micro-batch | `generate_chats.py --stream`, `07` | Streaming berbasis file, bukan Kafka. Laju ditentukan skrip generator |
| **Variety** | JSON semi-terstruktur bersarang, teks bebas, dan graph relasi | `chats.json`, Neo4j | Metadata gambar diagnostik (di skenario modul) **tidak** didemokan |
| **Veracity** | Negasi ("tidak ada sesak"), pesan dokter diabaikan, dedup lewat `MERGE`, serta precision/recall terhadap label | `symptoms.py`, `06` | Lihat catatan di bawah |
| **Value** | Gejala teratas, gejala penyerta, pasien mirip, beban dokter | `04` | Data dummy, tidak ada klaim klinis |

**Catatan Veracity:** data sintetis dibuat dengan kalimat negasi sederhana yang memang bisa
ditangani regex, jadi `06` menghasilkan precision dan recall 1.0. Itu hasil yang melingkar dan
**tidak membuktikan akurasi pada chat nyata**. Sebutkan itu di slide, atau tambahkan kalimat
sulit ("sama sekali bebas dari sesak") dan tunjukkan akurasinya turun.

## Yang sudah diuji dan yang belum

Diuji di sandbox pembuat (PySpark 3.5.10, Python 3.12, Java 21, tanpa Docker):

- `generate_chats.py`: mode 100 ribu menghasilkan 100.000 baris valid (20 file, ±43 MB).
- `02_extract_symptoms.py`: jalan di `chats.json` (12 chat → 32 pasangan chat-gejala) dan di
  100 ribu chat (→ 262.798 pasangan, 20 partisi, ±33 detik di mesin sandbox, mode lokal).
- `06_evaluate_extraction.py`: jalan, hasil 1.0 pada data sintetis (lihat catatan di atas).
- `07_stream_to_neo4j.py --dry-run`: jalan, membaca file yang datang dan memproses per micro-batch.
- Semua skrip lolos pemeriksaan sintaks.

**Belum diuji** (sandbox tidak punya Docker dan tidak bisa mengakses Maven Central):

- Semua yang bicara ke Neo4j: `01`, `03`, `03b`, `04`, `05`, dan `07` tanpa `--dry-run`.
- Konektor Spark-Neo4j.
- Memuat 100 ribu chat ke Neo4j. `03` memakai `coalesce(1)` (satu penulis, menghindari deadlock),
  jadi kemungkinan lambat; ukur sendiri. Kalau terlalu lambat, kecilkan `--n` ke 10 ribu.

Jalankan dulu bagian A sampai sukses sebelum bagian B dan C. Kalau ada error, kirim pesan error lengkapnya.

## Troubleshooting

| Gejala | Kemungkinan penyebab | Tindakan |
|---|---|---|
| `01` gagal: connection refused / Unable to retrieve routing | Neo4j belum selesai start | Tunggu, cek `docker compose logs neo4j` |
| `docker compose pull` gagal: tag tidak ditemukan | Tag image berubah | Cek tag `neo4j` di Docker Hub, ganti di `docker-compose.yml` |
| Auth gagal | Password di compose berbeda dengan skrip | Samakan, atau set env `NEO4J_PASSWORD` |
| Port 7687/7474 sudah dipakai | Neo4j lain berjalan | Hentikan, atau ganti port di compose dan env `NEO4J_URI` |
| `JAVA_HOME is not set` / Spark tidak jalan | Java belum terpasang | Pasang Java 17, set `JAVA_HOME` |
| `NoSuchMethodError` / `ClassNotFoundException` di `03` | Versi Scala konektor tidak cocok | PySpark pip 3.5 memakai Scala 2.12. Kalau tetap error, pakai `03b` |
| `03` macet saat mengunduh | Tidak ada internet / diblokir | Pakai `03b` |
| Error worker Python / versi Python berbeda | Spark memakai Python sistem | Jalankan lewat `uv run`; `common.py` mengatur `PYSPARK_PYTHON` |
| `http://localhost:7474` tidak menampilkan Browser | Edisi Community tidak selalu menyertakan Browser | Pakai `graph.html` dari skrip 04 |
| Windows: error `winutils` / `HADOOP_HOME` | Spark di Windows | `common.py` memakai `C:\hadoop\bin` kalau ada; pasang winutils di sana |
| Data 100 ribu: `graph.html` berat / browser macet | Terlalu banyak node untuk divisualisasikan | Visualisasikan subset (mis. beberapa pasien) atau pakai data 12 chat untuk grafik |
| `07` tidak memproses apa-apa | Generator belum berjalan atau folder salah | Jalankan generator dengan `--out data/stream` dan perhatikan folder yang sama |

## Keterbatasan (bahan tanya-jawab)

1. **Ekstraksi gejala berbasis kamus** itu sederhana. Negasi langsung di depan kata kunci
   ("tidak/tanpa") ditangani, tetapi kalimat rumit akan salah. Produksi memakai model NLP/NER
   bahasa Indonesia.
2. **Semua data adalah dummy atau sintetis.** Tidak ada data pasien nyata (data kesehatan itu
   sensitif). Tidak ada klaim performa selain angka yang kalian ukur sendiri.
3. **Spark berjalan lokal di satu mesin.** 100 ribu chat menunjukkan alur dan pemartisian, bukan
   skala klaster. Jangan menyebutnya "petabyte-scale".
4. **Chat mentah tidak ada di database.** Keputusan desain: dokumen chat cocok untuk document
   store (MongoDB), relasi gejala cocok untuk graph. MongoDB tidak didemokan di proyek ini.
5. **Cassandra tidak didemokan.** Bahas konsep saja: cocok untuk metadata gambar (tulis tinggi,
   akses berbasis key), bukan untuk query relasi.
6. **RAG belum dibuat.** Query Cypher di `04` (gejala penyerta, pasien mirip) bisa menjadi
   komponen retrieval GraphRAG. Langkah berikutnya: embedding + vector index + LLM. Sampai itu
   dikerjakan dan diuji, jangan masukkan RAG sebagai fitur di slide.
