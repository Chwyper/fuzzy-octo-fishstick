# Demo Telemedicine: Spark + Neo4j (Modul 9 NoSQL for Big Data)

Alur data (sesuai pola di modul: *ingestion → processing → NoSQL storage → analytics*):

```
data/chats.json  --(Spark: baca + ekstrak gejala)-->  DataFrame  --(konektor)-->  Neo4j
   (ingestion)              (processing)                                  (NoSQL storage)
                                                          Cypher query + graph.html (analytics)
```

Model graph:

```
(Pasien)-[:MENGALAMI {chat_id, waktu}]->(Gejala)
(Pasien)-[:DIPERIKSA_OLEH]->(Dokter)
```

Chat mentah **tidak** disimpan di Neo4j. Neo4j menyimpan *hubungan* hasil olahan.
Jelaskan ini apa adanya saat presentasi, dan jelaskan juga alasannya: graph
unggul untuk pertanyaan relasi, bukan untuk menyimpan transkrip utuh.

## Prasyarat

| Kebutuhan | Versi | Cek |
|---|---|---|
| Docker Desktop | terbaru, harus sedang berjalan | `docker --version` |
| Java | **17** (disarankan, sesuai rencana kelompok) | `java -version` |
| uv | terbaru | `uv --version` |

Python 3.12 diatur otomatis oleh uv lewat file `.python-version`. Python 3.14 di
sistem tidak masalah, proyek ini tidak memakainya (PySpark 3.5 dan konektor 5.4.3
hanya mendukung Python 3.9 sampai 3.13).

## Langkah menjalankan

```bash
# 0. Siapkan environment Python
uv sync

# 1. Jalankan Neo4j, lalu tunggu sekitar 30 detik sampai siap
docker compose up -d
docker compose logs -f neo4j      # tunggu tulisan "Started." lalu Ctrl+C

# 2. Uji koneksi + buat constraint
uv run python src/01_test_neo4j.py

# 3. Uji Spark saja (tidak butuh Neo4j)
uv run python src/02_extract_symptoms.py

# 4. Spark -> Neo4j (konektor). Run pertama mengunduh JAR, butuh internet
uv run python src/03_load_with_spark.py
#    Kalau konektor bermasalah, pakai cadangan (hasilnya sama):
#    uv run python src/03b_load_with_driver.py

# 5. Query analitik + visualisasi (buka graph.html di browser)
uv run python src/04_queries_and_viz.py

# 6. Demo CRUD (setara Lab MongoDB di modul, versi Cypher)
uv run python src/05_crud_demo.py
```

Menghentikan: `docker compose down` (data tetap ada). Mengosongkan semuanya:
`docker compose down -v`.

Jalankan semua skrip lewat `uv run`, bukan `python` langsung, supaya memakai Python 3.12 proyek ini.

## Yang sudah diuji dan yang belum

Jujur soal status, supaya tidak ada kejutan:

- **Sudah diuji** (PySpark 3.5.9, Python 3.12, Java 21 di sandbox): `02_extract_symptoms.py`
  berjalan benar dan hasilnya sudah diperiksa manual terhadap `chats.json`, termasuk
  kasus negasi ("tidak ada sesak" tidak dihitung). Pembuatan `graph.html` diuji dengan data tiruan.
  Semua skrip lolos pemeriksaan sintaks.
- **Belum diuji**: semua skrip yang bicara ke Neo4j (01, 03, 03b, 04, 05) dan konektor Spark.
  Sandbox tempat skrip ini dibuat tidak punya Docker dan tidak bisa mengakses Maven Central.
  Query Cypher ditulis mengikuti dokumentasi, tetapi **jalankan dulu sebelum percaya**.
  Kalau ada error, kirim pesan error lengkapnya.

## Troubleshooting

| Gejala | Kemungkinan penyebab | Tindakan |
|---|---|---|
| `01` gagal: connection refused / Unable to retrieve routing | Neo4j belum selesai start | Tunggu, cek `docker compose logs neo4j` |
| `docker compose pull` gagal: tag tidak ditemukan | Tag image berubah | Cek tag `neo4j` di Docker Hub, ganti di `docker-compose.yml` |
| Auth gagal | Password di compose berbeda dengan yang dipakai skrip | Samakan, atau set env `NEO4J_PASSWORD` |
| Port 7687/7474 sudah dipakai | Neo4j lain berjalan | Hentikan, atau ganti port di compose dan env `NEO4J_URI` |
| `JAVA_HOME is not set` / Spark tidak jalan | Java belum terpasang | Pasang Java 17, set `JAVA_HOME` |
| Error `NoSuchMethodError` / `ClassNotFoundException` di `03` | Versi Scala konektor tidak cocok | PySpark pip 3.5 memakai Scala 2.12 (sudah dicek). Kalau tetap error, pakai `03b` |
| `03` macet saat mengunduh | Tidak ada internet / diblokir | Pakai `03b` |
| Error worker Python / versi Python berbeda | Spark memakai Python sistem | Jalankan lewat `uv run`; `common.py` sudah mengatur `PYSPARK_PYTHON` |
| `http://localhost:7474` tidak menampilkan Browser | Edisi Community tidak selalu menyertakan Browser | Pakai `graph.html` dari skrip 04 |
| Windows: error `winutils` / `HADOOP_HOME` | Spark di Windows | Cari solusinya dulu (cukup untuk `03`; `03b` tetap bisa) |

## Keterbatasan yang harus diketahui (bahan tanya-jawab)

1. **Ekstraksi gejala berbasis kamus** itu sederhana. Kata "tidak/tanpa" langsung di depan
   kata kunci ditangani, tetapi kalimat negasi yang lebih rumit ("sama sekali bebas dari
   sesak") akan salah. Produksi memakai model NLP/NER bahasa Indonesia.
2. **Data hanya 12 chat dummy.** Tidak ada klaim performa. Untuk skala besar, bahas partisi
   Spark dan batch penulisan konektor, jangan klaim sudah diuji.
3. **Chat mentah tidak ada di database.** Di rancangan ini chat tetap berupa file JSON.
   Kalau dosen bertanya "di mana MongoDB?", jawabannya ada di keputusan desain: dokumen
   chat cocok untuk document store, relasi gejala cocok untuk graph.
4. **Cassandra tidak didemokan.** Bahas secara konsep saja: cocok untuk metadata gambar
   (tulis tinggi, akses berbasis key), bukan untuk query relasi.
5. **Data dummy, bukan data pasien nyata.** Sebutkan ini di slide, karena data kesehatan
   sensitif.
