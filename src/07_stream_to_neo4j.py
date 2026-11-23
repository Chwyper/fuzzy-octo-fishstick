"""Langkah 7 (Velocity): Spark Structured Streaming membaca file chat yang terus datang
dan memperbarui graph Neo4j per micro-batch.

Terminal 1 (produsen, chat "masuk" terus):
  uv run python src/generate_chats.py --stream --batch 500 --interval 5 --out data/stream
Terminal 2 (konsumen):
  uv run python src/07_stream_to_neo4j.py              # menulis ke Neo4j
  uv run python src/07_stream_to_neo4j.py --dry-run    # hanya menghitung, tanpa Neo4j

Sambil berjalan, jalankan query di Neo4j (mis. hitung relasi MENGALAMI) berulang kali:
angkanya naik terus. Hentikan dengan Ctrl+C.

Catatan: ini streaming berbasis file (Spark memantau folder). Produksi biasanya memakai
Kafka. Sebutkan itu sebagai keterbatasan, jangan diklaim sebagai pipeline produksi.
"""
import argparse
import os

from common import STREAM_DIR, get_chat_schema, get_driver, get_spark, init_schema, run_query

LOAD_QUERY = """
UNWIND $rows AS event
MERGE (p:Pasien {id: event.patient_id})
MERGE (d:Dokter {id: event.doctor_id})
MERGE (g:Gejala {nama: event.gejala})
MERGE (p)-[:DIPERIKSA_OLEH]->(d)
MERGE (p)-[m:MENGALAMI {chat_id: event.chat_id}]->(g)
SET m.waktu = event.timestamp
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default=STREAM_DIR, help="folder yang dipantau")
    ap.add_argument("--dry-run", action="store_true", help="jangan tulis ke Neo4j")
    ap.add_argument("--max-files", type=int, default=2, help="file per micro-batch")
    a = ap.parse_args()

    os.makedirs(a.dir, exist_ok=True)
    driver = None
    if not a.dry_run:
        driver = get_driver()
        init_schema(driver)

    spark = get_spark("stream-neo4j")
    # Streaming file wajib punya skema eksplisit (Spark tidak boleh menebak dari data yang belum datang).
    # File sementara diawali "." dan di-rename oleh generator, jadi tidak terbaca setengah jadi.
    chats = (
        spark.readStream.schema(get_chat_schema())
        .option("maxFilesPerTrigger", a.max_files)
        .json(a.dir)
    )
    hasil = extract_symptoms_stream(chats)

    def tulis(batch_df, batch_id):
        n = batch_df.count()
        if n == 0:
            return
        if driver is None:
            print(f"[batch {batch_id}] {n} pasangan chat-gejala (dry-run)")
            return
        rows = [r.asDict() for r in batch_df.collect()]
        for i in range(0, len(rows), 5000):
            run_query(driver, LOAD_QUERY, rows=rows[i:i + 5000])
        total = run_query(driver, "MATCH ()-[m:MENGALAMI]->() RETURN count(m) AS n")[0]["n"]
        print(f"[batch {batch_id}] +{n} baris | total relasi MENGALAMI di Neo4j: {total}")

    q = hasil.writeStream.foreachBatch(tulis).start()
    print(f"Memantau {a.dir} ... (Ctrl+C untuk berhenti)")
    try:
        q.awaitTermination()
    except KeyboardInterrupt:
        q.stop()
        spark.stop()
        if driver:
            driver.close()


def extract_symptoms_stream(chats):
    """Versi streaming dari extract_symptoms: streaming tidak boleh memakai orderBy/distinct
    global tanpa agregasi, jadi di sini deduplikasi dilakukan per micro-batch di `tulis`
    (MERGE di Neo4j sudah idempoten, jadi duplikat tidak membuat data ganda)."""
    from pyspark.sql import functions as F
    from symptoms import PATTERNS

    pesan = (
        chats.select("chat_id", "patient_id", "doctor_id", "timestamp", F.explode("messages").alias("m"))
        .select(
            "chat_id", "patient_id", "doctor_id", "timestamp",
            F.col("m").getField("from").alias("pengirim"),
            F.lower(F.col("m").getField("text")).alias("teks"),
        )
        .where(F.col("pengirim") == "patient")
    )
    terdeteksi = F.array_compact(F.array(*[
        F.when(F.col("teks").rlike(pola), F.lit(nama)) for nama, pola in PATTERNS.items()
    ]))
    return pesan.withColumn("gejala", F.explode(terdeteksi)).select(
        "chat_id", "patient_id", "doctor_id", "timestamp", "gejala"
    )


if __name__ == "__main__":
    main()
