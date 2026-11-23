"""Langkah 3 (CADANGAN): Spark tetap mengekstrak gejala, tetapi penulisan ke Neo4j
memakai driver Python biasa, tanpa konektor Spark.

Pakai ini kalau konektor bermasalah (versi Scala/Spark tidak cocok, tidak ada internet
untuk mengunduh JAR, dll). Hasil di Neo4j identik dengan 03_load_with_spark.py.

Jalankan:  uv run python src/03b_load_with_driver.py
"""
import time

from common import baca_chat, get_driver, get_spark, init_schema, run_query
from symptoms import extract_symptoms

UKURAN_BATCH = 5000

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
    spark = get_spark("load-neo4j-driver")  # tanpa konektor
    chats = baca_chat(spark)
    hasil = extract_symptoms(chats)

    # Dikumpulkan ke driver, lalu ditulis per batch (satu query per UKURAN_BATCH baris).
    # Untuk data sangat besar, pakai 03_load_with_spark.py (penulisan lewat konektor).
    rows = [r.asDict() for r in hasil.collect()]
    spark.stop()
    print(f"Menulis {len(rows)} baris lewat driver Python (batch {UKURAN_BATCH})...")

    driver = get_driver()
    init_schema(driver)
    t0 = time.time()
    for i in range(0, len(rows), UKURAN_BATCH):
        run_query(driver, LOAD_QUERY, rows=rows[i:i + UKURAN_BATCH])
    print(f"Waktu tulis ke Neo4j: {time.time() - t0:.1f} detik")

    jumlah = run_query(driver, "MATCH ()-[m:MENGALAMI]->() RETURN count(m) AS jumlah")
    print("Jumlah relasi MENGALAMI di Neo4j:", jumlah[0]["jumlah"])
    driver.close()


if __name__ == "__main__":
    main()
