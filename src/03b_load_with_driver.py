"""Langkah 3 (CADANGAN): Spark tetap mengekstrak gejala, tetapi penulisan ke Neo4j
memakai driver Python biasa, tanpa konektor Spark.

Pakai ini kalau konektor bermasalah (versi Scala/Spark tidak cocok, tidak ada internet
untuk mengunduh JAR, dll). Hasil di Neo4j identik dengan 03_load_with_spark.py.

Jalankan:  uv run python src/03b_load_with_driver.py
"""
from common import CHATS_PATH, get_driver, get_spark, init_schema, run_query
from symptoms import extract_symptoms

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
    chats = spark.read.option("multiLine", "true").json(CHATS_PATH)
    hasil = extract_symptoms(chats)

    # Data kecil: aman dikumpulkan ke driver. Untuk data besar, pakai konektor.
    rows = [r.asDict() for r in hasil.collect()]
    spark.stop()
    print(f"Menulis {len(rows)} baris lewat driver Python...")

    driver = get_driver()
    init_schema(driver)
    run_query(driver, LOAD_QUERY, rows=rows)

    jumlah = run_query(driver, "MATCH ()-[m:MENGALAMI]->() RETURN count(m) AS jumlah")
    print("Jumlah relasi MENGALAMI di Neo4j:", jumlah[0]["jumlah"])
    driver.close()


if __name__ == "__main__":
    main()
