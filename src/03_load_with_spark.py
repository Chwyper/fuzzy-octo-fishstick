"""Langkah 3: Spark menulis hasil ekstraksi ke Neo4j lewat Neo4j Connector for Spark.

Jalankan:  uv run python src/03_load_with_spark.py
Pertama kali jalan, Spark mengunduh konektor dari Maven Central (butuh internet).
Kalau gagal terus, pakai cadangan: src/03b_load_with_driver.py
"""
from common import (
    CHATS_PATH, NEO4J_PASSWORD, NEO4J_URI, NEO4J_USER,
    get_driver, get_spark, init_schema,
)
from symptoms import extract_symptoms

# Spark menjalankan query ini untuk setiap baris, dengan baris tersedia sebagai `event`
# (konektor membungkusnya dalam UNWIND $events AS event).
# MERGE = "buat kalau belum ada", jadi skrip aman dijalankan berulang kali.
WRITE_QUERY = """
MERGE (p:Pasien {id: event.patient_id})
MERGE (d:Dokter {id: event.doctor_id})
MERGE (g:Gejala {nama: event.gejala})
MERGE (p)-[:DIPERIKSA_OLEH]->(d)
MERGE (p)-[m:MENGALAMI {chat_id: event.chat_id}]->(g)
SET m.waktu = event.timestamp
"""

READ_QUERY = """
MATCH (p:Pasien)-[m:MENGALAMI]->(g:Gejala)
RETURN p.id AS pasien, g.nama AS gejala, m.chat_id AS chat_id
"""


def main():
    # 1) Constraint dibuat lewat driver Python dulu (mencegah node ganda)
    driver = get_driver()
    init_schema(driver)
    driver.close()

    # 2) Spark: baca JSON -> ekstrak gejala
    spark = get_spark("load-neo4j", with_neo4j=True)
    chats = spark.read.option("multiLine", "true").json(CHATS_PATH)
    hasil = extract_symptoms(chats)
    print(f"Akan menulis {hasil.count()} baris ke Neo4j...")

    # 3) Tulis ke Neo4j. coalesce(1): satu partisi = satu penulis, sehingga tidak ada
    #    transaksi paralel yang saling mengunci (deadlock) pada node yang sama.
    (
        hasil.coalesce(1).write.format("org.neo4j.spark.DataSource")
        .mode("Append")
        .option("url", NEO4J_URI)
        .option("authentication.basic.username", NEO4J_USER)
        .option("authentication.basic.password", NEO4J_PASSWORD)
        .option("query", WRITE_QUERY)
        .save()
    )
    print("Selesai menulis.")

    # 4) Bukti integrasi dua arah: baca kembali dari Neo4j ke DataFrame Spark
    kembali = (
        spark.read.format("org.neo4j.spark.DataSource")
        .option("url", NEO4J_URI)
        .option("authentication.basic.username", NEO4J_USER)
        .option("authentication.basic.password", NEO4J_PASSWORD)
        .option("query", READ_QUERY)
        .load()
    )
    print("\n=== Dibaca kembali dari Neo4j ke Spark ===")
    kembali.show(10, truncate=False)
    print("Jumlah relasi MENGALAMI di Neo4j:", kembali.count())
    spark.stop()


if __name__ == "__main__":
    main()
