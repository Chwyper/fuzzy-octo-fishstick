"""Langkah 2: Spark membaca chat dan mengekstrak gejala. TIDAK butuh Neo4j.

Jalankan:  uv run python src/02_extract_symptoms.py
"""
from common import CHATS_PATH, get_spark
from symptoms import extract_symptoms


def main():
    spark = get_spark("ekstraksi-gejala")

    # multiLine=true karena chats.json adalah satu array JSON, bukan satu baris per objek
    chats = spark.read.option("multiLine", "true").json(CHATS_PATH)
    print("\n=== Skema chat (struktur bersarang, seperti dokumen) ===")
    chats.printSchema()

    hasil = extract_symptoms(chats)
    print("=== Gejala per chat ===")
    hasil.show(60, truncate=False)

    print("=== Gejala paling sering muncul (agregasi Spark) ===")
    hasil.groupBy("gejala").count().orderBy("count", ascending=False).show(truncate=False)

    print(f"Total chat: {chats.count()} | total pasangan chat-gejala: {hasil.count()}")
    spark.stop()


if __name__ == "__main__":
    main()
