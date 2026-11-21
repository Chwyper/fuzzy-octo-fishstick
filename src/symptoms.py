"""Ekstraksi gejala dari chat dengan Spark (NLP sederhana berbasis kamus).

Catatan untuk presentasi: ini sengaja sederhana. Pendekatan produksi memakai
model NER/NLP bahasa Indonesia. Keterbatasannya dibahas di README.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# nama gejala -> kata kunci yang dianggap menunjuk gejala itu
SYMPTOM_KEYWORDS = {
    "demam": ["demam", "meriang", "panas badan"],
    "batuk": ["batuk"],
    "sesak napas": ["sesak"],
    "pusing": ["pusing", "sakit kepala"],
    "mual": ["mual", "muntah"],
    "nyeri dada": ["nyeri dada"],
    "sakit tenggorokan": ["sakit tenggorokan", "tenggorokan perih"],
    "diare": ["diare", "mencret"],
    "ruam": ["ruam"],
    "gatal": ["gatal"],
    "lemas": ["lemas", "lemah"],
}

# Kata yang membuat gejala di belakangnya dianggap TIDAK dialami pasien.
# Dibuat sebagai lookbehind regex: kata kunci tidak dihitung kalau persis
# didahului salah satu frasa ini. Panjang tiap frasa harus tetap (aturan regex Java).
NEGATION_PREFIXES = ["tidak ", "tidak ada ", "tanpa ", "belum ada "]


def _pattern(keywords):
    lookbehinds = "".join(f"(?<!{p})" for p in NEGATION_PREFIXES)
    return lookbehinds + r"\b(?:" + "|".join(keywords) + ")"


PATTERNS = {name: _pattern(kws) for name, kws in SYMPTOM_KEYWORDS.items()}


def extract_symptoms(chats: DataFrame) -> DataFrame:
    """Ubah DataFrame chat menjadi baris (chat, pasien, dokter, gejala).

    Langkah:
      1. explode: satu baris per pesan
      2. ambil hanya pesan pasien (pertanyaan dokter bukan gejala pasien)
      3. cocokkan kata kunci gejala (tanpa negasi) di tiap pesan
      4. explode hasil, buang duplikat per chat
    """
    pesan = (
        chats.select(
            "chat_id", "patient_id", "doctor_id", "timestamp",
            F.explode("messages").alias("m"),
        )
        .select(
            "chat_id", "patient_id", "doctor_id", "timestamp",
            F.col("m").getField("from").alias("pengirim"),
            F.lower(F.col("m").getField("text")).alias("teks"),
        )
        .where(F.col("pengirim") == "patient")
    )

    terdeteksi = F.array_compact(
        F.array(*[
            F.when(F.col("teks").rlike(pola), F.lit(nama))
            for nama, pola in PATTERNS.items()
        ])
    )

    return (
        pesan.withColumn("gejala", F.explode(terdeteksi))
        .select("chat_id", "patient_id", "doctor_id", "timestamp", "gejala")
        .distinct()
        .orderBy("chat_id", "gejala")
    )
