"""Langkah 6 (Veracity): ukur akurasi ekstraksi gejala terhadap label asli.

Hanya berlaku untuk data sintetis (punya field `label_gejala`).
Jalankan:
  uv run python src/generate_chats.py --n 100000 --out data/synthetic
  CHATS_SOURCE=data/synthetic uv run python src/06_evaluate_extraction.py

PENTING untuk presentasi: data sintetis dibuat dengan kalimat negasi yang sederhana,
jadi angka yang tinggi di sini TIDAK membuktikan akurasi pada chat nyata. Sebutkan
itu apa adanya. Lihat README bagian "Veracity".
"""
from pyspark.sql import functions as F

from common import CHATS_SOURCE, baca_chat, get_spark
from symptoms import extract_symptoms


def main():
    spark = get_spark("evaluasi-ekstraksi")
    chats = baca_chat(spark)
    if "label_gejala" not in chats.columns:
        print("Sumber data ini tidak punya label_gejala (bukan data sintetis). Tidak ada yang dievaluasi.")
        spark.stop()
        return

    prediksi = extract_symptoms(chats).select("chat_id", F.col("gejala").alias("g")).distinct()
    asli = chats.select("chat_id", F.explode("label_gejala").alias("g")).distinct()

    tp = prediksi.join(asli, ["chat_id", "g"], "inner").count()
    n_pred = prediksi.count()
    n_asli = asli.count()
    fp, fn = n_pred - tp, n_asli - tp

    precision = tp / n_pred if n_pred else 0.0
    recall = tp / n_asli if n_asli else 0.0
    print(f"Sumber: {CHATS_SOURCE}")
    print(f"Chat: {chats.count()} | gejala asli: {n_asli} | gejala terdeteksi: {n_pred}")
    print(f"TP={tp}  FP={fp} (terdeteksi tapi salah)  FN={fn} (terlewat)")
    print(f"Precision = {precision:.4f} | Recall = {recall:.4f}")
    spark.stop()


if __name__ == "__main__":
    main()
