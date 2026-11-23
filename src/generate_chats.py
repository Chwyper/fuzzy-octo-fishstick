"""Generator chat telemedisin sintetis (data DUMMY, bukan data pasien nyata).

Keluaran: JSON Lines (satu chat per baris), dipecah ke beberapa file part-*.json
supaya Spark bisa membaca paralel (banyak file = banyak partisi).

Contoh:
  python generate_chats.py --n 100000 --out data/synthetic        # Volume
  python generate_chats.py --stream --batch 500 --interval 5      # Velocity
"""
import argparse, json, random, time
from datetime import datetime, timedelta
from pathlib import Path

# Profil penyakit: gejala utama + peluang munculnya. Dibuat berkorelasi supaya
# analisis "gejala penyerta" dan "pasien mirip" menghasilkan pola yang bermakna.
PROFIL = {
    "flu":        {"demam": .9, "batuk": .8, "lemas": .6, "sakit tenggorokan": .5, "pusing": .3},
    "gastro":     {"mual": .9, "diare": .8, "demam": .4, "lemas": .6},
    "dbd":        {"demam": .95, "ruam": .5, "pusing": .6, "mual": .5, "lemas": .7},
    "alergi":     {"gatal": .9, "ruam": .8},
    "jantung":    {"nyeri dada": .9, "sesak napas": .7, "pusing": .4, "lemas": .4},
    "pernapasan": {"batuk": .9, "sesak napas": .8, "demam": .4},
}
BOBOT_PROFIL = [30, 18, 12, 12, 8, 20]

# Beberapa cara menyebut tiap gejala. SEMUA mengandung kata kunci kamus di
# symptoms.py, supaya ekstraksi regex bisa dievaluasi terhadap label asli.
FRASA = {
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
ADA = ["Dok, saya {g} sejak {w}.", "Saya mengalami {g} dari {w}.",
       "Sudah {w} ini saya {g}.", "Keluhan saya {g}, mulai {w}."]
TIDAK = ["Tidak ada {g}, Dok.", "Saya tanpa {g} kok.", "Belum ada {g} sampai sekarang."]
WAKTU = ["kemarin", "tiga hari lalu", "pagi tadi", "seminggu lalu", "dua hari lalu", "semalam"]
TANYA = ["Apakah ada {g}?", "Sudah berapa lama keluhannya?", "Ada riwayat alergi?",
         "Sudah minum obat apa?", "Apakah ada {g} juga?"]
DOKTER_BALAS = ["Baik, saya catat.", "Terima kasih informasinya.", "Saya paham."]


def bikin_chat(i, rng, awal, rentang_menit, n_pasien, n_dokter):
    profil = rng.choices(list(PROFIL), BOBOT_PROFIL)[0]
    gejala = [g for g, p in PROFIL[profil].items() if rng.random() < p]
    if not gejala:
        gejala = [next(iter(PROFIL[profil]))]
    # gejala "pengganggu" yang sengaja DISANGKAL pasien (menguji penanganan negasi)
    bantahan = [g for g in rng.sample(list(FRASA), 2) if g not in gejala][:1]

    msgs = [{"from": "patient", "text": rng.choice(ADA).format(
        g=" dan ".join(rng.choice(FRASA[g]) for g in gejala[:2]), w=rng.choice(WAKTU))}]
    msgs.append({"from": "doctor", "text": rng.choice(TANYA).format(
        g=rng.choice(FRASA[rng.choice(list(FRASA))]))})   # pertanyaan dokter BUKAN gejala pasien
    for g in gejala[2:]:
        msgs.append({"from": "patient", "text": f"Juga {rng.choice(FRASA[g])}, Dok."})
    for g in bantahan:
        msgs.append({"from": "patient", "text": rng.choice(TIDAK).format(g=rng.choice(FRASA[g]))})
    msgs.append({"from": "doctor", "text": rng.choice(DOKTER_BALAS)})

    t = awal + timedelta(minutes=rng.randrange(rentang_menit))
    return {
        "chat_id": f"C{i:07d}",
        "patient_id": f"P{rng.randrange(n_pasien):06d}",
        "doctor_id": f"D{rng.randrange(n_dokter):03d}",
        "timestamp": t.isoformat(timespec="seconds"),
        "messages": msgs,
        "label_gejala": sorted(gejala),   # ground truth, untuk mengukur akurasi ekstraksi (Veracity)
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=100_000)
    ap.add_argument("--out", default="data/synthetic")
    ap.add_argument("--parts", type=int, default=20, help="jumlah file keluaran")
    ap.add_argument("--patients", type=int, default=20_000)
    ap.add_argument("--doctors", type=int, default=50)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--stream", action="store_true", help="mode Velocity: tulis batch terus-menerus")
    ap.add_argument("--batch", type=int, default=500)
    ap.add_argument("--interval", type=float, default=5.0)
    a = ap.parse_args()
    rng = random.Random(a.seed)
    out = Path(a.out); out.mkdir(parents=True, exist_ok=True)

    if a.stream:
        i = 0
        print(f"Streaming: {a.batch} chat tiap {a.interval}s ke {out}/ (Ctrl+C untuk berhenti)")
        while True:
            tmp = out / f".tmp-{i}.json"
            with open(tmp, "w", encoding="utf-8") as f:
                for _ in range(a.batch):
                    i += 1
                    f.write(json.dumps(bikin_chat(i, rng, datetime.now(), 1, a.patients, a.doctors), ensure_ascii=False) + "\n")
            tmp.rename(out / f"stream-{i:09d}.json")   # rename = atomik, Spark tidak membaca file setengah jadi
            print(f"  +{a.batch} chat (total {i})")
            time.sleep(a.interval)

    awal = datetime(2026, 5, 1)
    files = [open(out / f"part-{k:03d}.json", "w", encoding="utf-8") for k in range(a.parts)]
    for i in range(1, a.n + 1):
        c = bikin_chat(i, rng, awal, 60 * 24 * 200, a.patients, a.doctors)
        files[i % a.parts].write(json.dumps(c, ensure_ascii=False) + "\n")
    for f in files: f.close()
    print(f"Selesai: {a.n} chat -> {out}/ ({a.parts} file)")

if __name__ == "__main__":
    main()
