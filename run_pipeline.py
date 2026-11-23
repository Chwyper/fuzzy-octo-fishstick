"""Pipeline runner terpadu untuk Telemedicine Demo.

Menjalankan seluruh tahapan end-to-end tanpa perlu menjalankan skrip satu per satu:
1. Uji koneksi & constraint Neo4j
2. Ekstraksi gejala chat
3. Memuat relasi ke Neo4j
4. Menghasilkan visualisasi graph.html
5. Menampilkan ringkasan analitik

Jalankan dengan:
    uv run python run_pipeline.py
"""
import os
import sys
import time
from pathlib import Path

# Tambahkan src ke path
ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))

from common import (
    CHATS_SOURCE,
    GRAPH_HTML,
    baca_chat,
    get_driver,
    get_spark,
    init_schema,
    run_query,
)
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


def jalankan_pipeline(sumber_chat=None, use_spark=True):
    sumber = sumber_chat or CHATS_SOURCE
    print("=" * 60)
    print("🏥 PIPELINE BIG DATA TELEMEDISIN (SPARK + NEO4J)")
    print(f"📁 Sumber data: {sumber}")
    print("=" * 60)

    # Langkah 1: Cek Koneksi Neo4j
    print("\n[1/5] Memeriksa koneksi database Neo4j...")
    try:
        driver = get_driver()
        init_schema(driver)
        print("  ✅ Berhasil terhubung ke Neo4j & constraint keunikan siap.")
    except Exception as e:
        print(f"  ❌ Gagal terhubung ke Neo4j: {e}")
        print("\n  👉 Pastikan Docker Desktop aktif lalu jalankan: docker compose up -d")
        return False

    # Langkah 2 & 3: Ekstraksi & Muat Data
    print("\n[2/5] Mengekstrak gejala dari transkrip chat...")
    t_start = time.time()
    rows = []

    try:
        spark = get_spark("telemed-pipeline")
        chats = baca_chat(spark, sumber)
        hasil = extract_symptoms(chats)
        rows = [r.asDict() for r in hasil.collect()]
        spark.stop()
        print(f"  ✅ PySpark berhasil mengekstrak {len(rows)} relasi (chat-gejala).")
    except Exception as e_spark:
        print(f"  ⚠️ Spark lokal terkendala ({e_spark}), menggunakan Python engine fallback...")
        import json
        import re
        from symptoms import PATTERNS

        chat_items = []
        if os.path.isdir(sumber):
            for f in Path(sumber).glob("*.json"):
                with open(f, "r", encoding="utf-8") as fp:
                    for line in fp:
                        if line.strip():
                            chat_items.append(json.loads(line.strip()))
        else:
            with open(sumber, "r", encoding="utf-8") as fp:
                data = json.load(fp)
                chat_items = data if isinstance(data, list) else [data]

        for c in chat_items:
            cid = c.get("chat_id")
            pid = c.get("patient_id")
            did = c.get("doctor_id")
            ts = c.get("timestamp", "")
            for m in c.get("messages", []):
                if m.get("from") == "patient":
                    txt = m.get("text", "").lower()
                    for nama, pola in PATTERNS.items():
                        if re.search(pola, txt):
                            rows.append({
                                "chat_id": cid,
                                "patient_id": pid,
                                "doctor_id": did,
                                "timestamp": ts,
                                "gejala": nama,
                            })
        # Distinct
        rows = [dict(t) for t in {tuple(sorted(d.items())) for d in rows}]
        print(f"  ✅ Python engine berhasil mengekstrak {len(rows)} relasi (chat-gejala).")

    print("\n[3/5] Memuat hasil olahan ke graf Neo4j...")
    batch_size = 5000
    for i in range(0, len(rows), batch_size):
        run_query(driver, LOAD_QUERY, rows=rows[i:i + batch_size])
    t_load = time.time() - t_start
    print(f"  ✅ Berhasil memuat ke Neo4j dalam {t_load:.2f} detik.")

    # Langkah 4: Visualisasi Graf
    print("\n[4/5] Memperbarui visualisasi graf interaktif (graph.html)...")
    try:
        from pyvis.network import Network
        net = Network(height="700px", width="100%", directed=True, bgcolor="#ffffff",
                      notebook=False, cdn_resources="in_line")
        warna = {"Pasien": "#4c9be8", "Gejala": "#e8704c", "Dokter": "#4cc38a"}

        rel = run_query(
            driver,
            "MATCH (a)-[r]->(b) "
            "RETURN labels(a)[0] AS la, coalesce(a.id, a.nama) AS a, type(r) AS tipe, "
            "labels(b)[0] AS lb, coalesce(b.id, b.nama) AS b LIMIT 500",
        )
        for r in rel:
            for label, nama in ((r["la"], r["a"]), (r["lb"], r["b"])):
                net.add_node(f"{label}:{nama}", label=str(nama), color=warna.get(label, "#999"),
                             title=label)
            net.add_edge(f"{r['la']}:{r['a']}", f"{r['lb']}:{r['b']}", title=r["tipe"])

        html_content = net.generate_html()
        with open(GRAPH_HTML, "w", encoding="utf-8") as f:
            f.write(html_content)
        print(f"  ✅ graph.html berhasil disimpan ({len(rel)} relasi ditampilkan).")
    except Exception as e_viz:
        print(f"  ⚠️ Gagal memperbarui graph.html: {e_viz}")

    # Langkah 5: Ringkasan Analitik
    print("\n[5/5] Ringkasan Database & Analitik:")
    counts = run_query(
        driver,
        """
        MATCH (p:Pasien) WITH count(p) as total_pasien
        MATCH (d:Dokter) WITH total_pasien, count(d) as total_dokter
        MATCH (g:Gejala) WITH total_pasien, total_dokter, count(g) as total_gejala
        MATCH ()-[m:MENGALAMI]->() WITH total_pasien, total_dokter, total_gejala, count(m) as total_mengalami
        RETURN total_pasien, total_dokter, total_gejala, total_mengalami
        """
    )
    if counts:
        c = counts[0]
        print(f"  • Total Pasien : {c['total_pasien']}")
        print(f"  • Total Dokter : {c['total_dokter']}")
        print(f"  • Total Gejala : {c['total_gejala']}")
        print(f"  • Total Relasi MENGALAMI : {c['total_mengalami']}")

    driver.close()
    print("\n" + "=" * 60)
    print("✨ Pipeline Selesai Sukses!")
    print("💡 Anda bisa membuka Dashboard Streamlit dengan menjalankan:")
    print("   uv run streamlit run app.py")
    print("=" * 60)
    return True


if __name__ == "__main__":
    sumber = sys.argv[1] if len(sys.argv) > 1 else None
    jalankan_pipeline(sumber)
