"""Langkah 4: query analitik Cypher + visualisasi graph ke graph.html.

Jalankan:  uv run python src/04_queries_and_viz.py
Lalu buka graph.html di browser (cadangan kalau Neo4j Browser tidak tersedia).
"""
import pandas as pd

from common import GRAPH_HTML, get_driver, run_query

# 1) Gejala paling sering: agregasi sederhana
TOP_GEJALA = """
MATCH (:Pasien)-[m:MENGALAMI]->(g:Gejala)
RETURN g.nama AS gejala, count(m) AS jumlah_chat
ORDER BY jumlah_chat DESC, gejala
"""

# 2) Gejala yang muncul BERSAMA gejala tertentu di chat yang sama.
#    Inilah kekuatan graph: pola "gejala <- pasien -> gejala" dibaca seperti kalimat.
PENYERTA = """
MATCH (g1:Gejala {nama: $nama})<-[m1:MENGALAMI]-(:Pasien)-[m2:MENGALAMI]->(g2:Gejala)
WHERE m1.chat_id = m2.chat_id AND g1 <> g2
RETURN g2.nama AS gejala_penyerta, count(DISTINCT m1.chat_id) AS jumlah_chat
ORDER BY jumlah_chat DESC, gejala_penyerta
"""

# 3) Pasien lain yang punya gejala sama dengan pasien tertentu (penelusuran 2 lompatan,
#    di database relasional ini butuh self-JOIN).
PASIEN_MIRIP = """
MATCH (:Pasien {id: $pid})-[:MENGALAMI]->(g:Gejala)<-[:MENGALAMI]-(lain:Pasien)
WHERE lain.id <> $pid
RETURN lain.id AS pasien, collect(DISTINCT g.nama) AS gejala_sama,
       count(DISTINCT g) AS jumlah_gejala_sama
ORDER BY jumlah_gejala_sama DESC, pasien
"""

# 4) Beban dokter
BEBAN_DOKTER = """
MATCH (d:Dokter)<-[:DIPERIKSA_OLEH]-(p:Pasien)
RETURN d.id AS dokter, count(DISTINCT p) AS jumlah_pasien
ORDER BY jumlah_pasien DESC, dokter
"""


def tampil(judul, baris):
    print(f"\n=== {judul} ===")
    print(pd.DataFrame(baris).to_string(index=False) if baris else "(kosong)")


def buat_visualisasi(driver):
    """Gambar graph Pasien-Gejala-Dokter ke file HTML interaktif."""
    from pyvis.network import Network

    # cdn_resources="in_line": semua JS ditanam di dalam graph.html, jadi satu file
    # saja dan tetap jalan tanpa internet saat presentasi.
    net = Network(height="700px", width="100%", directed=True, bgcolor="#ffffff",
                  notebook=False, cdn_resources="in_line")
    warna = {"Pasien": "#4c9be8", "Gejala": "#e8704c", "Dokter": "#4cc38a"}

    rel = run_query(
        driver,
        "MATCH (a)-[r]->(b) "
        "RETURN labels(a)[0] AS la, coalesce(a.id, a.nama) AS a, type(r) AS tipe, "
        "labels(b)[0] AS lb, coalesce(b.id, b.nama) AS b",
    )
    for r in rel:
        for label, nama in ((r["la"], r["a"]), (r["lb"], r["b"])):
            net.add_node(f"{label}:{nama}", label=str(nama), color=warna.get(label, "#999"),
                         title=label)
        net.add_edge(f"{r['la']}:{r['a']}", f"{r['lb']}:{r['b']}", title=r["tipe"])

    net.write_html(GRAPH_HTML)
    print(f"\nGraph disimpan: {GRAPH_HTML}  ({len(rel)} relasi). Buka di browser.")


def main():
    driver = get_driver()
    tampil("Gejala paling sering", run_query(driver, TOP_GEJALA))
    tampil("Gejala penyerta 'demam'", run_query(driver, PENYERTA, nama="demam"))
    tampil("Pasien dengan gejala mirip P001", run_query(driver, PASIEN_MIRIP, pid="P001"))
    tampil("Jumlah pasien per dokter", run_query(driver, BEBAN_DOKTER))
    buat_visualisasi(driver)
    driver.close()


if __name__ == "__main__":
    main()
