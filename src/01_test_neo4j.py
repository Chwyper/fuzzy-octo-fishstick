"""Langkah 1: uji koneksi ke Neo4j dan buat constraint.

Jalankan:  uv run python src/01_test_neo4j.py
Prasyarat: `docker compose up -d` sudah dijalankan dan Neo4j siap (tunggu ~30 detik).
"""
from common import get_driver, init_schema, run_query


def main():
    driver = get_driver()
    print("OK: terhubung ke Neo4j")

    init_schema(driver)
    print("OK: constraint keunikan dibuat (Pasien.id, Dokter.id, Gejala.nama)")

    # Uji tulis-baca kecil memakai label khusus supaya tidak mengotori data demo.
    run_query(
        driver,
        "MERGE (p:UjiPasien {id: 'T1'}) "
        "MERGE (g:UjiGejala {nama: 'demam'}) "
        "MERGE (p)-[:MENGALAMI]->(g)",
    )
    baris = run_query(
        driver,
        "MATCH (p:UjiPasien)-[:MENGALAMI]->(g:UjiGejala) "
        "RETURN p.id AS pasien, g.nama AS gejala",
    )
    print("Hasil baca:", baris)
    assert baris == [{"pasien": "T1", "gejala": "demam"}], "hasil baca tidak sesuai"

    run_query(driver, "MATCH (n) WHERE n:UjiPasien OR n:UjiGejala DETACH DELETE n")
    print("OK: data uji dihapus. Neo4j siap dipakai.")
    driver.close()


if __name__ == "__main__":
    main()
