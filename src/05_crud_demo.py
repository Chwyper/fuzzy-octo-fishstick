"""Langkah 5: demo CRUD di Neo4j, sejajar dengan Lab modul (insert, find, update, delete).

Jalankan:  uv run python src/05_crud_demo.py
Memakai pasien P999 supaya tidak mengganggu data demo lain; dihapus di akhir.
"""
from common import get_driver, run_query


def main():
    driver = get_driver()

    print("--- CREATE (setara insertMany di modul) ---")
    run_query(
        driver,
        "MERGE (p:Pasien {id: 'P999'}) SET p.usia = 30 "
        "MERGE (g:Gejala {nama: 'demam'}) "
        "MERGE (p)-[:MENGALAMI {chat_id: 'C999'}]->(g)",
    )
    print("P999 dibuat dengan gejala demam")

    print("\n--- READ (setara find di modul) ---")
    print(run_query(driver, "MATCH (p:Pasien {id: 'P999'}) RETURN p.id AS id, p.usia AS usia"))
    print("Pasien usia > 25 (setara filter $gt):")
    print(run_query(driver, "MATCH (p:Pasien) WHERE p.usia > 25 RETURN p.id AS id, p.usia AS usia"))

    print("\n--- UPDATE (setara updateOne + $set di modul) ---")
    run_query(driver, "MATCH (p:Pasien {id: 'P999'}) SET p.usia = 31")
    print(run_query(driver, "MATCH (p:Pasien {id: 'P999'}) RETURN p.usia AS usia"))

    print("\n--- DELETE (setara deleteOne di modul) ---")
    # DETACH DELETE: hapus node sekaligus semua relasinya. Tanpa DETACH, Neo4j menolak
    # menghapus node yang masih punya relasi (menjaga integritas data).
    run_query(driver, "MATCH (p:Pasien {id: 'P999'}) DETACH DELETE p")
    sisa = run_query(driver, "MATCH (p:Pasien {id: 'P999'}) RETURN count(p) AS jumlah")
    print("Sisa P999 setelah dihapus:", sisa[0]["jumlah"])

    driver.close()


if __name__ == "__main__":
    main()
