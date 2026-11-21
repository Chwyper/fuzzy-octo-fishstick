"""Konfigurasi dan fungsi bantu yang dipakai semua skrip.

Semua nilai koneksi bisa diganti lewat environment variable, jadi tidak perlu
mengedit kode kalau password atau alamat Neo4j berbeda.
"""
import os
import sys
from pathlib import Path

# Pastikan worker Spark memakai Python dari virtualenv uv, bukan Python sistem.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

# Di Windows, Spark membutuhkan HADOOP_HOME dan winutils.exe di PATH
if os.name == "nt":
    if os.path.exists(r"C:\hadoop\bin"):
        os.environ.setdefault("HADOOP_HOME", r"C:\hadoop")
        if r"C:\hadoop\bin" not in os.environ.get("PATH", ""):
            os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ.get("PATH", "")

ROOT = Path(__file__).resolve().parent.parent
CHATS_PATH = str(ROOT / "data" / "chats.json")
GRAPH_HTML = str(ROOT / "graph.html")

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "telemed123")
NEO4J_DB = os.getenv("NEO4J_DB", "neo4j")

# Koordinat konektor untuk Spark 3.5. Angka _2.12 harus sama dengan versi Scala
# milik PySpark. Kalau error "NoSuchMethodError" / "ClassNotFound", cek Scala
# yang dipakai (lihat README) lalu ganti menjadi _2.13 bila perlu.
NEO4J_SPARK_CONNECTOR = (
    "org.neo4j:neo4j-connector-apache-spark_2.12:5.4.3_for_spark_3"
)


def get_spark(app_name: str = "telemed-demo", with_neo4j: bool = False):
    """Buat SparkSession lokal. Konektor Neo4j hanya dimuat kalau diminta."""
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")  # data kecil, cukup 4
        .config("spark.ui.showConsoleProgress", "false")
    )
    if with_neo4j:
        builder = builder.config("spark.jars.packages", NEO4J_SPARK_CONNECTOR)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_driver():
    """Buat driver Neo4j (Python) dan pastikan server bisa dihubungi."""
    from neo4j import GraphDatabase

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    driver.verify_connectivity()
    return driver


def run_query(driver, query: str, **params):
    """Jalankan satu query Cypher, kembalikan daftar baris sebagai dict."""
    result = driver.execute_query(query, parameters_=params, database_=NEO4J_DB)
    return [record.data() for record in result.records]


def init_schema(driver):
    """Buat constraint keunikan.

    Penting: tanpa constraint, MERGE yang berjalan bersamaan bisa membuat node
    ganda. Constraint juga otomatis membuat index sehingga MERGE lebih cepat.
    """
    for label, prop in [("Pasien", "id"), ("Dokter", "id"), ("Gejala", "nama")]:
        driver.execute_query(
            f"CREATE CONSTRAINT {label.lower()}_{prop} IF NOT EXISTS "
            f"FOR (n:{label}) REQUIRE n.{prop} IS UNIQUE",
            database_=NEO4J_DB,
        )
