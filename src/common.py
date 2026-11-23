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
# Sumber data chat. Bisa satu file array JSON (default, 12 chat dummy) ATAU folder
# berisi file JSON Lines hasil generate_chats.py (mis. data/synthetic, 100 ribu chat).
# Ganti tanpa mengedit kode:  CHATS_SOURCE=data/synthetic uv run python src/02_...
CHATS_PATH = str(ROOT / "data" / "chats.json")
CHATS_SOURCE = os.getenv("CHATS_SOURCE", CHATS_PATH)
if not os.path.isabs(CHATS_SOURCE):
    CHATS_SOURCE = str(ROOT / CHATS_SOURCE)
STREAM_DIR = str(ROOT / "data" / "stream")
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


def _chat_schema():
    from pyspark.sql.types import ArrayType, StringType, StructField, StructType

    return StructType([
        StructField("chat_id", StringType()),
        StructField("patient_id", StringType()),
        StructField("doctor_id", StringType()),
        StructField("timestamp", StringType()),
        StructField("messages", ArrayType(StructType([
            StructField("from", StringType()),
            StructField("text", StringType()),
        ]))),
        StructField("label_gejala", ArrayType(StringType())),  # hanya ada di data sintetis
    ])


def get_chat_schema():
    return _chat_schema()


def get_spark(app_name: str = "telemed-demo", with_neo4j: bool = False):
    """Buat SparkSession lokal. Konektor Neo4j hanya dimuat kalau diminta."""
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        # Default Spark (128 MB/partisi) menggabungkan 43 MB data jadi 1 partisi, sehingga
        # tidak ada paralelisme. 4 MB membuat 100 ribu chat terbagi ke beberapa partisi.
        .config("spark.sql.files.maxPartitionBytes", str(4 * 1024 * 1024))
        .config("spark.ui.showConsoleProgress", "false")
    )
    if with_neo4j:
        builder = builder.config("spark.jars.packages", NEO4J_SPARK_CONNECTOR)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def baca_chat(spark, sumber: str = None):
    """Baca chat dari file tunggal (array JSON) atau folder (JSON Lines).

    - File  : multiLine=true, karena isinya satu array JSON.
    - Folder: JSON Lines, satu chat per baris (bisa dipecah per bagian, jadi Spark
      dapat membaginya ke beberapa partisi). Skema diberikan eksplisit supaya Spark
      tidak perlu memindai seluruh data hanya untuk menebak skema.
    """
    sumber = sumber or CHATS_SOURCE
    if os.path.isdir(sumber):
        return spark.read.schema(get_chat_schema()).json(sumber)
    return spark.read.option("multiLine", "true").json(sumber)


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
