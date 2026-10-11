# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Bronze Ingest Overview
# MAGIC %md
# MAGIC # 01_ingest_bronze — BIS CPMI cashless bulk zip -> Unity Volume -> bronze Delta
# MAGIC # Free Edition: Serverless only. Uses absolute /Volumes/... paths (no relative paths, no DBFS).
# MAGIC
# MAGIC ### 01 · Bronze ingest
# MAGIC Downloads `WS_CPMI_CASHLESS_csv_flat.zip` (or reuses a manually uploaded copy),
# MAGIC extracts to the `raw` Volume, reads CSV as all-strings, adds lineage, overwrites bronze.

# COMMAND ----------

import os
import sys
from pathlib import Path


def _strip_dbfs(p: str) -> str:
    """dbutils.fs.ls returns 'dbfs:/Volumes/...' but open()/ZipFile need '/Volumes/...'."""
    return p[len("dbfs:") :] if isinstance(p, str) and p.startswith("dbfs:") else p


try:
    _ = dbutils  # noqa: F821 - injected by Databricks Runtime
    _ = spark  # noqa: F821 - injected by Databricks Runtime
    _ON_DATABRICKS = True
except NameError:
    _ON_DATABRICKS = False

if not _ON_DATABRICKS:
    # ---- Local-run shims (so `python src/01_ingest_bronze.py` doesn't die
    # ---- with NameError: dbutils). Databricks path is untouched.
    class _FileInfo:
        """Mimics Databricks FileInfo: .path str + .isDir() method."""

        def __init__(self, path, is_dir):
            self.path = path
            self._is_dir = is_dir

        def isDir(self):
            return self._is_dir

        def __repr__(self):
            return f"FileInfo(path={self.path!r}, isDir={self._is_dir})"

    class _LocalWidgets:
        def __init__(self):
            self._defaults = {}

        def text(self, name, default, _label=""):
            self._defaults[name] = default

        def get(self, name):
            # CLI `--name=value` wins, then env var, then notebook default.
            for a in sys.argv[1:]:
                if a.startswith(f"--{name}="):
                    return a.split("=", 1)[1]
            return os.getenv(name.upper(), os.getenv(name, self._defaults.get(name, "")))

    class _LocalFS:
        def mkdirs(self, path):
            Path(_strip_dbfs(path)).mkdir(parents=True, exist_ok=True)

        def ls(self, path):
            p = Path(_strip_dbfs(path))
            if not p.exists():
                raise FileNotFoundError(f"No such directory: {path}")
            out = []
            for child in sorted(p.iterdir()):
                out.append(_FileInfo(path=str(child), is_dir=child.is_dir()))
            return out

    class _LocalDbutils:
        def __init__(self):
            self.widgets = _LocalWidgets()
            self.fs = _LocalFS()

    dbutils = _LocalDbutils()  # noqa: F821

    try:
        from pyspark.sql import SparkSession

        spark = (  # noqa: F821
            SparkSession.builder.appName("bis-bronze-local").getOrCreate()
        )
    except Exception as e:  # pyspark/java missing -> fail later with a clear msg
        spark = None  # noqa: F821
        print(f"NOTE: local Spark unavailable ({e}). Steps 1-2 still run; step 3 needs pyspark.")

    try:
        display  # noqa: F821
    except NameError:
        def display(obj=None):  # noqa: F821
            if obj is None:
                return
            if hasattr(obj, "show"):
                try:
                    obj.show(20, truncate=False)
                    return
                except Exception:
                    pass
            print(obj)

    _base = Path(__file__).resolve().parents[1] if "__file__" in globals() else Path.cwd()
    VOLUMES_ROOT = os.getenv("BIS_LOCAL_ROOT") or str(_base / "_volumes_local")
    print(f"Local mode: mapping /Volumes -> {VOLUMES_ROOT}")
else:
    VOLUMES_ROOT = "/Volumes"

dbutils.widgets.text("catalog", "bis", "Catalog")
dbutils.widgets.text("schema", "payments", "Schema")
dbutils.widgets.text("data_url", "https://data.bis.org/static/bulk/WS_CPMI_CASHLESS_csv_flat.zip", "BIS bulk URL")

CATALOG = dbutils.widgets.get("catalog").strip() or "bis"
SCHEMA = dbutils.widgets.get("schema").strip() or "payments"
DATA_URL = dbutils.widgets.get("data_url").strip()

RAW_DIR = f"{VOLUMES_ROOT}/{CATALOG}/{SCHEMA}/raw"
EXTRACT_DIR = f"{RAW_DIR}/extracted_cashless"
ZIP_NAME = DATA_URL.split("/")[-1]
ZIP_PATH = f"{RAW_DIR}/{ZIP_NAME}"
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_cpmi_cashless"

print(f"RAW_DIR      = {RAW_DIR}")
print(f"ZIP_PATH     = {ZIP_PATH}")
print(f"BRONZE_TABLE = {BRONZE_TABLE}")

# COMMAND ----------

import os
import zipfile

# Ensure raw dirs exist (Volumes-backed; mkdirs is idempotent)
dbutils.fs.mkdirs(RAW_DIR)
dbutils.fs.mkdirs(EXTRACT_DIR)

# Sanitize DATA_URL: the data_url widget is sticky across runs, so a previous
# accidental paste can leave a doubled filename (...zipWS_...zip). Repair that
# instead of failing with a confusing 404.
if ZIP_NAME and DATA_URL.endswith(ZIP_NAME + ZIP_NAME):
    DATA_URL = DATA_URL[: -len(ZIP_NAME)]
    print(f"Repaired doubled data_url -> {DATA_URL}")
if not (DATA_URL.startswith("https://") and DATA_URL.endswith(".zip")):
    raise ValueError(
        f"data_url widget looks wrong: {DATA_URL!r}. Reset it to "
        f"https://data.bis.org/static/bulk/WS_CPMI_CASHLESS_csv_flat.zip and re-run."
    )
ZIP_NAME = DATA_URL.split("/")[-1]
ZIP_PATH = f"{RAW_DIR}/{ZIP_NAME}"

def find_existing_zip(raw_dir: str, prefer: str):
    """Return OS-usable path of already-uploaded zip, or None. Lists the directory instead
    of ls() on a file path (dbutils.fs.ls on a file throws, so the old
    volume_has_file() check always missed manual uploads).
    Normalizes 'dbfs:/Volumes/...' -> '/Volumes/...' so open()/ZipFile work."""
    prefer = _strip_dbfs(prefer)
    prefer_name = prefer.split("/")[-1]
    try:
        files = [f for f in dbutils.fs.ls(raw_dir) if not f.isDir()]
    except Exception as e:
        print(f"Could not list {raw_dir}: {e}")
        return None
    for f in files:
        p = _strip_dbfs(f.path)
        if p == prefer or p.endswith("/" + prefer_name):
            return p
    zips = [_strip_dbfs(f.path) for f in files if f.path.lower().endswith(".zip")]
    return sorted(zips)[0] if zips else None

# 1) Reuse manual upload if present; download only if the Volume has no zip
existing = find_existing_zip(RAW_DIR, ZIP_PATH)
if existing:
    ZIP_PATH = _strip_dbfs(existing)
    ZIP_NAME = ZIP_PATH.split("/")[-1]
    print(f"Zip already in Volume, skipping download: {ZIP_PATH}")
else:
    print(f"Downloading {DATA_URL} ...")
    try:
        # Serverless blocks file:/tmp access via dbutils.fs.cp, so stream
        # the download straight into the Volume path (open() on /Volumes works).
        try:
            import requests

            with requests.get(DATA_URL, stream=True, timeout=120) as r:
                r.raise_for_status()
                with open(ZIP_PATH, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1 << 20):
                        if chunk:
                            f.write(chunk)
        except ImportError:
            import urllib.request

            urllib.request.urlretrieve(DATA_URL, _strip_dbfs(ZIP_PATH))
        print(f"Downloaded -> {ZIP_PATH}")
    except Exception as e:
        try:
            have = [f.path for f in dbutils.fs.ls(RAW_DIR)]
        except Exception:
            have = []
        raise RuntimeError(
            f"Download failed ({e}). Free Edition serverless outbound internet is allow-listed, "
            f"so download the zip in your browser from https://data.bis.org/bulkdownload "
            f"(Retail payments -> CPMI cashless payments (CSV, flat)) and upload it via "
            f"Catalog Explorer -> {CATALOG} -> {SCHEMA} -> raw -> Upload to volume, then re-run this cell. "
            f"RAW_DIR now contains: {have}"
        ) from e

display(dbutils.fs.ls(RAW_DIR))

# COMMAND ----------

# 2) Unzip into the Volume (overwrite extracted CSVs).
# Serverless forbids file:/tmp via dbutils.fs.cp, so read the zip and write
# members directly through /Volumes/... paths (plain Python I/O works there).
# Clean stale CSVs first so a smaller refreshed zip doesn't leave old files behind.
for stale in Path(_strip_dbfs(EXTRACT_DIR)).glob("*.csv"):
    stale.unlink()
    print(f"Removed stale extract: {stale}")

with zipfile.ZipFile(_strip_dbfs(ZIP_PATH), "r") as z:
    members = z.namelist()
    print("Archive members:", members)
    for member in members:
        if not member.lower().endswith(".csv"):
            continue
        name = os.path.basename(member) or member.replace("/", "_")
        dest = _strip_dbfs(f"{EXTRACT_DIR}/{name}")
        with z.open(member) as src, open(dest, "wb") as out:
            while True:
                chunk = src.read(1 << 20)
                if not chunk:
                    break
                out.write(chunk)
        print(f"Extracted {member} -> {dest}")

display(dbutils.fs.ls(EXTRACT_DIR))

# COMMAND ----------

# 3) Read CSV as raw strings (no inference — bronze keeps source fidelity)
if spark is None:
    raise RuntimeError(
        "pyspark is not available locally. Install it with `pip install pyspark` "
        "(plus Java 17) and re-run, or run this notebook on Databricks Serverless "
        "where `spark`/`dbutils` are injected. Steps 1-2 (download/unzip) already ran."
    )
from pyspark.sql import functions as F

csv_files = [
    _strip_dbfs(f.path)
    for f in dbutils.fs.ls(EXTRACT_DIR)
    if f.path.lower().endswith(".csv")
]
assert csv_files, f"No CSVs found in {EXTRACT_DIR}"
print("Reading:", csv_files)

raw_df = (
    spark.read.option("header", "true")
    .option("inferSchema", "false")
    .option("mode", "PERMISSIVE")
    .csv(csv_files)
)

raw_df = raw_df.withColumn("_ingest_ts", F.current_timestamp()).withColumn(
    "_source_file", F.lit(ZIP_NAME)
)

print(f"Rows: {raw_df.count():,}, Cols: {len(raw_df.columns)}")
display(raw_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Cell 6
# 4) Write bronze Delta (full overwrite; history preserved via Time Travel)
if _ON_DATABRICKS:
    (
        raw_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("delta.columnMapping.mode", "name")
        .saveAsTable(BRONZE_TABLE)
    )

    spark.sql(f"DESCRIBE HISTORY {BRONZE_TABLE}").select("version", "timestamp", "operation").limit(3).display()

    bronze_count = spark.table(BRONZE_TABLE).count()
    years = [r[0] for r in spark.table(BRONZE_TABLE).select("`TIME_PERIOD:Time period or range`").distinct().orderBy("`TIME_PERIOD:Time period or range`").collect()]
    print(f"BRONZE OK: {bronze_count:,} rows -> {BRONZE_TABLE}; years={years}")
else:
    # Local run has no Unity Catalog: write parquet/delta to the mapped volume dir.
    local_out = _strip_dbfs(f"{VOLUMES_ROOT}/{CATALOG}/{SCHEMA}/bronze_local")
    Path(local_out).mkdir(parents=True, exist_ok=True)
    try:
        (raw_df.write.format("delta").mode("overwrite").save(local_out))
        print(f"BRONZE OK (local delta): {raw_df.count():,} rows -> {local_out}")
    except Exception as e:
        print(f"Delta write failed locally ({e}), falling back to parquet.")
        (raw_df.write.format("parquet").mode("overwrite").save(local_out))
        print(f"BRONZE OK (local parquet): {raw_df.count():,} rows -> {local_out}")