# Databricks notebook source
# 01_ingest_bronze — BIS CPMI cashless bulk zip -> Unity Volume -> bronze Delta
# Free Edition: Serverless only. Uses absolute /Volumes/... paths (no relative paths, no DBFS).
# MAGIC %md
# MAGIC ### 01 · Bronze ingest
# MAGIC Downloads `WS_CPMI_CASHLESS_csv_flat.zip` (or reuses a manually uploaded copy),
# MAGIC extracts to the `raw` Volume, reads CSV as all-strings, adds lineage, overwrites bronze.

# COMMAND ----------

dbutils.widgets.text("catalog", "bis", "Catalog")
dbutils.widgets.text("schema", "payments", "Schema")
dbutils.widgets.text("data_url", "https://data.bis.org/static/bulk/WS_CPMI_CASHLESS_csv_flat.zip", "BIS bulk URL")

CATALOG = dbutils.widgets.get("catalog").strip() or "bis"
SCHEMA = dbutils.widgets.get("schema").strip() or "payments"
DATA_URL = dbutils.widgets.get("data_url").strip()

RAW_DIR = f"/Volumes/{CATALOG}/{SCHEMA}/raw"
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

def volume_has_file(path: str) -> bool:
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:
        return False

# 1) Download if not already in the Volume (manual-upload fallback supported)
if volume_has_file(ZIP_PATH):
    print(f"Zip already in Volume, skipping download: {ZIP_PATH}")
else:
    print(f"Downloading {DATA_URL} ...")
    try:
        import requests
        with requests.get(DATA_URL, stream=True, timeout=120) as r:
            r.raise_for_status()
            # Write to driver local tmp first, then copy into Volume (serverless-safe)
            tmp_zip = f"/tmp/{ZIP_NAME}"
            with open(tmp_zip, "wb") as f:
                for chunk in r.iter_content(chunk_size=1 << 20):
                    if chunk:
                        f.write(chunk)
            dbutils.fs.cp(f"file:{tmp_zip}", ZIP_PATH)
            os.remove(tmp_zip)
        print(f"Downloaded -> {ZIP_PATH}")
    except Exception as e:
        raise RuntimeError(
            f"Download failed ({e}). Free Edition serverless outbound internet is allow-listed. "
            f"Fallback: download the zip in your browser from https://data.bis.org/bulkdownload "
            f"(Retail payments -> CPMI cashless payments (CSV, flat)) and upload it via "
            f"Catalog Explorer -> {CATALOG} -> {SCHEMA} -> raw -> Upload to volume, then re-run this cell."
        ) from e

display(dbutils.fs.ls(RAW_DIR))

# COMMAND ----------

# 2) Unzip into the Volume (overwrite extracted CSVs)
local_zip = "/tmp/bronze_dl.zip"
dbutils.fs.cp(ZIP_PATH, f"file:{local_zip}")
os.makedirs("/tmp/bronze_extract", exist_ok=True)
with zipfile.ZipFile(local_zip, "r") as z:
    z.extractall("/tmp/bronze_extract")
    members = z.namelist()
print("Archive members:", members)

# Copy extracted CSV(s) into the Volume
import glob
for csv_file in glob.glob("/tmp/bronze_extract/*.csv"):
    name = os.path.basename(csv_file)
    dbutils.fs.cp(f"file:{csv_file}", f"{EXTRACT_DIR}/{name}", recurse=False)
    print(f"Copied {name} -> {EXTRACT_DIR}/{name}")

display(dbutils.fs.ls(EXTRACT_DIR))

# COMMAND ----------

# 3) Read CSV as raw strings (no inference — bronze keeps source fidelity)
from pyspark.sql import functions as F

csv_files = [f.path for f in dbutils.fs.ls(EXTRACT_DIR) if f.path.lower().endswith(".csv")]
assert csv_files, f"No CSVs found in {EXTRACT_DIR}"
print("Reading:", csv_files)

raw_df = (
    spark.read.option("header", "true")
    .option("mode", "PERMISSIVE")
    .csv(csv_files)
)

raw_df = raw_df.withColumn("_ingest_ts", F.current_timestamp()).withColumn(
    "_source_file", F.lit(ZIP_NAME)
)

print(f"Rows: {raw_df.count():,}, Cols: {len(raw_df.columns)}")
display(raw_df.limit(10))

# COMMAND ----------

# 4) Write bronze Delta (full overwrite; history preserved via Time Travel)
(
    raw_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(BRONZE_TABLE)
)

spark.sql(f"DESCRIBE HISTORY {BRONZE_TABLE}").select("version", "timestamp", "operation").limit(3).display()

bronze_count = spark.table(BRONZE_TABLE).count()
years = [r[0] for r in spark.table(BRONZE_TABLE).select("`TIME_PERIOD:Time period or range`").distinct().orderBy("`TIME_PERIOD:Time period or range`").collect()]
print(f"BRONZE OK: {bronze_count:,} rows -> {BRONZE_TABLE}; years={years}")
