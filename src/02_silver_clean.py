# Databricks notebook source
# 02_silver_clean — bronze raw strings -> typed, deduped silver + quarantine
# MAGIC %md
# MAGIC ### 02 · Silver clean
# MAGIC Splits `CODE: Label` columns, casts `year`/`obs_value`, dedupes, quarantines bad rows.

# COMMAND ----------

dbutils.widgets.text("catalog", "bis", "Catalog")
dbutils.widgets.text("schema", "payments", "Schema")

CATALOG = dbutils.widgets.get("catalog").strip() or "bis"
SCHEMA = dbutils.widgets.get("schema").strip() or "payments"
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_cpmi_cashless"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_cpmi_cashless"
QUARANTINE_TABLE = f"{CATALOG}.{SCHEMA}.silver_quarantine"

print(f"BRONZE={BRONZE_TABLE} SILVER={SILVER_TABLE}")

# COMMAND ----------

from pyspark.sql import functions as F

bronze = spark.table(BRONZE_TABLE)
print(f"Bronze rows: {bronze.count():,}")

def code(col_name: str):
    """'BR: Brazil' -> 'BR'"""
    return F.trim(F.split(F.col(f"`{col_name}`"), ":").getItem(0))

def label(col_name: str):
    """'BR: Brazil' -> 'Brazil' (falls back to full string if no colon)"""
    c = F.col(f"`{col_name}`")
    return F.when(F.instr(c, ":") > 0, F.trim(F.split(c, ":").getItem(1))).otherwise(F.trim(c))

silver = (
    bronze
    .withColumn("freq", code("FREQ:Frequency"))
    .withColumn("country_code", code("REP_CTY:Reporting country"))
    .withColumn("country", label("REP_CTY:Reporting country"))
    .withColumn("measure_code", code("MEASURE:Measure"))          # N=Number, V=Value
    .withColumn("measure", label("MEASURE:Measure"))
    .withColumn("instrument_code", code("INSTRUMENT_TYPE:Instrument type"))
    .withColumn("instrument", label("INSTRUMENT_TYPE:Instrument type"))
    .withColumn("direction_code", code("DIRECTION:Direction"))
    .withColumn("year", F.col("`TIME_PERIOD:Time period or range`").cast("int"))
    .withColumn("obs_value", F.col("`OBS_VALUE:Observation Value`").cast("double"))
    .withColumn("unit_mult", F.trim(F.col("`UNIT_MULT:Unit Multiplier`")))
    .withColumn("currency_code", code("UNIT_MEASURE:Unit of measure"))
    .withColumn("currency", label("UNIT_MEASURE:Unit of measure"))
    .withColumn("title", F.col("`TITLE:Title`"))
    .withColumn("table_id", F.trim(F.col("`TABLE:Table`")))
    .withColumn("obs_status_code", code("OBS_STATUS:Observation Status"))
    .withColumn("obs_status", label("OBS_STATUS:Observation Status"))
    .withColumn("collection", F.trim(F.col("`COLLECTION:Collection Indicator`")))
    .select(
        "freq", "country_code", "country", "measure_code", "measure",
        "instrument_code", "instrument", "direction_code",
        "year", "obs_value", "unit_mult", "currency_code", "currency",
        "title", "table_id", "obs_status_code", "obs_status", "collection",
        "_ingest_ts", "_source_file",
    )
)

# COMMAND ----------

# Split good vs quarantine rows
bad_cond = F.col("obs_value").isNull() | F.col("year").isNull() | F.col("country_code").isNull()

quarantine = silver.filter(bad_cond)
good = silver.filter(~bad_cond).filter((F.col("year") >= 2000) & (F.col("year") <= 2030) & (F.col("obs_value") >= 0))

q_count = quarantine.count()
g_count = good.count()
print(f"good={g_count:,} quarantine={q_count:,}")
display(quarantine.limit(20))

# Natural-key dedupe: one observation per country/measure/instrument/year/direction/table
key_cols = ["country_code", "measure_code", "instrument_code", "direction_code", "year", "table_id"]
deduped = good.dropDuplicates(key_cols)
print(f"after dedupe: {deduped.count():,} (removed {g_count - deduped.count():,})")

# COMMAND ----------

# Write silver + quarantine
deduped.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(SILVER_TABLE)
quarantine.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(QUARANTINE_TABLE)

# Expectations (fail fast)
checks = {
    "years_2012_2024": spark.table(SILVER_TABLE).select("year").distinct().orderBy("year").collect(),
    "countries": spark.table(SILVER_TABLE).select("country_code").distinct().count(),
    "measures": sorted(r[0] for r in spark.table(SILVER_TABLE).select("measure_code").distinct().collect()),
    "null_obs_values": spark.table(SILVER_TABLE).filter("obs_value IS NULL").count(),
}
print(checks)
assert checks["countries"] >= 20, f"Expected >=20 countries, got {checks['countries']}"
assert set(checks["measures"]) == {"N", "V"}, f"Expected measures N/V, got {checks['measures']}"
assert checks["null_obs_values"] == 0

# Quick profile: rows per instrument
display(spark.sql(f"""
  SELECT instrument_code, instrument, measure_code, COUNT(*) AS n, MIN(year) AS min_y, MAX(year) AS max_y
  FROM {SILVER_TABLE} GROUP BY ALL ORDER BY n DESC
"""))

print(f"SILVER OK -> {SILVER_TABLE} (+ quarantine -> {QUARANTINE_TABLE})")
