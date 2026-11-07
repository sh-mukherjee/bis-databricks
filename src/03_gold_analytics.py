# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Gold Analytics Overview
# MAGIC %md
# MAGIC # 03_gold_analytics — silver -> analytics-ready gold tables (Delta, OPTIMIZE + ZORDER)
# MAGIC
# MAGIC ### 03 · Gold analytics
# MAGIC Builds `gold_country_year` (totals), `gold_instrument_share` (mix), `gold_yoy_growth` (dynamics).
# MAGIC Values are in **millions of domestic currency** (not FX-adjusted — don't rank values cross-country);
# MAGIC volumes (millions of txns) and shares are comparable.

# COMMAND ----------

dbutils.widgets.text("catalog", "bis", "Catalog")
dbutils.widgets.text("schema", "payments", "Schema")

CATALOG = dbutils.widgets.get("catalog").strip() or "bis"
SCHEMA = dbutils.widgets.get("schema").strip() or "payments"
SILVER = f"{CATALOG}.{SCHEMA}.silver_cpmi_cashless"
T_COUNTRY_YEAR = f"{CATALOG}.{SCHEMA}.gold_country_year"
T_INSTR_SHARE = f"{CATALOG}.{SCHEMA}.gold_instrument_share"
T_YOY = f"{CATALOG}.{SCHEMA}.gold_yoy_growth"

# Cashless scope: exclude cash withdrawals/deposits (M/N/L) and memo items (O/R)
CASHLESS_INSTRUMENTS = ["B", "C", "D", "F", "E"]  # credit transfers, direct debits, cheques, card+e-money, other
TOTAL_CODE = "A"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

silver_all = spark.table(SILVER)
print(f"Silver rows: {silver_all.count():,}")

# The source mixes headline series with overlapping sub-series under the same instrument_code
# (e.g. instrument A holds 'total' AND 'fast payments'; B holds 'credit transfers' AND
# 'cross-border credit transfers, sent'). Summing by instrument_code double counts, so keep
# only the headline series, identified by the title suffix after "<Country> - ".
HEADLINE_SERIES = {
    "A": "cashless payments, total",
    "B": "cashless payments, credit transfers",
    "C": "cashless payments, direct debits",
    "D": "cashless payments, cheques",
    "E": "cashless payments, other payment instruments",
    "F": "cashless payments, card and e-money payments with cards and e-money issued inside the country",
}
title_core = F.lower(F.trim(F.regexp_replace("title", r"^.*? - (Value|Volume) of ", "")))
headline_cond = F.lit(False)
for _code, _suffix in HEADLINE_SERIES.items():
    headline_cond = headline_cond | ((F.col("instrument_code") == _code) & (title_core == _suffix))
silver = silver_all.filter(headline_cond)
print(f"Headline-series rows: {silver.count():,}")

# --- Gold 1: country-year totals (instrument A), N and V pivoted to columns ---
totals_long = silver.filter(F.col("instrument_code") == TOTAL_CODE)
gold_country_year = (
    # N rows carry unit "373: Pure Number", V rows carry the currency, so currency must NOT be a
    # grouping key (it split each country-year into two half-empty rows). Take it from V rows.
    totals_long.groupBy("country_code", "country", "year")
    .agg(
        F.sum(F.when(F.col("measure_code") == "N", F.col("obs_value"))).alias("volume_mio_txns"),
        F.sum(F.when(F.col("measure_code") == "V", F.col("obs_value"))).alias("value_mio_domestic"),
        F.max(F.when(F.col("measure_code") == "V", F.col("currency_code"))).alias("currency_code"),
        F.max(F.when(F.col("measure_code") == "V", F.col("currency"))).alias("currency"),
    )
    .select("country_code", "country", "year", "currency_code", "currency", "volume_mio_txns", "value_mio_domestic")
    .withColumn("_built_ts", F.current_timestamp())
)
gold_country_year.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(T_COUNTRY_YEAR)

# --- Gold 2: instrument mix with share of total value/volume ---
parts = silver.filter(F.col("instrument_code").isin(*CASHLESS_INSTRUMENTS))
totals = (
    totals_long.groupBy("country_code", "year", "measure_code")
    .agg(F.sum("obs_value").alias("total_obs"))
)
gold_instrument_share = (
    parts.groupBy("country_code", "country", "year", "measure_code", "measure",
                  "instrument_code", "instrument", "currency_code", "currency")
    .agg(F.sum("obs_value").alias("obs_value"))
    .join(totals, on=["country_code", "year", "measure_code"], how="left")
    .withColumn("share_of_total", F.col("obs_value") / F.nullif(F.col("total_obs"), F.lit(0)))
    .withColumn("_built_ts", F.current_timestamp())
)
gold_instrument_share.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(T_INSTR_SHARE)

# --- Gold 3: YoY growth + CAGR on totals ---
w = Window.partitionBy("country_code", "currency_code").orderBy("year")
base = spark.table(T_COUNTRY_YEAR)
gold_yoy = (
    base
    .withColumn("prev_value", F.lag("value_mio_domestic").over(w))
    .withColumn("prev_volume", F.lag("volume_mio_txns").over(w))
    .withColumn("value_yoy_pct", (F.col("value_mio_domestic") - F.col("prev_value")) / F.nullif(F.col("prev_value"), F.lit(0)) * 100)
    .withColumn("volume_yoy_pct", (F.col("volume_mio_txns") - F.col("prev_volume")) / F.nullif(F.col("prev_volume"), F.lit(0)) * 100)
    .withColumn("_built_ts", F.current_timestamp())
)
gold_yoy.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(T_YOY)

# COMMAND ----------

# Optimize for dashboard queries (small tables — fast and cheap)
for t in [T_COUNTRY_YEAR, T_INSTR_SHARE, T_YOY]:
    spark.sql(f"OPTIMIZE {t} ZORDER BY (country_code, year)")
    n = spark.table(t).count()
    print(f"{t}: {n:,} rows")

display(spark.sql(f"SELECT * FROM {T_COUNTRY_YEAR} ORDER BY year DESC, volume_mio_txns DESC LIMIT 15"))
display(spark.sql(f"""
  SELECT country, year, instrument, obs_value, ROUND(share_of_total*100, 1) AS share_pct
  FROM {T_INSTR_SHARE} WHERE measure_code='V' AND year=(SELECT MAX(year) FROM {T_INSTR_SHARE}) ORDER BY obs_value DESC LIMIT 15
"""))
display(spark.sql(f"""
  SELECT country, year, value_mio_domestic, ROUND(value_yoy_pct,1) AS value_yoy_pct,
         volume_mio_txns, ROUND(volume_yoy_pct,1) AS volume_yoy_pct
  FROM {T_YOY} WHERE year >= 2022 ORDER BY country, year LIMIT 30
"""))

print("GOLD OK")