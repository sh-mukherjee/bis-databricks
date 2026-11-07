# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Validate Overview
# MAGIC %md
# MAGIC # 05_validate — post-pipeline gate for the annual refresh Job.
# MAGIC # Fails the Job (raises) if any medallion table is empty or expectations break.
# MAGIC
# MAGIC ### 05 · Validate (Job gate)
# MAGIC Used as the 4th task in `jobs/annual_refresh.job.json`. Safe to run manually too.

# COMMAND ----------

dbutils.widgets.text("catalog", "bis", "Catalog")
dbutils.widgets.text("schema", "payments", "Schema")

CATALOG = dbutils.widgets.get("catalog").strip() or "bis"
SCHEMA = dbutils.widgets.get("schema").strip() or "payments"

TABLES = {
    "bronze": f"{CATALOG}.{SCHEMA}.bronze_cpmi_cashless",
    "silver": f"{CATALOG}.{SCHEMA}.silver_cpmi_cashless",
    "quarantine": f"{CATALOG}.{SCHEMA}.silver_quarantine",
    "gold_country_year": f"{CATALOG}.{SCHEMA}.gold_country_year",
    "gold_instrument_share": f"{CATALOG}.{SCHEMA}.gold_instrument_share",
    "gold_yoy_growth": f"{CATALOG}.{SCHEMA}.gold_yoy_growth",
}

# COMMAND ----------

results = {}
for name, table in TABLES.items():
    n = spark.table(table).count()
    results[name] = n
    print(f"{name:22s} {n:>10,}  -> {table}")

# Hard gates: pipeline is broken if core tables are empty
assert results["bronze"] > 10_000, f"bronze unexpectedly small: {results['bronze']}"
assert results["silver"] > 5_000, f"silver unexpectedly small: {results['silver']}"
assert results["gold_country_year"] > 100, "gold_country_year empty"
assert results["gold_instrument_share"] > 100, "gold_instrument_share empty"
assert results["gold_yoy_growth"] > 100, "gold_yoy_growth empty"

# Expectation: both measures present, sane year range
years = sorted(r["year"] for r in spark.table(TABLES["silver"]).select("year").distinct().collect())
measures = sorted(r["measure_code"] for r in spark.table(TABLES["silver"]).select("measure_code").distinct().collect())
print(f"years={years} measures={measures}")
assert measures == ["N", "V"], f"expected measures [N, V], got {measures}"
assert min(years) <= 2012 and max(years) >= 2023, f"unexpected year span: {years}"

# Quarantine should stay small (<5% of silver); warn but don't fail
# "source_missing_value" rows are BIS observations flagged missing (NaN): expected, so not counted here
unexpected_q = spark.table(TABLES["quarantine"]).filter("_reject_reason <> 'source_missing_value'").count()
print(f"quarantine: total={results['quarantine']:,} unexpected={unexpected_q:,}")
q_ratio = unexpected_q / max(results["bronze"], 1)
print(f"quarantine_ratio (vs bronze)={q_ratio:.4f}")
if q_ratio > 0.05:
    print(f"WARNING: quarantine ratio {q_ratio:.1%} exceeds 5% — inspect {TABLES['quarantine']}")

# COMMAND ----------

# Structural gates (these catch logic bugs that plain row counts miss)
from pyspark.sql import functions as F

cy = spark.table(TABLES["gold_country_year"])
yoy = spark.table(TABLES["gold_yoy_growth"])
share = spark.table(TABLES["gold_instrument_share"])

# 1) gold_country_year: exactly one row per country + year (guards against split/duplicated rows)
dups = cy.groupBy("country_code", "year").count().filter("count > 1").count()
assert dups == 0, f"gold_country_year has {dups} duplicated country/year keys"

# 2) Both measures populated on every row (guards against half-empty N/V rows)
null_rows = cy.filter(
    "volume_mio_txns IS NULL OR value_mio_domestic IS NULL OR isnan(volume_mio_txns) OR isnan(value_mio_domestic)"
).count()
assert null_rows == 0, f"gold_country_year has {null_rows} rows missing volume or value"
assert cy.filter("currency_code IS NULL").count() == 0, "gold_country_year has rows without a currency"

# 3) YoY table must mirror the country-year table
assert results["gold_yoy_growth"] == results["gold_country_year"], (
    f"gold_yoy_growth rows ({results['gold_yoy_growth']}) != gold_country_year rows ({results['gold_country_year']})"
)

# 4) Instrument shares are fractions of the total (guards against overlapping sub-series being summed)
bad_share_df = share.filter("share_of_total IS NULL OR isnan(share_of_total) OR share_of_total < 0 OR share_of_total > 1.0001")
bad_share = bad_share_df.count()
if bad_share:
    # Show what is wrong before failing (null = no matching total, >1 = part exceeds total)
    display(bad_share_df.groupBy("instrument_code", "measure_code", F.col("share_of_total").isNull().alias("null_share")).count().orderBy("instrument_code"))
    display(bad_share_df.orderBy("country_code", "year").limit(30))
assert bad_share == 0, f"gold_instrument_share has {bad_share} rows with share outside [0, 1]"

# Parts should roughly add up to the total per country/year/measure; warn only (sources can have gaps)
over = (
    share.groupBy("country_code", "year", "measure_code")
    .agg(F.sum("share_of_total").alias("s"))
    .filter("s > 1.02")
    .count()
)
if over:
    print(f"WARNING: {over} country/year/measure groups have instrument shares summing to >102%")
print("VALIDATE OK")