# Databricks notebook source
# 05_validate — post-pipeline gate for the annual refresh Job.
# Fails the Job (raises) if any medallion table is empty or expectations break.
# MAGIC %md
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
assert results["silver"] > 10_000, f"silver unexpectedly small: {results['silver']}"
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
q_ratio = results["quarantine"] / max(results["silver"], 1)
print(f"quarantine_ratio={q_ratio:.4f}")
if q_ratio > 0.05:
    print(f"WARNING: quarantine ratio {q_ratio:.1%} exceeds 5% — inspect {TABLES['quarantine']}")

print("VALIDATE OK")
