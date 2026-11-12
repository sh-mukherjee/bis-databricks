# BIS Cashless Payments — Delta Medallion ETL on Databricks Free Edition

Medallion pipeline over BIS CPMI cashless payments (Red Book, Tables T5/T6).

- **Source:** `https://data.bis.org/static/bulk/WS_CPMI_CASHLESS_csv_flat.zip` (flat CSV, ~19.5k rows, 26 countries, 2012–2024, updated 27 Apr 2026)
- **Compute:** Serverless notebooks only (Free Edition constraint)
- **Storage:** Unity Catalog `bis.payments.*` + Volume `bis.payments.raw`
- **Workflow:** Develop locally → push to Git → Databricks Git Folders → run notebooks in order

> Attribution required: BIS statistics used under the [BIS Terms and Conditions](https://www.bis.org/terms_conditions.htm). Cite BIS Data Portal, CPMI Red Book statistics.

## Repo layout

```text
src/
  00_setup.sql          # one-time: CATALOG bis, SCHEMA payments, VOLUME raw
  01_ingest_bronze.py   # notebook: download/unzip → bronze Delta (raw strings + lineage)
  02_silver_clean.py    # notebook: parse CODE:Label, cast types, dedupe, quarantine
  03_gold_analytics.py  # notebook: country/year totals, instrument share, YoY growth
  04_analysis_sql.sql   # dashboard queries (run on 2X-Small SQL Warehouse); years derived from data
  05_validate.py        # notebook: post-pipeline gate (fails the Job on broken data)
jobs/
  annual_refresh.job.json  # Databricks Job: 01 -> 02 -> 03 -> 05, annual schedule (created PAUSED)
```

All `*.py` files are Databricks notebooks (`# Databricks notebook source` header) so Git Folders renders them directly. No relative imports — serverless CWD is not guaranteed.

## Quickstart (Free Edition, Chrome)

### 1. Create the `BIS` catalog (one time)

Free Edition workspaces are Unity Catalog-enabled with default storage. Creating a new catalog requires metastore admin (you have it on your own Free Edition metastore).

In a new SQL Editor / notebook connected to **Serverless**:

```sql
-- run src/00_setup.sql, or paste:
CREATE CATALOG IF NOT EXISTS bis COMMENT 'BIS CPMI payments analytics';
CREATE SCHEMA IF NOT EXISTS bis.payments COMMENT 'Cashless payments medallion tables';
CREATE VOLUME IF NOT EXISTS bis.payments.raw COMMENT 'Raw BIS bulk zips and extracted CSVs';
```

Verify: Catalog Explorer → `bis` → `payments` → `raw`.

> If `CREATE CATALOG` fails with a permissions error, fall back to your default catalog (usually `workspace`):
> change the `catalog` widget to `workspace` when running the notebooks. Everything else is identical.

### 2. Connect this folder via Git Folders

1. Push this folder to GitHub (or Azure DevOps).
2. In Databricks: **Workspace → Repos / Git Folders → Add Git Folder** → paste repo URL.
3. Open `src/01_ingest_bronze.py` from the Git Folder (not a personal copy, so pulls stay clean).

### 3. Run the pipeline in order

| Order | Notebook | Serverless? | What it does |
|-------|----------|-------------|--------------|
| 0 | `00_setup.sql` | SQL Warehouse or Serverless | catalog/schema/volume |
| 1 | `01_ingest_bronze.py` | Serverless | fetch zip → `/Volumes/bis/payments/raw/` → `bis.payments.bronze_cpmi_cashless` |
| 2 | `02_silver_clean.py` | Serverless | typed/clean → `silver_cpmi_cashless` + `silver_quarantine` |
| 3 | `03_gold_analytics.py` | Serverless | `gold_country_year`, `gold_instrument_share`, `gold_yoy_growth` |
| 4 | `04_analysis_sql.sql` | 2X-Small Warehouse | dashboard queries (optional, not part of the Job) |
| 5 | `05_validate.py` | Serverless | structural gate: unique country/year keys, no half-empty or NaN rows, shares within [0, 1], YoY row count matches |

Each notebook takes `catalog` / `schema` widgets (defaults `bis` / `payments`), displays row counts and data-quality checks.

### 4. Internet-allowlist fallback (important)

Free Edition serverless outbound internet is restricted to trusted domains. `01_ingest_bronze` first tries:

```python
requests.get("https://data.bis.org/static/bulk/WS_CPMI_CASHLESS_csv_flat.zip")
```

If that fails with a connection error:

1. Download the zip **locally in Chrome** from https://data.bis.org/bulkdownload (section *Retail payments… → CPMI cashless payments (CSV, flat)*).
2. In Databricks: **Catalog Explorer → bis → payments → raw → Upload to volume** → upload the `.zip`.
3. Set the `force_download` widget to `false` and re-run `01_ingest_bronze`. By default (`true`) it always downloads the latest zip, which the scheduled Job relies on. — it auto-detects the existing zip and skips the download.

### 5. Build the dashboard

Create a new SQL Dashboard on the 2X-Small warehouse, add one tile per query in `src/04_analysis_sql.sql`:

- Global cashless value trend by instrument (stacked area)
- Top 10 countries (latest year) + YoY growth (bar)
- Cards vs credit transfers share (table/map)

### 6. Refresh (annual)

BIS Red Book releases yearly (autumn T+1 for reference year T). The Job `jobs/annual_refresh.job.json` automates this:

1. Create it: `databricks jobs create --json @jobs/annual_refresh.job.json` (git source is this repo, `main` branch).
2. It is created **PAUSED**. Run it once manually from Workflows; when all four tasks (ingest, silver, gold, validate) succeed, set `pause_status` to `UNPAUSED`.
3. Schedule: Quartz cron `0 0 7 11 11 ?` = 07:00 UTC on 11 Nov each year (Quartz needs 6 fields; a 5-field cron is invalid).

Each run re-downloads the zip (`force_download=true`), fully overwrites bronze/silver/gold, and then `05_validate` fails the Job if the data looks wrong. Bronze history remains via Delta Time Travel (`DESCRIBE HISTORY`). To refresh manually, run notebooks 01-03 and 05 in order.
## Data notes

Flat CSV columns are `CODE: Label` encoded, e.g. `BR: Brazil`, `V: Value`, `F: Card and e-money payments`. Silver splits these into `*_code` + human-readable columns.

- `MEASURE`: `N` = Number (millions of transactions), `V` = Value (millions of domestic currency)
- `INSTRUMENT_TYPE`: `A` total cashless, `B` credit transfers, `C` direct debits, `D` cheques, `F` card + e-money, `E` other; `M/N/L` = cash withdrawals/deposits (excluded from gold cashless views), `O/R` = memo items
- `TIME_PERIOD`: year 2012–2024
- Values are in **domestic currency** — do not compare values cross-country without FX normalization. Volumes and shares are comparable.
- **Missing values:** BIS encodes missing observations as the text `NaN` (status `L`/`M`). Silver quarantines them in `silver_quarantine` with `_reject_reason = 'source_missing_value'`; other rejects (null/out-of-range year, negative value, null country) get their own reason. Nothing is dropped silently.
- **Overlapping series:** each instrument code also holds sub-series (e.g. `A` includes "fast payments", `B` includes "cross-border credit transfers"). Gold keeps only the six headline series (A-F, selected by title) to avoid double counting.
- Countries/years with no valid total are absent from `gold_country_year` and `gold_instrument_share`, since a share cannot be computed without a total.
