# data_sample — do NOT commit large BIS zips/CSVs

`.gitignore` excludes `*.zip` and `*.csv` at repo root, so this folder stays light for Git Folders
(Free Edition limit: 1,000 MB working branch, 20,000 files).

## How to get a tiny local sample (optional, for quick schema checks)

PowerShell (Windows):

```powershell
# 1. Download (315 KB zip, ~9.6 MB CSV)
Invoke-WebRequest `
  -Uri "https://data.bis.org/static/bulk/WS_CPMI_CASHLESS_csv_flat.zip" `
  -OutFile "$env:TEMP\WS_CPMI_CASHLESS_csv_flat.zip"

# 2. Extract
Expand-Archive "$env:TEMP\WS_CPMI_CASHLESS_csv_flat.zip" "$env:TEMP\bis_cashless" -Force

# 3. Take first 1000 rows for local inspection (kept OUTSIDE the repo)
python -c "import itertools; src=open(r'$env:TEMP\bis_cashless\WS_CPMI_CASHLESS_csv_flat.csv',encoding='utf-8-sig'); dst=open(r'$env:TEMP\bis_cashless_sample.csv','w',encoding='utf-8',newline=''); [dst.write(l) for l in itertools.islice(src,1001)]; print('wrote sample')"
```

## What the pipeline uses instead

The notebooks read directly from the Unity Volume `/Volumes/bis/payments/raw/`, NOT from this folder:

- `01_ingest_bronze` downloads the zip straight into the Volume (or reuses your manual upload).
- No local data file needs to be committed.

## Manual-upload fallback (when serverless internet is blocked)

1. Download the zip in Chrome from https://data.bis.org/bulkdownload
   (section *Retail payments… → CPMI cashless payments (CSV, flat)*).
2. Databricks → Catalog Explorer → `bis` → `payments` → `raw` → **Upload to volume**.
3. Re-run `src/01_ingest_bronze.py` — it skips the download if the zip is already there.

Attribution: BIS Data Portal, CPMI Red Book statistics. See repo `README.md`.
