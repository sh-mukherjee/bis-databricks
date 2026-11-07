-- 04_analysis_sql — dashboard queries over bis.payments gold tables.
-- Run on the 2X-Small SQL Warehouse. Replace USE CATALOG if you used the fallback catalog.
-- Each -- TILE -- block = one dashboard tile.

USE CATALOG bis;
USE SCHEMA payments;

-- TILE 1: Global cashless VALUE trend by instrument (stacked area)
-- Values are summed across currencies for shape-of-growth only; do NOT read as a single currency total.
SELECT year, instrument, SUM(obs_value) AS value_mio_domestic_sum
FROM gold_instrument_share
WHERE measure_code = 'V'
GROUP BY ALL
ORDER BY year, instrument;

-- Years are derived from the data (latest / earliest year in gold), so no edits are needed after a refresh.
-- TILE 2: Top 10 countries by VOLUME in the latest year + YoY growth (bar + table)
SELECT g.country, g.volume_mio_txns, y.volume_yoy_pct, g.value_mio_domestic, y.value_yoy_pct, g.currency
FROM gold_country_year g
JOIN gold_yoy_growth y USING (country_code, country, year, currency_code, currency)
WHERE g.year = (SELECT MAX(year) FROM gold_country_year)
ORDER BY g.volume_mio_txns DESC
LIMIT 10;

-- TILE 3: Cards vs credit transfers share of VALUE, latest year (table)
SELECT country, instrument, ROUND(share_of_total * 100, 1) AS share_pct
FROM gold_instrument_share
WHERE measure_code = 'V' AND year = (SELECT MAX(year) FROM gold_instrument_share) AND instrument_code IN ('F', 'B')
ORDER BY country, instrument;

-- TILE 4: Cheque decline, value share earliest vs latest year (table)
SELECT
  a.country,
  a.year AS first_year,
  b.year AS latest_year,
  ROUND(a.share_of_total * 100, 1) AS cheque_share_first_pct,
  ROUND(b.share_of_total * 100, 1) AS cheque_share_latest_pct,
  ROUND((b.share_of_total - a.share_of_total) * 100, 1) AS pp_change
FROM gold_instrument_share a
JOIN gold_instrument_share b
  ON a.country_code = b.country_code AND a.measure_code = b.measure_code AND a.instrument_code = b.instrument_code
WHERE a.measure_code = 'V' AND a.instrument_code = 'D' AND a.year = (SELECT MIN(year) FROM gold_instrument_share)
  AND b.year = (SELECT MAX(year) FROM gold_instrument_share)
ORDER BY pp_change;

-- TILE 5: Fastest-growing cashless VALUE, 5-year CAGR ending at the latest year (bar)
-- CAGR computed on domestic-currency totals per country.
WITH span AS (
  SELECT country, MIN(year) AS y0, MAX(year) AS y1,
         MIN_BY(value_mio_domestic, year) AS v0, MAX_BY(value_mio_domestic, year) AS v1
  FROM gold_country_year
  WHERE year IN ((SELECT MAX(year) - 5 FROM gold_country_year), (SELECT MAX(year) FROM gold_country_year)) AND value_mio_domestic IS NOT NULL AND value_mio_domestic > 0
  GROUP BY ALL HAVING COUNT(*) = 2
)
SELECT country, ROUND((POWER(v1 / v0, 1.0 / (y1 - y0)) - 1) * 100, 1) AS cagr_pct_5y, y0 AS from_year, y1 AS to_year
FROM span
ORDER BY cagr_pct_5y DESC;
