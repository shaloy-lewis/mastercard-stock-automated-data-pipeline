# Repository guidance

These instructions apply to the entire repository.

## Project context

This project is an automated data pipeline for Mastercard stock trading data.
Each dataset row represents the trading activity for one trading day.

- Source: [MasterCard Stock Data - Latest and Updated on Kaggle](https://www.kaggle.com/datasets/kalilurrahman/mastercard-stock-data-latest-and-updated)
- Local input: `data/Mastercard_stock.csv`
- The root `data/` directory is ignored by Git. Keep source downloads and generated
  data out of version control.
- Windowed ingestion is implemented in `dags/mastercard_pipeline/ingestion.py`
  and invoked by the Airflow 2.x DAG in `dags/mastercard_ingestion.py`.
  Phase 2 cleaning is in `dags/mastercard_pipeline/cleaning.py`; the DAG returns
  cleaned rows in its existing task's XCom. Tests live in `tests/`; install
  `requirements.txt` and run `python -m unittest discover -s tests -v`.
  Phase 3 analysis in `dags/mastercard_pipeline/analysis.py` finds the row with
  maximum `High` and the row with minimum `Low`. A downstream
  `analyze_stock_data` task returns those records in its XCom. Phase 4 export in
  `dags/mastercard_pipeline/export.py` writes a two-row CSV through the
  `save_to_csv` task. See `README.md` for setup and export schema.
- `compose.yaml` provides local Airflow 2.11.0/Python 3.12 execution in Docker.
  DAGs and source data are read-only mounts; a separate writable mount exposes
  host `data/output/` at `/opt/airflow/output`. The named `airflow-home` volume holds
  metadata and offsets. Preserve that volume unless a reset is explicitly intended.

## Dataset schema

The local CSV has the following header:

```text
Date,Open,High,Low,Close,Volume,Dividends,Stock Splits
```

| Column | Meaning / handling |
| --- | --- |
| `Date` | Trading date for the record. The local CSV includes timestamps with timezone offsets; preserve the source trading date when parsing. |
| `Open` | Stock price when trading opened that day. |
| `High` | Highest stock price reached during the trading day. |
| `Low` | Lowest stock price reached during the trading day. |
| `Close` | Final trading price at the end of the trading day. |
| `Volume` | Total number of shares traded during the day. Higher volume typically indicates greater trading activity. |
| `Dividends` | Additional source field present in the local CSV. Preserve it; verify its units and semantics before using it in calculations. |
| `Stock Splits` | Additional source field present in the local CSV. Preserve it; verify its encoding before using it in calculations. |

Do not assume that prices are adjusted or unadjusted for corporate actions.
Verify the source methodology before applying dividend or stock-split adjustments.

## Data handling

- Preserve the downloaded CSV as raw input. Write transformed outputs separately
  under `data/`; do not overwrite the source file during normal processing.
- Keep source column names intact during ingestion. Document any downstream
  renaming, type conversion, filtering, or derived fields.
- Parse dates explicitly and handle timezone offsets consistently. Avoid UTC
  conversions that accidentally change the intended trading date.
- Treat price fields as numeric and volume as a nonnegative whole-number count.
  Retain source precision during processing and round only for presentation or
  an explicitly documented output requirement.
- Do not silently fill missing prices, replace missing volume with zero, drop
  invalid records, or deduplicate conflicting records. Report the issue and make
  the handling policy explicit.
- Phase 2 explicitly skips invalid records and later duplicate trading dates,
  logging their source row numbers and reasons. It consumes a fixed number of
  source rows per run, including rejections, and checkpoints after cleaning the
  window. Dates become `YYYY-MM-DD` in the source timezone; numeric strings keep
  their exact source precision after trimming and numeric validation.
- Trading data does not require a row for every calendar day. Do not synthesize
  weekend or holiday records unless a documented downstream use requires them.
- Do not assume the local file is current because the dataset title says
  "Latest and Updated." Record source and retrieval details when adding ingestion.

## Implementation and validation

- Keep ingestion, validation, transformation, and output responsibilities clear.
  Make repeated pipeline runs avoid duplicate output records.
- Validate required columns, parseable dates, missing values, numeric types,
  positive prices, and nonnegative integral volume.
- Check for duplicate trading dates and enforce `Low <= High`,
  `Low <= Open <= High`, and `Low <= Close <= High`.
- Preserve additional source columns or explicitly document why an output omits
  them. Report schema changes rather than silently accepting incompatible input.
- Validate actual date coverage and sort records explicitly before time-based
  calculations. Do not rely on input order or a hardcoded row count.
- Analysis compares source numeric strings with `Decimal` and preserves all
  source fields in its `highest`/`lowest` result rows. Ties select the earliest
  trading date; empty clean windows return `None` for both results. Analysis
  must not advance the ingestion offset or mutate the clean input window.
- Export uses the full clean window's earliest/latest dates for
  `YYYY-MM-DD_to_YYYY-MM-DD.csv`, with columns
  `type,date,open,high_or_low,close,volume` and `HIGHEST` then `LOWEST` rows.
  The summary omits other source fields as documented in `README.md`; clean and
  analytical XComs retain them. Preserve numeric strings. Empty clean windows
  produce no file. Publish completed CSVs with atomic replacement so rerunning
  export does not append duplicates or expose partial output. Export must not
  change the ingestion offset.
- When adding pipeline logic, cover meaningful cases such as malformed rows,
  missing columns, timezone handling, duplicate dates, and repeat runs. Use small
  synthetic fixtures so checks can run without the ignored Kaggle download.
- Keep credentials and tokens out of code, logs, and version control. Use
  environment variables for any authenticated data access.
- Update `README.md` with setup, dependencies, data acquisition, execution, and
  validation commands as those workflows are implemented. Report what was
  actually checked; do not claim unrun checks passed.
