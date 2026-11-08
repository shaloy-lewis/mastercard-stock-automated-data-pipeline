# Mastercard stock automated data pipeline

The pipeline reads a fixed window of daily Mastercard stock records from CSV,
cleans and validates each record, and stores the next source-row offset in an
Airflow Variable. Ingestion uses Python's `csv` and `os` modules. Phase 2 uses
`pandas.to_datetime()` for dates, `float()` for numeric validation, and `.strip()`
to remove surrounding whitespace. Phase 3 analyzes the clean window to find the
day with the highest `High` and the day with the lowest `Low`. Phase 4 writes
those two records to a date-range-named CSV under `data/output/`.

## Dataset

Download [MasterCard Stock Data - Latest and Updated from Kaggle](https://www.kaggle.com/datasets/kalilurrahman/mastercard-stock-data-latest-and-updated)
and place the CSV at `data/Mastercard_stock.csv`. The root `data/` directory is
ignored by Git. Keep the source file unchanged while consuming its windows;
replacing or reordering records invalidates a positional offset.

The required columns are `Date`, `Open`, `High`, `Low`, `Close`, and `Volume`.
Additional columns, including `Dividends` and `Stock Splits`, are preserved.

## How ingestion works

The reader is in `dags/mastercard_pipeline/ingestion.py`. The DAG invokes
`read_clean_stock_window()` from `dags/mastercard_pipeline/cleaning.py` to combine
ingestion and cleaning within the same task. Its clean-window XCom feeds the
downstream `analyze_stock_data` task.

1. Check that the dataset file exists using `os.path.isfile`.
2. Read `mastercard_stock_offset` using `Variable.get`, defaulting to `0`.
3. Open the file with `csv.DictReader` and scan past the already consumed records.
4. Read the next `window_size` source records (default: **10**), cleaning each
   record and reporting any rejected rows.
5. After the entire source window has been examined, save
   `offset + source_rows_read` using `Variable.set` and return the accepted rows
   as a list of dictionaries.

The header does not count toward the offset. With a window of 10, successive
runs examine source records 1-10, 11-20, and 21-30. Rejected records still count
toward the source window and offset. For example, if 3 of the first 10 records
are rejected, the clean window has 7 rows and the next offset is 10. The last
source window can contain fewer than 10 records.

At end of file the reader returns `[]` and leaves the offset unchanged. An
all-rejected window also returns `[]`, but advances the offset by the source rows
examined. Logs distinguish these cases with source, accepted, rejected, and
offset counts. No extra rows are read to fill gaps left by rejected records.

## Phase 2: data cleaning

The cleaner returns a new record and preserves the source CSV. It:

- Strips surrounding whitespace from every column value.
- Converts a full ISO timestamp or calendar date to `YYYY-MM-DD` using
  `pandas.to_datetime()`, preserving the trading day in the source timezone.
- Rejects missing required values, including blank strings and common null
  markers such as `NULL`, `NaN`, and `N/A`.
- Uses `float()` and a finite-number check to reject invalid numeric values,
  including infinity. All four stock prices must be positive.
- Enforces `Low <= Open <= High` and `Low <= Close <= High`, and requires volume
  to be a nonnegative whole number. Decimal checks prevent float rounding from
  hiding inconsistent prices or fractional volume.
- Validates `Dividends` and `Stock Splits` as finite numbers when those columns
  are present, without interpreting their units or applying adjustments.
- Retains the first valid record for a trading date and logs later duplicates
  as rejected. The scanned source prefix is checked too, so duplicates across
  windows are detected. Previously consumed rejections are not logged again.

Numeric values remain stripped **strings** to retain their exact source precision;
the `float()` conversion is for validation. Dates are readable strings and extra
source columns are preserved. There is no imputation, price rounding, or source
file rewriting. The clean dataset window is the task's `return_value` XCom.

Each rejected row is logged with its source data-row number (excluding the
header) and rejection reason. Missing or duplicate CSV headers, missing required
columns, unrecoverable CSV syntax, file access errors, and unexpected cleaning
errors fail the task without checkpointing that window. The reader does not try
to guess record boundaries after broken quoting. Standalone
`clean_stock_window(rows)` supports cleaning an in-memory list or iterator;
`read_clean_stock_window()` also checks duplicates against prior source windows.

Calling the original `read_stock_window()` without a row transform retains the
strict Phase 1 behavior: invalid records raise an error instead of being skipped.

## Phase 3: window analysis

`analyze_stock_window(rows)` in `dags/mastercard_pipeline/analysis.py` receives
the cleaned records and returns two full source rows:

- `highest`: the row whose `High` value is greatest.
- `lowest`: the row whose `Low` value is smallest.

Prices are compared numerically with `Decimal`, using `max()` and `min()`.
The returned records retain all source columns, readable dates, and original
numeric strings, so source precision is preserved for later export. The input
window is not changed. Price comparisons use `High` and `Low`, independently of
`Open` and `Close`.

Records are explicitly ordered by trading date for comparison. If several rows
share an extreme price, the earliest trading date wins. For a one-row window,
both results contain that row as independent copies. For an empty window (EOF or
all source records rejected by cleaning), the result is
`{"highest": null, "lowest": null}`. Invalid analysis inputs, including missing
fields, non-finite/nonpositive prices, or dates that have not been normalized,
fail explicitly instead of silently affecting the result.

The DAG runs `ingest_stock_data` followed by `analyze_stock_data`. The second task
reads the first task's XCom; it does not reopen the CSV or update the offset.
Its `return_value` XCom contains the analysis result consumed by `save_to_csv`.
To recompute an existing window, rerun `analyze_stock_data` and `save_to_csv`,
which reuse that run's cleaned XCom and leave ingestion progress unchanged.

## Phase 4: CSV result export

`export_stock_results(results, rows, output_dir=None)` in
`dags/mastercard_pipeline/export.py` receives the analysis result and its clean
window. The downstream `save_to_csv` task writes a UTF-8 CSV with this header:

```csv
type,date,open,high_or_low,close,volume
```

There are exactly two data rows, in this order:

- `HIGHEST`: the selected row's `Date`, `Open`, `High`, `Close`, and `Volume`.
- `LOWEST`: the selected row's `Date`, `Open`, `Low`, `Close`, and `Volume`.

These are summary columns: `Date`, `Open`, `Close`, and `Volume` become lowercase
names; `high_or_low` holds the relevant extreme price; `type` labels that extreme.
`Dividends`, `Stock Splits`, other source fields, and the opposite price are omitted
from this summary, while remaining available in the clean and analytical XComs.
Numeric strings retain source precision without rounding.

The filename uses the earliest and latest accepted dates across the **entire
clean window**, sorted explicitly, even when neither extreme falls on a boundary.
For example, a window covering August 14-18, 2006 produces
`data/output/2006-08-14_to_2006-08-18.csv`. Rejected records do not determine the
date range. A one-row window uses the same date at both ends and still exports
both labeled rows. An empty clean window creates no file and returns `None`.

The output directory is created if needed. Set `MASTERCARD_OUTPUT_DIR` in the
worker environment or pass `output_dir` explicitly to change it. The default
outside Docker is this checkout's `data/output/`. In Docker, `/opt/airflow/output`
maps to that host directory. The raw dataset remains unchanged and all output
stays ignored by Git.

Writing uses `csv.DictWriter` and a temporary file in the destination directory,
then `os.replace` publishes the completed file. Re-exporting the same range
replaces the file rather than appending records. Write errors fail the task,
remove its temporary file, and preserve an existing completed CSV. The task's
`return_value` XCom is the absolute output path (inside the container for Docker).
To retry an export, clear **only `save_to_csv`** for that run; its existing upstream
XComs supply the same window without advancing the offset. Ingestion still
checkpoints before downstream work; see the recovery limitation below.

## Windows virtual environment

A Python 3.12 virtual environment is available in `.venv/`, which is ignored by
Git. From the repository root, activate it in PowerShell:

```powershell
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python -m unittest discover -s tests -v
```

You can also use its interpreter directly without activation:

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

The test suite uses `unittest` and requires the pandas dependency in
`requirements.txt`. The pinned version matches the existing Airflow Docker image.
This environment supports local development and the ingestion, cleaning, and
analysis and export tests.
For running the Airflow DAG, use Docker Compose below or the Linux/WSL2 setup
in a separate Linux checkout. Keep its Linux virtual environment separate from
this Windows `.venv`.

## Run Airflow with Docker (Windows)

The root `compose.yaml` runs Airflow **2.11.0 with Python 3.12** in a Linux
container using `airflow standalone`. This is a local development setup with
SQLite and sequential task execution. See the
[Airflow standalone guide](https://airflow.apache.org/docs/apache-airflow/2.11.0/start.html)
for how this command initializes the database, creates an admin user, and starts
the scheduler and webserver.

Start Docker Desktop in **Linux containers** mode first. Check `wsl --version`.
If Windows reports that WSL is not installed, open a separate PowerShell window
**as Administrator** and install WSL without adding an Ubuntu distribution:

```powershell
wsl --install --no-distribution
```

Restart Windows if prompted, then start Docker Desktop again. Follow the
[Docker Desktop Windows prerequisites](https://docs.docker.com/desktop/setup/install/windows-install/#wsl-verification-and-setup)
if installation fails or Docker still reports that virtualization is unavailable.
That error alone does not establish that a BIOS setting is disabled. Verify that
`docker info` can reach the Linux engine before continuing.

From the repository root in PowerShell, with the Kaggle CSV already in `data/`:

```powershell
New-Item -ItemType Directory -Force data/output | Out-Null
docker compose config --quiet
docker compose up --detach --wait --wait-timeout 180
docker compose ps
```

The first start downloads the Airflow image. The health check waits for both the
metadata database and scheduler to be healthy. Open **http://localhost:8080**.
Find the generated admin login in the startup logs:

```powershell
docker compose logs airflow
```

The web UI is bound to localhost. If port 8080 is already occupied, set
`$env:AIRFLOW_PORT = "8081"` before starting and use http://localhost:8081.

The DAGs and source dataset are mounted read-only. A separate writable bind
mount exposes `data/output/` at `/opt/airflow/output` for exported files.
The `airflow-home` named volume
persists the metadata database, Variables (including the ingestion offset),
task history, logs, and generated login information across container restarts.
No Airflow installation is required inside the Windows `.venv`.

Check that the DAG loads and trigger a processing window:

```powershell
docker compose exec airflow airflow dags list-import-errors
docker compose exec airflow airflow dags list
docker compose exec airflow airflow dags unpause mastercard_stock_ingestion
docker compose exec airflow airflow dags trigger mastercard_stock_ingestion
```

Wait for all three tasks to succeed in the UI:
`ingest_stock_data` → `analyze_stock_data` → `save_to_csv`.
The `ingest_stock_data` task's
`return_value` XCom contains the clean batch; the `analyze_stock_data` task's
`return_value` XCom contains the `highest` and `lowest` records. The `save_to_csv`
task returns the output path; inspect its CSV under `data/output/` on the host.
Check progress, then trigger again to process and export the next window:

```powershell
docker compose exec airflow airflow variables get mastercard_stock_offset
docker compose exec airflow airflow dags trigger mastercard_stock_ingestion
```

With a fresh metadata volume and at least 20 valid source rows, the first two
successful runs advance the offset to 10 and 20 respectively. To intentionally
restart ingestion, ensure no DAG run is active and run:

```powershell
docker compose exec airflow airflow variables set mastercard_stock_offset 0
```

Stop the project while retaining its volume and checkpoint:

```powershell
docker compose down
```

Deleting the named volume also deletes its offset, task history, and login.

### Unhealthy container after a restart

If the logs report `Already running on PID ...` or a stale
`/opt/airflow/airflow-webserver.pid`, the webserver can fail to start even though
the scheduler and triggerer are running. An abrupt stop can leave this runtime
PID file in the persistent Airflow home directory; after restart its process ID
may belong to a different component.

The Compose startup command removes this PID file before launching Airflow.
After pulling this fix, recreate the service so the updated command takes effect:

```powershell
docker compose up --detach --wait --wait-timeout 180
```

This reuses the existing `airflow-home` volume, preserving the ingestion offset,
task history, and login. Check `docker compose ps` and http://localhost:8080/health
to confirm that the webserver, metadata database, and scheduler are available.

## Run with Airflow

The supplied DAG uses the requested `from airflow.models import Variable` API
and targets **Airflow 2.x**. Its API is documented in the
[Airflow 2.11 Variables guide](https://airflow.apache.org/docs/apache-airflow/2.11.0/core-concepts/variables.html).
Use Python 3.10-3.12 with Airflow 2.11. On Windows, run Airflow in WSL2 or a Linux
container, as described in the
[Airflow prerequisites](https://airflow.apache.org/docs/apache-airflow/2.11.0/installation/prerequisites.html).

For a new local development environment, these commands assume Linux/WSL2 and
Python 3.12, run from the repository root:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install "apache-airflow==2.11.0" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.11.0/constraints-3.12.txt"
pip install "apache-airflow==2.11.0" -r requirements.txt
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"
export MASTERCARD_DATASET_PATH="$PWD/data/Mastercard_stock.csv"
airflow standalone
```

For an existing Airflow installation, deploy both `dags/mastercard_ingestion.py`
and the `dags/mastercard_pipeline/` package into its DAGs folder. Make the same
CSV available to every worker and set `MASTERCARD_DATASET_PATH` to its absolute
path in the worker environment. Without that environment variable, the default
path is `data/Mastercard_stock.csv` relative to this checkout, regardless of the
process working directory. Set `MASTERCARD_OUTPUT_DIR` to a writable, persistent
output directory accessible to the export worker. For multiple workers, use a
shared output location so exported files can be retrieved consistently.

In the Airflow UI, unpause `mastercard_stock_ingestion` and trigger it manually.
The DAG has no automatic schedule. Inspect `ingest_stock_data`'s `return_value`
XCom for the clean batch, and `analyze_stock_data`'s `return_value` for its price
extrema. `save_to_csv` exports the CSV and returns its path in `return_value`.
The `mastercard_stock_offset` Variable tracks source-row progress.
Trigger again after completion to read the next batch. Alternatively, from a
shell configured for the same Airflow instance:

```bash
airflow dags unpause mastercard_stock_ingestion
airflow dags trigger mastercard_stock_ingestion
```

Change `window_size` in the DAG's `op_kwargs` to choose another fixed positive
integer window. The default is `DEFAULT_WINDOW_SIZE = 10` in the reader module.
To intentionally restart from the beginning, stop active runs and reset the
offset in Airflow's Variables UI or CLI:

```bash
airflow variables set mastercard_stock_offset 0
```

Keep the offset in Airflow's writable metadata store; do not override it with
an `AIRFLOW_VAR_MASTERCARD_STOCK_OFFSET` environment variable or a read-only
secrets backend, which would mask updates.

The DAG sets `max_active_runs=1` because reading and updating a Variable is not
an atomic claim. Use a separate offset key for another dataset and serialize all
callers sharing a key. This stage checkpoints a successfully examined and cleaned
source window, before Airflow persists its clean-window XCom, analyzes, or exports.
A crash after saving the offset can lose that
batch's handoff; this is not an exactly-once delivery mechanism. Automatic task
retries are disabled. If analysis or export fails after its upstream XComs were
saved, rerun only the failed downstream tasks. Clearing ingestion consumes a new
window. A crash before the clean XCom is stored still requires manual recovery;
file replacement alone does not make the whole pipeline exactly-once.

## Verify locally

The ingestion, cleaning, analysis, and export tests require pandas, but do not require Airflow or
the Kaggle download. From the repository root in an activated environment:

```bash
python -m pip install -r requirements.txt
python -m unittest discover -s tests -v
```

Tests use synthetic CSV files and an in-memory replacement for Airflow Variables
to check successive windows, whitespace trimming, readable local dates, invalid
numbers/prices, rejection reporting, duplicate dates, source precision, and
checkpoint behavior for partial, empty, and all-rejected windows. Unexpected
errors are checked for unchanged offsets. Analysis tests cover numeric extrema,
exact decimal comparisons, unsorted inputs and ties, empty/single-row windows,
source-field preservation, and independent results for successive windows.
Export tests cover the exact CSV schema, full-window date ranges, decimal-string
preservation, repeated exports, write failures, output configuration, and the
full ingestion-to-export flow through rejected, partial, and empty windows.
A live DAG run additionally requires a configured Airflow instance and its
metadata store.
