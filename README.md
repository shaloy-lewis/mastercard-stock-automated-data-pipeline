# Mastercard stock automated data pipeline

The pipeline reads a fixed window of daily Mastercard stock records from CSV,
cleans and validates each record, and stores the next source-row offset in an
Airflow Variable. Ingestion uses Python's `csv` and `os` modules. Phase 2 uses
`pandas.to_datetime()` for dates, `float()` for numeric validation, and `.strip()`
to remove surrounding whitespace.

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
ingestion and cleaning within the same task.

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
This environment supports local development and the ingestion/cleaning tests.
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

The DAGs and dataset are mounted read-only. The `airflow-home` named volume
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

Wait for `ingest_stock_data` to succeed in the UI. Its `return_value` XCom contains
the batch. Check progress, then trigger again to read the next window:

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
process working directory.

In the Airflow UI, unpause `mastercard_stock_ingestion` and trigger it manually.
The DAG has no automatic schedule. Inspect the task's `return_value` XCom to see
the small batch and the `mastercard_stock_offset` Variable to see its progress.
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
source window, before Airflow persists its XCom result or any future downstream
processing. A crash after saving the offset can lose that
batch's handoff; this is not an exactly-once delivery mechanism. Automatic task
retries are disabled. When adding transformations or loading, design their
checkpoint/commit boundary before relying on retries or clearing completed tasks.

## Verify locally

The ingestion and cleaning tests require pandas, but do not require Airflow or
the Kaggle download. From the repository root in an activated environment:

```bash
python -m pip install -r requirements.txt
python -m unittest discover -s tests -v
```

Tests use synthetic CSV files and an in-memory replacement for Airflow Variables
to check successive windows, whitespace trimming, readable local dates, invalid
numbers/prices, rejection reporting, duplicate dates, source precision, and
checkpoint behavior for partial, empty, and all-rejected windows. Unexpected
errors are checked for unchanged offsets. A live DAG run additionally requires a
configured Airflow instance and its metadata store.
