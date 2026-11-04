# Mastercard stock automated data pipeline

This stage reads a fixed window of daily Mastercard stock records from CSV and
stores the next row offset in an Airflow Variable. It uses Python's `csv` and
`os` modules; no pandas or external CSV library is needed.

## Dataset

Download [MasterCard Stock Data - Latest and Updated from Kaggle](https://www.kaggle.com/datasets/kalilurrahman/mastercard-stock-data-latest-and-updated)
and place the CSV at `data/Mastercard_stock.csv`. The root `data/` directory is
ignored by Git. Keep the source file unchanged while consuming its windows;
replacing or reordering records invalidates a positional offset.

The required columns are `Date`, `Open`, `High`, `Low`, `Close`, and `Volume`.
Additional columns, including `Dividends` and `Stock Splits`, are preserved.

## How ingestion works

The reader is in `dags/mastercard_pipeline/ingestion.py`.

1. Check that the dataset file exists using `os.path.isfile`.
2. Read `mastercard_stock_offset` using `Variable.get`, defaulting to `0`.
3. Open the file with `csv.DictReader` and skip the already consumed records.
4. Collect the next `window_size` records (default: **10**).
5. After the complete window passes validation, save `offset + len(rows)` using
   `Variable.set` and return the rows as a list of dictionaries.

The header does not count toward the offset. With a window of 10, successive
runs return records 1-10, 11-20, and 21-30. The last window can contain fewer
than 10 records. At end of file the reader returns `[]` and leaves the offset
unchanged, so it does not restart automatically.

Source values remain strings, including original precision and timezone offsets.
The reader checks headers, row structure, dates, missing values, positive prices,
price ranges, nonnegative integral volume, and duplicate trading dates. Scanned
prefix rows are also validated to catch duplicates across windows. Invalid input
raises an error without saving a new offset. No rows are silently dropped or
filled. Each run scans the file from the beginning, without loading the entire
dataset into memory.

## Windows virtual environment

A Python 3.12 virtual environment is available in `.venv/`, which is ignored by
Git. From the repository root, activate it in PowerShell:

```powershell
.\.venv\Scripts\Activate.ps1
python -m unittest discover -s tests -v
```

You can also use its interpreter directly without activation:

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

This environment supports local development and the standard-library tests.
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
callers sharing a key. This stage checkpoints a successfully read batch, before
any future downstream processing. A crash after saving the offset can lose that
batch's handoff; this is not an exactly-once delivery mechanism. Automatic task
retries are disabled. When adding transformations or loading, design their
checkpoint/commit boundary before relying on retries or clearing completed tasks.

## Verify locally

The ingestion tests require only Python's standard library, without Airflow or
the Kaggle download. From the repository root:

```bash
python -m unittest discover -s tests -v
```

Tests use synthetic CSV files and an in-memory replacement for Airflow Variables
to check successive windows, partial and exhausted files, validation failures,
timezone preservation, and unchanged offsets after input failures. A live DAG
run additionally requires a configured Airflow instance and its metadata store.
