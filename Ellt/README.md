# Food Orders Analysis and Batch ELT

This project analyzes the provided food-order CSV and implements an idempotent,
100-row batch ELT pipeline. The original source is kept unchanged.

## Project files

- `food_orders.csv` — project copy of the supplied source CSV.
- `food_orders_analysis.ipynb` — eight assignment tasks with executed outputs.
- `food_orders_cleaned.csv` — accepted, typed, transformed records.
- `extract.py`, `load_raw.py`, `retrieve_raw.py` — CSV extraction and raw MySQL stage.
- `transform.py`, `validate.py` — documented cleaning, rejection and validation.
- `load_transformed.py` — validated analytics database load.
- `elt_batch_pipeline.py` — end-to-end batch workflow and execution report.
- `sql/business_analysis.sql` — SQL answers to the business questions.
- `reports/` — data quality, validation, rejection and execution reports.
- `config.py` — paths, batch size and environment-based MySQL configuration.

## Windows setup

From this directory in PowerShell:

```powershell
py -3.14 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

If PowerShell activation is restricted by local policy, do not change security
settings; call the environment executable directly:

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

VS Code is configured to use `.venv\Scripts\python.exe`. Select the same
interpreter for the notebook kernel if VS Code prompts.

## MySQL configuration and execution

Install/start MySQL Server 8, then create a local `.env` file by copying
`.env.example`. Set `DB_HOST`, `DB_PORT`, `DB_USER`, and `DB_PASSWORD` to a
MySQL account with permission to create databases and tables. Do not commit the
`.env` file or put credentials in source code.

Run the end-to-end workflow:

```powershell
.\.venv\Scripts\python.exe elt_batch_pipeline.py
```

The pipeline creates `food_orders_raw_db.raw_food_orders` and
`food_orders_analytics_db.cleaned_food_orders`. Raw fields are stored as text
to preserve CSV values and missing fields. Loads use InnoDB transactions in
batches of 100. A source-content hash plus pipeline version identifies a load
run; `(load_run_id, source_row_number)` is unique. Re-running the same source
is idempotent and appends no duplicate rows; previous runs remain untouched.
If transformation rules change, increment `PIPELINE_VERSION` in `config.py` to
create a distinct analytics run.

Retrieved batches are combined before duplicate detection, so an exact
duplicate split across two batches is still detected. Repeated Order IDs with
different source values are retained and reported rather than automatically
discarded.

Database credentials are checked at runtime. If connection or permissions are
unavailable, the local analysis, cleaned CSV and reports are still produced,
and `execution_report.json` records that database stages did not complete.
It never labels an unsuccessful database operation as successful.

## Notebook and SQL analysis

Open `food_orders_analysis.ipynb` and select the project `.venv` kernel. Run
cells from top to bottom. The notebook produces actual data summaries, charts,
validation checks and the cleaned CSV.

After a successful ELT run, execute `sql/business_analysis.sql` in a MySQL
client. Order value totals/averages use non-NULL values. Platform net revenue
is calculated only when commission, delivery fee, payment-processing fee and
refund/chargeback values are all present; SQL `SUM` excludes rows with unknown
revenue.

## Reports

- `reports/data_quality_report.csv` — missingness, duplicates, timestamp and
  negative-financial-value checks.
- `reports/validation_report.csv` — actual validation results and explanations.
- `reports/rejected_records.csv` — excluded records with rejection reasons.
- `reports/execution_report.json` — extracted/accepted/rejected counts,
  database stage results and batch totals.

Exact full-row duplicates are excluded after inspection. Repeated Order IDs
are not removed merely because the identifier repeats; only exact duplicate
rows are removed. Missing refunds, order values, fees, payment methods and
offers are not imputed. Delivery durations that cannot be computed or are
negative are rejected; there is no invented maximum delivery-time threshold.
