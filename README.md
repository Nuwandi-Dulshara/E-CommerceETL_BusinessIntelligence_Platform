# CommerceFlow

A local retail lakehouse and seven-page Streamlit analytics application. Real UCI transaction lines flow through **Python ingestion → Bronze Delta → PySpark validation → Silver/quarantine Delta → Gold star schema → Parquet export → DuckDB → Streamlit**. SQLite stores execution history, stage progress and the published snapshot pointer. No REST service, paid API, cloud deployment or synthetic dashboard data is used.

## Run on Windows

Use Python **3.11**, Java **17**, PySpark **3.5.3** and Delta Spark **3.2.1**. All direct Python dependencies are pinned in `requirements.txt`.

The current checkout already has `.venv` and a local `.runtime`. For a fresh checkout, run `./setup_windows.ps1` from PowerShell. It downloads free Temurin Java 17, Delta/ANTLR jars from Maven Central, and the community [Windows Hadoop compatibility binaries](https://github.com/cdarlint/winutils/tree/master/hadoop-3.3.5/bin), then installs the Python requirements. It changes no system environment variables. Internet access is required for first-time setup and UCI downloads; cached local runs are offline. Native binaries and runtime dependencies are ignored by Git.

```powershell
# Fresh setup (Python 3.11 must already be installed)
./setup_windows.ps1

# Process the supplied original Excel file
./.venv/Scripts/python.exe -m backend.pipelines.run --source "online+retail/Online Retail.xlsx" --sample-rows 50000

# Or acquire and cache the original UCI Excel file
./.venv/Scripts/python.exe -m backend.pipelines.run --download --sample-rows 50000

# Full dataset is the default: omit --sample-rows
./.venv/Scripts/python.exe -m backend.pipelines.run --source "online+retail/Online Retail.xlsx"

./.venv/Scripts/python.exe -m pytest -q
./.venv/Scripts/python.exe -m streamlit run frontend/app.py --server.address 127.0.0.1 --server.port 8501 --server.headless true --browser.gatherUsageStats false
```

Open **http://127.0.0.1:8501**. In restricted execution environments, Spark needs permission for JVM/Python localhost communication. Normal desktop PowerShell does not use the agent sandbox. Alternatively, install Java 17 and the native Hadoop dependencies yourself, create `.venv` with `python -m venv .venv`, and install with `./.venv/Scripts/python.exe -m pip install -r requirements.txt`. On Linux/macOS, use `.venv/bin/python`; native Windows compatibility files are unnecessary.

## Features

- **Overview:** six KPIs, daily/monthly revenue, products, countries, sales/returns and a labelled 100-line transaction preview.
- **Sales Analytics:** trends, orders, average order value, return ratio, product units and filtered aggregate CSVs.
- **Customer Analytics:** known/repeat customers, repeat purchase rate, customer rankings and documented frequency/value segments.
- **ETL Pipelines:** local/download source, sample/full runs, actual subprocess execution, automatic status refresh, stage history, errors and downloadable logs.
- **Lakehouse Explorer:** schemas, counts and bounded previews of every published Bronze, Silver, quarantine and Gold table.
- **Data Quality:** accepted/rejected/flagged counts, overlapping reason codes, repeated/missing-customer/zero-price flags, reconciliation checks and rejected-record CSV.
- **SQL Analytics:** five saved read-only queries with bound global filters and downloadable results.

Date boundaries are inclusive. An empty country selection means all countries. Filters apply to all business analytics; operational quality and lakehouse views describe complete run snapshots. Unknown customer IDs remain null. Screens render useful empty states before the first successful run or when filters match no records.

## Data integrity and publication

Bronze retains textual Excel values, source row number (Excel row 2 is the first data row), SHA-256 file hash, ingestion time and run ID. Row IDs derive from file hash plus source row number. Excel formatting is not retained; cell values are. Each run overwrites its own selected input snapshot; it never appends the previous run's rows.

Silver uses integer quantities, timestamps, string IDs and decimal prices/amounts with four decimal places. Invalid identifiers/country/date/quantity/price, zero quantity and negative price are quarantined with all applicable reasons. Missing descriptions remain valid. Repeated business rows are retained, with extra occurrences flagged. Missing customer, repeated-row and zero-price flags can overlap and can apply to rejected rows; they are separate from accepted/rejected totals.

Gold contains `fact_transactions`, `dim_product`, `dim_customer`, `dim_date`, `agg_daily_sales`, `agg_country_sales`, `agg_product_sales`. Products use the latest nonempty description, breaking timestamp ties by source row number. The unknown dimension member has key `UNKNOWN` and a null source customer ID. This is a warehouse member, not a fabricated customer identity.

Each run has isolated Delta and Parquet paths under `data/runs/<run_id>`. An OS file lock permits one writer, including across browser sessions and CLI processes. Parquet is imported into a staged DuckDB database. Row counts, unique keys, foreign keys and net totals must reconcile before the closed warehouse file is atomically renamed. A single SQLite transaction commits both run success and the published pointer. Failed runs preserve the previous snapshot. Read-only queries open and close connections independently; the UI never recomputes business metrics from Excel. Lakehouse previews read Parquet mirrors written from the committed layers.

## Metric definitions

- Gross revenue: non-return quantity × unit price.
- Returns: absolute line value for `C` invoices or negative quantities.
- Net revenue: gross minus returns. All amounts are **GBP (£)**.
- Orders: distinct non-return invoice numbers; AOV = gross / orders, or N/A.
- Known customers: non-null IDs on sale lines. Repeat customers have at least two distinct sale invoices in the selected filters. Repeat rate divides by known purchasing customers.
- High value repeat segment: at least three sale orders and £1,000 net revenue. Repeat: at least two orders. Otherwise: single order.
- Units sold exclude returns. Net units subtract absolute returned quantities. Return/gross ratio is N/A when gross is zero.

## Verification

The deterministic test fixture exists only in pytest temporary directories; it is never published to the dashboard. Tests cover required columns, parsing, quarantine reasons, unknown customers/products, retained repeats, returns, decimals, filtering, metric arithmetic, locks, empty states and atomic publication failures. The real-snapshot AppTest checks all pages, filters/reset, SQL execution/results/download controls and every Gold explorer selector. It skips when no snapshot exists.

Verified on 27 November 2026:

- **11 behavioral tests passed** in the final complete pytest run (29 seconds). This includes numeric underflow/overflow rejection, cross-process writer exclusion, interrupted-run recovery, failure-safe publication, and real-snapshot UI interactions.
- Two real 50,000-line runs (one launched through the UI test harness) produced identical counts and totals: **50,000 accepted**, **£955,346.88 gross**, **£114,726.38 returns**, **£840,620.50 net**.
- Two full runs processed **541,909 input lines**, **541,907 accepted** and **2 quarantined for negative prices**. All seven Gold tables matched exactly, including every fact row. Full runs took approximately **98–106 seconds** on this machine.
- Full-data decimal totals: **£10,666,684.5440 gross**, **£896,812.4900 returns**, **£9,769,872.0540 net**. The dashboard displays two decimal places. An independent source-row Decimal calculation matched warehouse totals.
- All **12 publication checks passed**. Full-data quality flags: 5,268 extra repeated rows retained, 135,080 missing customer IDs, 2,515 zero-price lines, and 140,345 distinct flagged rows.
- The UCI download completed and its SHA-256 matched the supplied workbook. Cached download mode was exercised through the UI's full-run control. Refresh did not launch a second job; Run was disabled while processing.
- Quarantine CSV contains exactly the two rejected source records. Saved query result tables/download controls were exercised with AppTest. The live Streamlit health endpoint returned **HTTP 200**. `pip check` found no broken requirements, and notebook code compiled.

## Dataset and license

Daqing Chen (2015), **Online Retail**, UCI Machine Learning Repository, [DOI: 10.24432/C5BW33](https://doi.org/10.24432/C5BW33). [Dataset and download](https://archive.ics.uci.edu/dataset/352/online-retail). The original workbook contains 541,909 transaction lines. UCI licenses the dataset under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/); preserve attribution when sharing derived outputs. Raw data, generated warehouses and logs are excluded from Git. The dataset does not supply categories, inventory, traffic, shipping charges or payment methods; this application does not invent them.

## Databricks demonstration and limitations

`notebooks/databricks_etl.ipynb` reuses the actual PySpark transformations with configurable workspace project, source Excel and writable Unity Catalog Volume paths. It demonstrates Bronze/Silver/Gold and reconciliation. It has not been executed in a Databricks account here.

Proposed migration only: local storage → ADLS Gen2; local Spark → Azure Databricks; Python orchestration → Azure Data Factory; DuckDB → cloud analytical warehouse; Streamlit → BI layer. No Azure resources or services have been deployed.

The one-hour MVP uses full snapshot batch runs, manual CLI/UI scheduling, and a local single-user dashboard. It has no authentication, automatic scheduling or historical snapshot cleanup. Run outputs are retained for inspection and consume disk space. Samples take the first N lines and are not random or representative of the whole year. Browser-level screenshot/mobile/download verification is unavailable because the session's browser automation host is disconnected; interaction testing uses Streamlit AppTest instead.
