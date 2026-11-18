# Senior Data Engineer Take-Home Assignment: Assignment 1
# Idempotent Ingestion & Safe Replay

A production-grade, incremental ingestion pipeline for bank transaction extracts built with **PySpark and Delta Lake** on Databricks.

The pipeline handles real-world messy feeds: malformed rows, duplicate transactions, accidental full-file re-deliveries, and late-arriving backfills with strict idempotency and complete audit reconciliation.

---

## Assignment Chosen & Executive Summary

- **Assignment Selected:** Assignment 1 — *Idempotent Ingestion & Safe Replay*
- **Technology Stack:** PySpark + Delta Lake on Databricks Runtime (DBR) + Unity Catalog (Silver layer).
- **Core Outcome:** Successfully ingested 3 messy bank feeds (555 total rows read) into **361 canonical records**, routed **6 corrupt rows** to quarantine with specific reason codes, rejected **188 duplicate rows**, merged **3 late-arriving backfill transactions** (net +\$78,500.00 cash impact), and proved idempotency via automated replay assertion with **0 discrepancy**.

---

## Architecture & Technology Choice: PySpark + Delta Lake

**PySpark and Delta Lake** were chosen as the sole technology stack:
- **ACID Transactions & Idempotency:** Delta Lake's native `MERGE INTO` (`whenNotMatchedInsertAll`) guarantees atomic, idempotent writes. Re-running a batch or re-feeding an already-processed file produces zero duplicate rows.
- **Incremental Volume Ingestion:** Files dropped into a **Databricks Volume** are ingested incrementally in arrival sequence.
- **Schema Enforcement & Partitioning:** The canonical table is partitioned by transaction date (`PARTITIONED BY (txn_date)`), allowing late backfill records to merge directly into past partitions without scanning or mutating unaffected historical data.
- **Unity Catalog Governance:** All outputs are stored as governed Unity Catalog Delta tables (`canonical_transactions`, `quarantine`, and `reconciliation_summary`).

---

## How to Deploy and Run in Databricks

### 1. Upload Source Files to Databricks Volume
Upload the daily extract CSV files from `raw_files/` into your Databricks Volume:
```
/Volumes/fci_assignment/raw/raw_files/
```
*(Catalog, Schema, and Volume names are configurable via widgets at the top of the notebook).*

### 2. Run the Notebook or Deploy via Databricks Asset Bundle (DAB)
- **Direct Notebook Execution:** Open [`src/ingestion_notebook.py`](src/ingestion_notebook.py) in your Databricks workspace and click **Run All**.
- **Databricks CLI / DAB Execution:**
  ```bash
  databricks bundle deploy --target dev
  databricks bundle run transaction_ingestion_job
  ```

---

## Target Delta Tables in Unity Catalog & Output CSVs

All results are inspectable both as live **Unity Catalog Delta Tables** and as exported CSV files in [`output/`](output/):

| Asset / Table | Unity Catalog Path | Local CSV Export | Description |
| :--- | :--- | :--- | :--- |
| **`canonical_transactions`** | `fci_assignment.silver.canonical_transactions` | [`output/canonical_transactions.csv`](output/canonical_transactions.csv) (361 rows) | Clean, deduplicated financial transactions (`PARTITIONED BY (txn_date)`). |
| **`quarantine`** | `fci_assignment.silver.quarantine` | [`output/quarantine.csv`](output/quarantine.csv) (6 rows) | Raw malformed records routed with `reason_code` and `error_message`. |
| **`reconciliation_summary`** | `fci_assignment.silver.reconciliation_summary` | [`output/reconciliation_summary.csv`](output/reconciliation_summary.csv) (3 rows) | Audit log tracking rows read, loaded, quarantined, and backfill movement. |

---

## Key Design Decisions & Justifications

### 1. Deduplication Key: `(account_number, ref_no)`
- **Domain Invariant:** In banking ledgers and payment switches (NEFT, UPI, SWIFT), each financial movement within an account is assigned a unique reference number (`ref_no`). Combining `account_number` with `ref_no` guarantees uniqueness across accounts.
- **Preserving Legitimate Repeat Transactions:** Deduplicating on transaction attributes (e.g. `account + date + time + amount + description`) would drop legitimate repeat transactions (such as a customer buying two \$5.50 coffees at 08:30:00 or two consecutive monthly subscription charges). Because our key is `(account_number, ref_no)`, legitimate repeat transactions receive distinct reference IDs from the core host and are **never** discarded.
- **Collapsing Accidental Re-deliveries:** In `batch_2026-08-02.csv`, Day 1 was accidentally re-delivered with formatting differences (`1/08/2026` vs `01-08-2026`, unpadded hours `4:29:00` vs `04:29:00`). Because date/time parsing standardizes values to ISO formats and deduplication matches on `(account_number, ref_no)`, the pipeline recognizes that all 180 valid re-delivered records are already loaded and collapses them without duplicating Day 1 history.

### 2. Malformed Row Detection & Quarantine Routing
Financial pipelines must **never silently drop** or **silently crash** on corrupted records. Records are read with `mode = PERMISSIVE` to prevent read-time failure. Each row is checked against strict banking invariants, routing failures to the `quarantine` Delta table:
- `MISSING_REF_NO`: `ref_no` is blank/null. A transaction cannot be reconciled without a reference number (2 rows quarantined).
- `BOTH_DEBIT_AND_CREDIT`: Single-entry violation where both debit and credit are populated (2 rows quarantined: `SUSPENSE ENTRY` with debit 1200 and credit 1200).
- `NEITHER_DEBIT_NOR_CREDIT`: Zero-movement entry where neither debit nor credit is populated (2 rows quarantined: empty POS purchase).
- `INVALID_TXN_DATE`: Unparseable date format.
- `INVALID_DEBIT_AMOUNT` / `INVALID_CREDIT_AMOUNT` / `INVALID_BALANCE_AMOUNT`: Amount contains non-numeric corrupted characters.

### 3. Idempotency & Safe Replay
- The pipeline uses Delta Lake `MERGE INTO ... WHEN NOT MATCHED THEN INSERT`.
- If an already-ingested file is re-fed, or if the pipeline is re-run, Delta MERGE finds matching keys and performs no inserts (`new_rows_loaded = 0`).
- The canonical table state remains completely identical at **361 records**.
- The notebook includes an automated replay assertion cell verifying that re-feeding the files results in exactly `0` new rows.

### 4. Backfill Handling (Late Correction File)
- `batch_2026-08-04_late_correction.csv` contains 3 transactions with `txn_date = "01-08-2026"` that missed the initial cutoff.
- The pipeline parses their date to `2026-08-01` and merges them into Day 1’s historical partition (`txn_date = 2026-08-01`).
- Day 1 transaction count increases from 180 to 183 without duplicating or mutating previously loaded Day 1 records.
- **Net Financial Impact of Backfill:**
  - Gross Inflow (Credit): **+\$97,000.00** (`REF900001` \$15,000 + `REF900002` \$82,000)
  - Gross Outflow (Debit): **-\$18,500.00** (`REF900003` \$18,500)
  - **Net Cash Movement:** **`+$78,500.00`**

---

## Reconciliation Summary

Across all three extract files, every row is accounted for with zero discrepancy:

```
================================================================================
                    RECONCILIATION AUDIT SUMMARY
================================================================================
Total Rows Read:                       555
  - batch_2026-08-01.csv:              187
  - batch_2026-08-02.csv:              365
  - batch_2026-08-04_late_correction:    3
--------------------------------------------------------------------------------
Valid Candidates Evaluated:            549
Duplicates Removed:                    188
  - Intra-file duplicates (Day 1):       4
  - Replay duplicates (Day 2 batch):   184 (4 intra-day + 180 re-delivered)
New Rows Loaded to Canonical Store:    361
  - Day 1 Initial Valid:               180
  - Day 2 Valid:                       178
  - Day 1 Backfill Valid:                3
Total Quarantined Rows:                  6
  - BOTH_DEBIT_AND_CREDIT:               2
  - MISSING_REF_NO:                      2
  - NEITHER_DEBIT_NOR_CREDIT:            2
--------------------------------------------------------------------------------
Row Conservation Equation:
  Total Rows Read (555)
  = Canonical Records (361) + Duplicates (188) + Quarantined (6)
  = 555 (Discrepancy: 0)
================================================================================
```

---

## Assumptions Made

1. **Bank Transaction Invariant:** Single-entry bookkeeping per record (either debit or credit, never both or neither).
2. **Chronological File Processing:** Files in the Volume are processed in chronological/lexicographical order by filename (`batch_YYYY-MM-DD.csv`).
3. **Currency & Precision:** All monetary amounts are in standard financial format and cast to `DECIMAL(18,2)` to prevent floating-point rounding errors.
4. **Correction Identification:** Files containing `correction` or `backfill` in their filename represent backfills into historical dates.

---

## Stretch Goal: Parameterized Configuration

The pipeline isolates configuration from implementation in [`config/pipeline_config.yaml`](config/pipeline_config.yaml):
- **Dedup Keys:** Configured as `["account_number", "ref_no"]` (can be swapped to `["transaction_id"]` for another bank).
- **Date/Time Formats:** Array of accepted date/time patterns.
- **Expected Columns & Table Names:** All catalog, schema, and Delta table names are parameterized.
- Onboarding a new source bank feed requires a configuration file change without code forks.

---

## What I Would Do Differently or Add with More Time

1. **Automated CI/CD Test Suite:** Add a GitHub Actions pipeline running `pytest` with a localized Spark session testing validation and dedup corner cases.
2. **Dead-Letter Queue (DLQ) Remediation Workflow:** Build an operational dashboard (Databricks Lakeview / Genie) allowing data stewards to view quarantined records, correct typos (e.g. missing ref_no), and replay them into the canonical pipeline.
3. **Alerting & Thresholds:** Integrate alerts (Slack/PagerDuty) triggered when the quarantine rate exceeds a threshold (e.g. >1% of a daily batch).
4. **Lakeflow / Auto Loader Streaming:** If feed frequency increases from daily batch drops to hourly/near real-time, transition the file reader from batch `spark.read` to Databricks Auto Loader (`spark.readStream.format("cloudFiles")`).

---

## Project Structure

```
FCI_Assigment/
├── config/
│   └── pipeline_config.yaml               # YAML configuration (tables, volume paths, CSV schema, dedup keys)
├── raw_files/                             # The 3 source CSV files & assignment notes
│   ├── batch_2026-08-01.csv
│   ├── batch_2026-08-02.csv
│   ├── batch_2026-08-04_late_correction.csv
│   └── NOTES.txt
├── src/
│   ├── pipeline_utils.py                  # Reusable PySpark logic (validation, quarantine, Delta MERGE, audit)
│   └── ingestion_notebook.py              # Clean daily Databricks notebook (triggered daily by job)
├── resources/
│   └── transaction_ingestion_job.job.yml  # Databricks Asset Bundle job definition
├── databricks.yml                         # DAB bundle configuration
├── output/                                # Exported CSV deliverables
│   ├── canonical_transactions.csv         # 361 deduplicated canonical records
│   ├── quarantine.csv                     # 6 quarantined records with reason codes
│   └── reconciliation_summary.csv         # 3 audit records with backfill metrics
└── README.md                              # Complete assignment documentation
```
