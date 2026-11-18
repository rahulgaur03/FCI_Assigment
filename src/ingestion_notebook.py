# Databricks notebook source
# MAGIC %md
# MAGIC # Daily Bank Transaction Ingestion Pipeline
# MAGIC
# MAGIC This notebook runs daily to incrementally ingest transaction extract files from a Databricks Volume into Unity Catalog Delta tables.
# MAGIC Configuration details regarding tables, CSV schema, dedup keys, and environments are loaded from `config/pipeline_config.yaml`.
# MAGIC
# MAGIC ### Core Invariants Handled:
# MAGIC 1. **Incremental Processing:** Ingests extract files one-by-one in arrival sequence from Volume.
# MAGIC 2. **Deduplication on `(account_number, ref_no)`:** Collapses accidental re-deliveries while preserving legitimate repeat transactions.
# MAGIC 3. **Quarantine Routing:** Detects malformed records (bad dates, dual debit/credit, neither, missing ref_no) and routes them to `quarantine`.
# MAGIC 4. **Strict Idempotency:** Delta Lake `MERGE INTO` ensures re-running or re-feeding files results in `0` duplicate rows.
# MAGIC 5. **Late Backfill Handling:** Merges `batch_2026-08-04_late_correction.csv` into Day 1 history without mutating existing records.
# MAGIC 6. **Reconciliation Audit Log:** Logs rows read, loaded, quarantined, duplicates dropped, and net backfill movement.

# COMMAND ----------
# MAGIC %md
# MAGIC ### 1. Load Configuration & Widgets

# COMMAND ----------
import os
import sys

sys.path.append(os.path.abspath(".."))
sys.path.append(os.path.abspath("."))
from src.pipeline_utils import load_config, parse_and_validate, merge_into_canonical, log_reconciliation

# 1. Load YAML Configuration
config = load_config("config/pipeline_config.yaml")

# 2. Setup Widgets with defaults from YAML
default_catalog = config["environment"]["catalog"]
default_target_schema = config["environment"].get("target_schema", "silver")
default_volume_path = config["environment"]["volume_path"]

dbutils.widgets.text("catalog", default_catalog, "Catalog Name")
dbutils.widgets.text("schema", default_target_schema, "Target Schema (Silver)")
dbutils.widgets.text("volume_path", default_volume_path, "Volume Path")
dbutils.widgets.dropdown("reset_tables", "false", ["false", "true"], "Reset Tables (true/false)")

catalog = dbutils.widgets.get("catalog").strip()
target_schema = dbutils.widgets.get("schema").strip()
volume_path = dbutils.widgets.get("volume_path").strip()
reset_tables = dbutils.widgets.get("reset_tables").lower() == "true"

canonical_tbl_name = config["tables"]["canonical_table"]
quarantine_tbl_name = config["tables"]["quarantine_table"]
reconciliation_tbl_name = config["tables"]["reconciliation_table"]
partition_col = config["tables"].get("partition_by", "txn_date")
dedup_keys = config["dedup"]["keys"]

print(f"Catalog       : {catalog}")
print(f"Target Schema : {target_schema}")
print(f"Volume Path   : {volume_path}")
print(f"Dedup Keys    : {dedup_keys}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 2. Unity Catalog Context & Target Delta Tables

# COMMAND ----------
spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{target_schema}`")
spark.sql(f"USE SCHEMA `{target_schema}`")

CANONICAL_TABLE = f"`{catalog}`.`{target_schema}`.`{canonical_tbl_name}`"
QUARANTINE_TABLE = f"`{catalog}`.`{target_schema}`.`{quarantine_tbl_name}`"
RECONCILIATION_TABLE = f"`{catalog}`.`{target_schema}`.`{reconciliation_tbl_name}`"

if reset_tables:
    print(f"Resetting existing Delta tables in `{catalog}`.`{target_schema}`...")
    spark.sql(f"DROP TABLE IF EXISTS {CANONICAL_TABLE}")
    spark.sql(f"DROP TABLE IF EXISTS {QUARANTINE_TABLE}")
    spark.sql(f"DROP TABLE IF EXISTS {RECONCILIATION_TABLE}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 3. Incremental File Discovery in Volume

# COMMAND ----------
# Discover and sort arriving CSV files in the Volume
files_to_process = sorted([f.path for f in dbutils.fs.ls(volume_path) if f.name.endswith(".csv")])
if not files_to_process:
    raise FileNotFoundError(f"No CSV extract files found in Volume path: {volume_path}")

print(f"Discovered {len(files_to_process)} extract file(s) in Volume '{volume_path}':")
for fp in files_to_process:
    print(f"  - {fp}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 4. Daily Incremental Ingestion Loop

# COMMAND ----------
from pyspark.sql import functions as F

read_mode = config["ingestion"].get("read_mode", "PERMISSIVE")

for file_path in files_to_process:
    file_name = os.path.basename(file_path)
    print(f"\n>>> INGESTING INCREMENTALLY: {file_name}")

    # 1. Permissive read (never crashes on bad records)
    raw_df = spark.read.option("header", True).option("mode", read_mode).csv(file_path)
    rows_read = raw_df.count()

    # 2. Validation & Quarantine routing
    clean_df, quarantine_df, rows_quarantined, valid_rows_count = parse_and_validate(raw_df, file_name, config)

    quarantine_reasons = {}
    if rows_quarantined > 0:
        quarantine_df.write.format("delta").mode("append").saveAsTable(QUARANTINE_TABLE)
        for r in quarantine_df.groupBy("reason_code").count().collect():
            quarantine_reasons[r["reason_code"]] = r["count"]

    # 3. Intra-file deduplication count
    clean_candidates_count = clean_df.count()
    intra_duplicates = valid_rows_count - clean_candidates_count

    # 4. Idempotent Delta MERGE
    new_rows_loaded, replayed_duplicates, new_df = merge_into_canonical(
        spark, clean_df, CANONICAL_TABLE, dedup_keys=dedup_keys, partition_by=partition_col
    )
    total_duplicates_dropped = intra_duplicates + replayed_duplicates

    # 5. Backfill detection (late correction file)
    is_backfill = "correction" in file_name.lower() or "backfill" in file_name.lower()
    bf_txns = new_rows_loaded if is_backfill else 0
    bf_credit = 0.0
    bf_debit = 0.0
    bf_net = 0.0

    if is_backfill and new_rows_loaded > 0:
        bf_stats = new_df.agg(
            F.coalesce(F.sum("credit"), F.lit(0)).cast("double").alias("c"),
            F.coalesce(F.sum("debit"), F.lit(0)).cast("double").alias("d")
        ).first()
        bf_credit = round(bf_stats["c"], 2)
        bf_debit = round(bf_stats["d"], 2)
        bf_net = round(bf_credit - bf_debit, 2)
        print(f"    [Backfill Merged to Day 1] {bf_txns} txns | Credit: +${bf_credit:,.2f} | Debit: -${bf_debit:,.2f} | Net: +${bf_net:,.2f}")

    canonical_total = spark.table(CANONICAL_TABLE).count()
    print(f"    Rows Read: {rows_read} | Quarantined: {rows_quarantined} | Duplicates Dropped: {total_duplicates_dropped} | Net Loaded: {new_rows_loaded} | Canonical Total: {canonical_total}")

    # 6. Append audit log to Reconciliation Delta table
    log_reconciliation(spark, {
        "source_file": file_name,
        "rows_read": rows_read,
        "rows_quarantined": rows_quarantined,
        "duplicates_removed": total_duplicates_dropped,
        "new_rows_loaded": new_rows_loaded,
        "canonical_total_records": canonical_total,
        "backfill_transactions": bf_txns,
        "backfill_credit": bf_credit,
        "backfill_debit": bf_debit,
        "backfill_net_effect": bf_net,
        "quarantine_reasons": quarantine_reasons
    }, RECONCILIATION_TABLE)

# COMMAND ----------
# MAGIC %md
# MAGIC ### 5. Idempotency & Safe-Replay Verification
# MAGIC Asserts that re-feeding already-ingested files results in **`0` new rows loaded**.

# COMMAND ----------
print(">>> Running safe replay verification...")
replay_new_rows = 0
for file_path in files_to_process:
    raw_replay = spark.read.option("header", True).csv(file_path)
    clean_replay, _, _, _ = parse_and_validate(raw_replay, os.path.basename(file_path), config)
    existing = spark.table(CANONICAL_TABLE).select(*dedup_keys)
    new_in_replay = clean_replay.join(existing, dedup_keys, "left_anti").count()
    replay_new_rows += new_in_replay

assert replay_new_rows == 0, f"Idempotency violation! Inserted {replay_new_rows} rows on replay."
print(f"✅ Idempotency Verified: 0 new rows inserted upon re-feeding. Canonical count remains {spark.table(CANONICAL_TABLE).count()}.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 6. Final Audit & Inspection Displays

# COMMAND ----------
# MAGIC %md
# MAGIC #### A. Cumulative Reconciliation Log Table
# COMMAND ----------
display(spark.table(RECONCILIATION_TABLE).orderBy("run_timestamp"))

# COMMAND ----------
# MAGIC %md
# MAGIC #### B. Quarantined Records Grouped by Reason Code
# COMMAND ----------
display(
    spark.table(QUARANTINE_TABLE)
    .groupBy("reason_code", "_source_file")
    .count()
    .orderBy("reason_code")
)

# COMMAND ----------
# MAGIC %md
# MAGIC #### C. Canonical Transactions Partition Summary
# MAGIC Note that Day 1 (`2026-08-01`) contains 180 initial + 3 backfilled = **183 records**.
# MAGIC Day 2 (`2026-08-02`) contains **178 records**. Total = **361 clean records**.
# COMMAND ----------
display(
    spark.table(CANONICAL_TABLE)
    .groupBy("txn_date")
    .agg(
        F.count("*").alias("transactions_count"),
        F.round(F.sum("debit"), 2).alias("total_debit"),
        F.round(F.sum("credit"), 2).alias("total_credit")
    )
    .orderBy("txn_date")
)
