"""
PySpark helper utilities for the daily bank transaction ingestion pipeline.
"""
import json
import os
from datetime import datetime
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
import yaml


def load_config(config_path="config/pipeline_config.yaml"):
    """Loads pipeline configuration from a YAML file."""
    if not os.path.isabs(config_path):
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        candidate = os.path.join(base_dir, config_path)
        if os.path.exists(candidate):
            config_path = candidate
        elif os.path.exists(config_path):
            pass
        else:
            # Fallback relative to current dir
            alt = os.path.join(os.getcwd(), config_path)
            if os.path.exists(alt):
                config_path = alt

    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def parse_and_validate(raw_df, file_name, config=None):
    """
    Cleans raw strings, validates business invariants, and splits records into
    clean valid candidates and quarantined rows with reason codes.
    """
    cfg = config or {}
    expected_columns = cfg.get("schema", {}).get("expected_columns", [
        "account_number", "txn_date", "txn_time", "description",
        "ref_no", "debit", "credit", "balance", "account_type"
    ])
    dedup_keys = cfg.get("dedup", {}).get("keys", ["account_number", "ref_no"])

    # Clean whitespace and convert blank strings to NULL
    cleaned = raw_df.select(
        *[
            F.when(F.trim(F.col(c)) == "", F.lit(None))
             .otherwise(F.trim(F.col(c)))
             .cast("string")
             .alias(c)
            for c in expected_columns
        ],
        F.lit(file_name).alias("_source_file"),
        F.current_timestamp().alias("_ingested_at")
    )

    # Standardize dates, times, and amounts
    parsed = (
        cleaned
        .withColumn(
            "txn_date_parsed",
            F.coalesce(
                F.expr("try_to_date(txn_date, 'd-M-yyyy')"),
                F.expr("try_to_date(txn_date, 'd/M/yyyy')"),
                F.expr("try_to_date(txn_date, 'yyyy-MM-dd')"),
                F.expr("try_to_date(txn_date, 'dd-MM-yyyy')"),
                F.expr("try_to_date(txn_date, 'dd/MM/yyyy')")
            )
        )
        .withColumn(
            "txn_time_parsed",
            F.coalesce(
                F.expr("try_to_timestamp(txn_time, 'H:mm:ss')"),
                F.expr("try_to_timestamp(txn_time, 'HH:mm:ss')")
            )
        )
        .withColumn("debit_amount", F.expr("try_cast(debit AS DECIMAL(18,2))"))
        .withColumn("credit_amount", F.expr("try_cast(credit AS DECIMAL(18,2))"))
        .withColumn("balance_amount", F.expr("try_cast(balance AS DECIMAL(18,2))"))
    )

    debit_has_val = F.col("debit").isNotNull()
    credit_has_val = F.col("credit").isNotNull()

    # Rule checks according to banking requirements
    reasons = F.filter(
        F.array(
            F.when(F.col("account_number").isNull(), F.lit("MISSING_ACCOUNT_NUMBER")),
            F.when(F.col("ref_no").isNull(), F.lit("MISSING_REF_NO")),
            F.when(F.col("txn_date_parsed").isNull(), F.lit("INVALID_TXN_DATE")),
            F.when(debit_has_val & credit_has_val, F.lit("BOTH_DEBIT_AND_CREDIT")),
            F.when(~debit_has_val & ~credit_has_val, F.lit("NEITHER_DEBIT_NOR_CREDIT")),
            F.when(debit_has_val & F.col("debit_amount").isNull(), F.lit("INVALID_DEBIT_AMOUNT")),
            F.when(credit_has_val & F.col("credit_amount").isNull(), F.lit("INVALID_CREDIT_AMOUNT")),
            F.when(F.col("balance").isNotNull() & F.col("balance_amount").isNull(), F.lit("INVALID_BALANCE_AMOUNT"))
        ),
        lambda x: x.isNotNull()
    )

    checked = parsed.withColumn("reason_codes", reasons)
    invalid_df = checked.filter(F.size("reason_codes") > 0)
    valid_df = checked.filter(F.size("reason_codes") == 0)

    # Format quarantine records
    quarantine_df = (
        invalid_df
        .withColumn("reason_code", F.explode("reason_codes"))
        .withColumn(
            "error_message",
            F.when(F.col("reason_code") == "BOTH_DEBIT_AND_CREDIT", F.concat(F.lit("Both debit ("), F.col("debit"), F.lit(") and credit ("), F.col("credit"), F.lit(") are populated")))
             .when(F.col("reason_code") == "NEITHER_DEBIT_NOR_CREDIT", F.lit("Neither debit nor credit amount is populated"))
             .when(F.col("reason_code") == "MISSING_REF_NO", F.lit("Transaction reference number is missing"))
             .when(F.col("reason_code") == "INVALID_TXN_DATE", F.concat(F.lit("Unparseable date: "), F.col("txn_date")))
             .otherwise(F.lit("Validation rule failure"))
        )
        .select(
            "account_number", "txn_date", "txn_time", "description", "ref_no",
            "debit", "credit", "balance", "account_type",
            "_source_file", "reason_code", "error_message", "_ingested_at"
        )
    )

    # Clean candidate representation with intra-batch dedup
    clean_candidates = (
        valid_df
        .withColumn("_rn", F.row_number().over(Window.partitionBy(*dedup_keys).orderBy("_ingested_at")))
        .filter(F.col("_rn") == 1)
        .withColumn("txn_date", F.col("txn_date_parsed"))
        .withColumn("txn_time", F.date_format("txn_time_parsed", "HH:mm:ss"))
        .withColumn("txn_timestamp", F.expr("try_to_timestamp(concat_ws(' ', txn_date, txn_time))"))
        .withColumn("debit", F.col("debit_amount"))
        .withColumn("credit", F.col("credit_amount"))
        .withColumn("balance", F.col("balance_amount"))
        .select(
            "account_number", "txn_date", "txn_time", "txn_timestamp",
            "description", "ref_no", "debit", "credit", "balance",
            "account_type", "_source_file", "_ingested_at"
        )
    )

    return clean_candidates, quarantine_df, invalid_df.count(), valid_df.count()


def merge_into_canonical(spark, clean_df, canonical_table, dedup_keys=None, partition_by="txn_date"):
    """
    Idempotently merges valid candidates into the canonical Delta table.
    - If table does not exist, creates it partitioned by txn_date.
    - If table exists, runs Delta MERGE (whenNotMatchedInsertAll).
    Returns (new_rows_loaded, replayed_duplicates, new_insertable_df).
    """
    keys = dedup_keys or ["account_number", "ref_no"]
    clean_count = clean_df.count()
    if clean_count == 0:
        return 0, 0, clean_df

    if spark.catalog.tableExists(canonical_table):
        existing_keys = spark.table(canonical_table).select(*keys)
        new_insertable_df = clean_df.join(existing_keys, keys, "left_anti")
        new_rows_count = new_insertable_df.count()

        merge_condition = " AND ".join([f"t.{k} = s.{k}" for k in keys])
        target_delta = DeltaTable.forName(spark, canonical_table)
        (
            target_delta.alias("t")
            .merge(
                clean_df.alias("s"),
                merge_condition
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
        replayed_duplicates = clean_count - new_rows_count
        return new_rows_count, replayed_duplicates, new_insertable_df
    else:
        (
            clean_df.write
            .format("delta")
            .mode("overwrite")
            .partitionBy(partition_by)
            .saveAsTable(canonical_table)
        )
        return clean_count, 0, clean_df


def log_reconciliation(spark, recon_dict, reconciliation_table):
    """Appends an audit record to the reconciliation Delta table."""
    row = [(
        datetime.utcnow(),
        recon_dict["source_file"],
        recon_dict["rows_read"],
        recon_dict["rows_quarantined"],
        recon_dict["duplicates_removed"],
        recon_dict["new_rows_loaded"],
        recon_dict["canonical_total_records"],
        recon_dict["backfill_transactions"],
        recon_dict["backfill_credit"],
        recon_dict["backfill_debit"],
        recon_dict["backfill_net_effect"],
        json.dumps(recon_dict.get("quarantine_reasons", {}))
    )]

    schema = T.StructType([
        T.StructField("run_timestamp", T.TimestampType(), False),
        T.StructField("source_file", T.StringType(), False),
        T.StructField("rows_read", T.LongType(), False),
        T.StructField("rows_quarantined", T.LongType(), False),
        T.StructField("duplicates_removed", T.LongType(), False),
        T.StructField("new_rows_loaded", T.LongType(), False),
        T.StructField("canonical_total_records", T.LongType(), False),
        T.StructField("backfill_transactions", T.LongType(), False),
        T.StructField("backfill_credit", T.DoubleType(), False),
        T.StructField("backfill_debit", T.DoubleType(), False),
        T.StructField("backfill_net_effect", T.DoubleType(), False),
        T.StructField("quarantine_reasons_json", T.StringType(), False),
    ])

    spark.createDataFrame(row, schema=schema).write.format("delta").mode("append").saveAsTable(reconciliation_table)
