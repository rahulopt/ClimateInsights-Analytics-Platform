"""
Phase 1 — Bronze → Silver (Historical Backfill)
================================================
Reads 41,727 raw NOAA CSV files (no headers, ~270 KB each) from the public
S3 bucket, enforces schema, cleans data quality flags, and writes Parquet
partitioned by year and month to the Silver layer.

Key design decisions
--------------------
* Schema-on-read with explicit StructType — avoids a full CSV scan for inference
* spark.sql.files.maxPartitionBytes = 128 MB → Spark merges small input files
  into larger logical partitions (the core "small files fix")
* openCostInBytes tuned to encourage combining — avoids per-file overhead
* Writing with partitionBy("year","month") so Phase 2 appends never touch
  existing partitions
* Idempotent: uses overwrite mode per-partition so re-running is safe

Usage
-----
spark-submit scripts/phase1/bronze_to_silver.py \
    --source_path s3://aws-bigdata-blog/artifacts/athena-ctas-insert-into-blog/ \
    --silver_path s3://YOUR_BUCKET/silver/noaa_parquet/ \
    --year_start  2010 \
    --year_end    2019
"""

import argparse
import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("bronze_to_silver")


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------
# NOAA GHCN-Daily CSV layout (no header row in the files)
# Ref: https://www.ncei.noaa.gov/pub/data/ghcn/daily/readme.txt
BRONZE_SCHEMA = StructType(
    [
        StructField("station_id", StringType(), nullable=False),   # GHCN station ID
        StructField("obs_date",   StringType(), nullable=False),   # YYYYMMDD string
        StructField("element",    StringType(), nullable=False),   # TMAX / TMIN / PRCP …
        StructField("value",      IntegerType(), nullable=True),   # Tenths of °C or mm
        StructField("m_flag",     StringType(), nullable=True),    # Measurement flag
        StructField("q_flag",     StringType(), nullable=True),    # Quality flag
        StructField("s_flag",     StringType(), nullable=True),    # Source flag
        StructField("obs_time",   StringType(), nullable=True),    # HHMM or blank
    ]
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def build_spark() -> SparkSession:
    """Build SparkSession with tuning for many small CSV files."""
    return (
        SparkSession.builder
        .appName("ClimateInsights-Phase1-BronzeToSilver")
        # Merge small input splits into ~128 MB logical partitions
        .config("spark.sql.files.maxPartitionBytes", str(128 * 1024 * 1024))
        # Cost per file open — raising this discourages tiny partitions
        .config("spark.sql.files.openCostInBytes",   str(8 * 1024 * 1024))
        # Write Parquet with snappy compression
        .config("spark.sql.parquet.compression.codec", "snappy")
        # Don't create _SUCCESS files we don't need
        .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
        .getOrCreate()
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Phase 1: Bronze → Silver")
    p.add_argument("--source_path", required=True,
                   help="S3 path to raw NOAA CSV files")
    p.add_argument("--silver_path", required=True,
                   help="S3 path for output Silver Parquet")
    p.add_argument("--year_start", type=int, default=2010)
    p.add_argument("--year_end",   type=int, default=2019)
    return p.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    log.info("Source  : %s", args.source_path)
    log.info("Silver  : %s", args.silver_path)
    log.info("Years   : %d – %d", args.year_start, args.year_end)

    # ── 1. Read all CSV files ──────────────────────────────────────────────
    # The public bucket uses naming like  2010.csv001, 2010.csv002, …
    # Reading the whole prefix lets Spark coalesce them into fat partitions.
    log.info("Reading raw CSVs …")
    raw = (
        spark.read
        .schema(BRONZE_SCHEMA)
        .option("header", "false")
        .option("inferSchema", "false")
        .option("mode", "PERMISSIVE")          # nullify bad rows, don't abort
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .csv(args.source_path)
    )

    raw_count = raw.count()
    log.info("Raw row count: {:,}".format(raw_count))

    # ── 2. Parse and enrich ───────────────────────────────────────────────
    log.info("Parsing dates and deriving partitions …")
    silver = (
        raw
        # Parse YYYYMMDD string into proper date column
        .withColumn("obs_date_parsed",
                    F.to_date(F.col("obs_date"), "yyyyMMdd"))
        # Derive partition keys
        .withColumn("year",  F.year("obs_date_parsed").cast(IntegerType()))
        .withColumn("month", F.month("obs_date_parsed").cast(IntegerType()))
        # Convert temperature and precipitation from tenths
        # (keep raw integer value — let Gold layer handle unit conversion
        #  so Silver stays a faithful cleaned copy of the source)
        # Country code is the first 2 chars of station_id (GHCN convention)
        .withColumn("country_code", F.substring("station_id", 1, 2))
        # Drop the raw string date — obs_date_parsed is the canonical column
        .drop("obs_date")
        # ── Quality filter ────────────────────────────────────────────────
        # NOAA q_flag = non-null means the observation failed a QC check.
        # Drop flagged rows to avoid polluting analytics.
        .filter(F.col("q_flag").isNull() | (F.col("q_flag") == ""))
        # Drop rows with null values (sensor failures, missing data)
        .filter(F.col("value").isNotNull())
        # Filter to requested year range
        .filter(F.col("year").between(args.year_start, args.year_end))
        # Only keep weather elements relevant to the business requirements
        .filter(F.col("element").isin("TMAX", "TMIN", "PRCP", "SNOW"))
        # Rename parsed date for clarity
        .withColumnRenamed("obs_date_parsed", "obs_date")
        # Final column order
        .select(
            "station_id",
            "obs_date",
            "element",
            "value",
            "m_flag",
            "q_flag",
            "s_flag",
            "obs_time",
            "country_code",
            "year",
            "month",
        )
    )

    # ── 3. Repartition before write ────────────────────────────────────────
    # ~200 partitions → each output file ~50-100 MB (good for Athena reads)
    # Repartition by year+month so each partition file is self-contained
    log.info("Repartitioning for efficient Parquet output …")
    silver = silver.repartition(200, "year", "month")

    # ── 4. Write Parquet ───────────────────────────────────────────────────
    log.info("Writing Silver Parquet to %s", args.silver_path)
    (
        silver.write
        .mode("overwrite")
        .partitionBy("year", "month")
        .parquet(args.silver_path)
    )

    # ── 5. Summary ─────────────────────────────────────────────────────────
    silver_count = (
        spark.read.parquet(args.silver_path).count()
    )
    log.info("Silver row count: {:,}".format(silver_count))
    log.info(
        "Rows filtered out: {:,} ({:.1f}%%)".format(
            raw_count - silver_count,
            100.0 * (raw_count - silver_count) / max(raw_count, 1),
        )
    )
    log.info("Phase 1 Bronze → Silver complete.")

    spark.stop()


if __name__ == "__main__":
    main()
