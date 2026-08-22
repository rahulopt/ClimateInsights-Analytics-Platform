"""
Phase 2 — Monthly Silver Append (Incremental)
==============================================
Processes a single month's new NOAA data from the landing zone and appends
it as a new partition to the Silver layer.

Idempotent design
-----------------
Before processing, the job checks whether the target partition
(year=YYYY/month=MM) already exists in Silver. If it does, the job exits
cleanly without re-processing. This makes Step Functions retries safe —
a spot-reclaimed cluster that restarts will detect the completed partition
and skip straight to the Gold refresh.

Landing zone layout expected
-----------------------------
s3://BUCKET/landing/YYYY/MM/
    ├── 2020.csv001
    ├── 2020.csv002
    └── …

Output (appended partition)
----------------------------
s3://BUCKET/silver/noaa_parquet/year=2020/month=1/

Usage
-----
spark-submit scripts/phase2/monthly_silver_append.py \
    --source_path s3://BUCKET/landing/2020/01/ \
    --silver_path s3://BUCKET/silver/noaa_parquet/ \
    --year  2020 \
    --month 1
"""

import argparse
import logging
import sys

import boto3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("monthly_silver_append")

# ---------------------------------------------------------------------------
BRONZE_SCHEMA = StructType(
    [
        StructField("station_id", StringType(),  nullable=False),
        StructField("obs_date",   StringType(),  nullable=False),
        StructField("element",    StringType(),  nullable=False),
        StructField("value",      IntegerType(), nullable=True),
        StructField("m_flag",     StringType(),  nullable=True),
        StructField("q_flag",     StringType(),  nullable=True),
        StructField("s_flag",     StringType(),  nullable=True),
        StructField("obs_time",   StringType(),  nullable=True),
    ]
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Phase 2: Monthly Silver append")
    p.add_argument("--source_path", required=True,
                   help="S3 path to this month's landing zone CSVs")
    p.add_argument("--silver_path", required=True,
                   help="S3 base path for Silver Parquet layer")
    p.add_argument("--year",  type=int, required=True)
    p.add_argument("--month", type=int, required=True)
    return p.parse_args()


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName(f"ClimateInsights-Phase2-SilverAppend")
        .config("spark.sql.files.maxPartitionBytes",  str(128 * 1024 * 1024))
        .config("spark.sql.files.openCostInBytes",    str(8  * 1024 * 1024))
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.shuffle.partitions", "100")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def partition_exists(silver_path: str, year: int, month: int) -> bool:
    """
    Returns True if the target Silver partition already has data.
    Checks for the presence of any .parquet file under the partition prefix.
    """
    # Parse bucket and prefix from s3:// URI
    path = silver_path.rstrip("/")
    assert path.startswith("s3://"), "silver_path must start with s3://"
    without_scheme = path[5:]
    bucket, _, prefix = without_scheme.partition("/")
    partition_prefix = f"{prefix}/year={year}/month={month}/"

    s3 = boto3.client("s3")
    response = s3.list_objects_v2(
        Bucket=bucket,
        Prefix=partition_prefix,
        MaxKeys=1,
    )
    exists = response.get("KeyCount", 0) > 0
    return exists


def main() -> None:
    args = parse_args()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    log.info("Year=%d  Month=%d", args.year, args.month)
    log.info("Source : %s", args.source_path)
    log.info("Silver : %s", args.silver_path)

    # ── Idempotency check ─────────────────────────────────────────────────
    if partition_exists(args.silver_path, args.year, args.month):
        log.warning(
            "Partition year=%d/month=%d already exists in Silver — "
            "skipping to avoid duplicate data. "
            "Delete the partition first if a re-process is intended.",
            args.year, args.month,
        )
        spark.stop()
        return

    # ── 1. Read this month's landing CSVs ─────────────────────────────────
    log.info("Reading landing zone CSVs …")
    raw = (
        spark.read
        .schema(BRONZE_SCHEMA)
        .option("header",    "false")
        .option("mode",      "PERMISSIVE")
        .csv(args.source_path)
    )
    raw_count = raw.count()
    log.info("Raw rows read: {:,}".format(raw_count))

    # ── 2. Parse, clean, enrich ───────────────────────────────────────────
    silver = (
        raw
        .withColumn("obs_date_parsed",
                    F.to_date(F.col("obs_date"), "yyyyMMdd"))
        .withColumn("year",         F.lit(args.year).cast(IntegerType()))
        .withColumn("month",        F.lit(args.month).cast(IntegerType()))
        .withColumn("country_code", F.substring("station_id", 1, 2))
        .drop("obs_date")
        # Quality and element filters — same logic as Phase 1
        .filter(F.col("q_flag").isNull() | (F.col("q_flag") == ""))
        .filter(F.col("value").isNotNull())
        .filter(F.col("element").isin("TMAX", "TMIN", "PRCP", "SNOW"))
        # Confirm the year/month from the data matches the CLI args —
        # safety guard against misrouted files in the landing zone
        .filter(F.year("obs_date_parsed")  == args.year)
        .filter(F.month("obs_date_parsed") == args.month)
        .withColumnRenamed("obs_date_parsed", "obs_date")
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

    # ── 3. Repartition and write (append single partition) ─────────────────
    # Monthly data ~10 GB → 100 output partitions ~ 100 MB each
    log.info("Writing new partition year=%d/month=%d …", args.year, args.month)
    silver = silver.repartition(100, "year", "month")
    (
        silver.write
        .mode("overwrite")                    # overwrite just this partition
        .partitionBy("year", "month")
        .parquet(args.silver_path)
    )

    silver_count = silver.count()
    log.info(
        "Partition written: {:,} rows  (filtered {:,} bad rows)".format(
            silver_count,
            raw_count - silver_count,
        )
    )
    log.info("Phase 2 Silver append complete.")
    spark.stop()


if __name__ == "__main__":
    main()
