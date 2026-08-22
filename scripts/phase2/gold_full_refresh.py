"""
Phase 2 — Gold Full Refresh
============================
Reads the entire Silver layer (all historical + newly appended partition)
and regenerates all three Gold tables from scratch.

Why full overwrite (not append)?
---------------------------------
Gold tables are aggregates across ALL years. E.g., "annual_temperature_trends"
shows avg temp per year from 2010–present. When 2020 data lands, the old 2020
row (partial) must be replaced, and you may also want to recalculate trend
statistics across the full window. A full overwrite is the safest pattern here.

Gold tables are small (one row per year per element) even though Silver has
347M+ rows — so the full overwrite is fast and cheap.

Usage
-----
spark-submit scripts/phase2/gold_full_refresh.py \
    --silver_path s3://BUCKET/silver/noaa_parquet/ \
    --gold_path   s3://BUCKET/gold/ \
    --year  2020 \
    --month 1
"""

import argparse
import logging
import sys

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("gold_full_refresh")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Phase 2: Gold full refresh")
    p.add_argument("--silver_path", required=True)
    p.add_argument("--gold_path",   required=True)
    p.add_argument("--year",  type=int, required=True,
                   help="Year just processed (used for logging / SNS message)")
    p.add_argument("--month", type=int, required=True,
                   help="Month just processed")
    return p.parse_args()


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("ClimateInsights-Phase2-GoldRefresh")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.shuffle.partitions", "100")
        .config("spark.sql.adaptive.enabled",   "true")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Gold table builders  (identical logic to Phase 1, re-used here cleanly)
# ---------------------------------------------------------------------------

def build_annual_temperature_trends(silver: DataFrame) -> DataFrame:
    return (
        silver
        .filter(F.col("element").isin("TMAX", "TMIN"))
        .groupBy("year", "element")
        .agg(
            F.round(F.avg("value") / 10.0, 2).alias("avg_temp_c"),
            F.round(F.min("value") / 10.0, 2).alias("min_temp_c"),
            F.round(F.max("value") / 10.0, 2).alias("max_temp_c"),
            F.count("*").alias("record_count"),
        )
        .orderBy("year", "element")
    )


def build_monthly_precipitation(silver: DataFrame) -> DataFrame:
    return (
        silver
        .filter(F.col("element").isin("PRCP", "SNOW"))
        .withColumn(
            "value_mm",
            F.when(F.col("element") == "PRCP",
                   F.round(F.col("value") / 10.0, 2))
             .otherwise(F.col("value").cast("double"))
        )
        .groupBy("year", "month", "element")
        .agg(
            F.round(F.sum("value_mm"), 2).alias("total_mm"),
            F.round(F.avg("value_mm"), 2).alias("avg_mm"),
            F.countDistinct("station_id").alias("station_count"),
            F.count("*").alias("record_count"),
        )
        .orderBy("year", "month", "element")
    )


def build_station_coverage(silver: DataFrame) -> DataFrame:
    return (
        silver
        .groupBy("year")
        .agg(
            F.countDistinct("station_id").alias("active_stations"),
            F.count("*").alias("total_records"),
            F.collect_set("element").alias("elements_covered"),
            F.countDistinct("country_code").alias("countries_covered"),
        )
        .orderBy("year")
    )


def write_gold(df: DataFrame, path: str, table_name: str) -> int:
    """Write Gold table (overwrite) and return row count."""
    log.info("Writing Gold table '%s' …", table_name)
    df.write.mode("overwrite").parquet(path)
    count = df.count()
    log.info("  → %s: {:,} rows".format(count), table_name)
    return count


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    log.info(
        "Gold full refresh triggered by year=%d month=%d",
        args.year, args.month,
    )
    log.info("Silver : %s", args.silver_path)
    log.info("Gold   : %s", args.gold_path)

    # ── Load full Silver (all partitions, all years) ───────────────────────
    log.info("Loading full Silver layer …")
    silver = spark.read.parquet(args.silver_path).cache()
    total = silver.count()
    log.info("Silver total rows: {:,}".format(total))

    gold_base = args.gold_path.rstrip("/")

    trend_count = write_gold(
        build_annual_temperature_trends(silver),
        f"{gold_base}/annual_temperature_trends/",
        "annual_temperature_trends",
    )
    precip_count = write_gold(
        build_monthly_precipitation(silver),
        f"{gold_base}/monthly_precipitation/",
        "monthly_precipitation",
    )
    station_count = write_gold(
        build_station_coverage(silver),
        f"{gold_base}/station_coverage/",
        "station_coverage",
    )

    silver.unpersist()

    log.info(
        "Gold refresh complete: "
        "temperature_trends=%d rows, precipitation=%d rows, station_coverage=%d rows",
        trend_count, precip_count, station_count,
    )
    log.info(
        "PIPELINE_COMPLETE: year=%d month=%d silver_rows=%d",
        args.year, args.month, total,
    )
    spark.stop()


if __name__ == "__main__":
    main()
