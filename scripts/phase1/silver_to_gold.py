"""
Phase 1 — Silver → Gold (Historical Aggregations)
==================================================
Reads the full Silver Parquet layer and produces three Gold-layer tables:

  1. annual_temperature_trends   — avg/min/max TMAX & TMIN per year
  2. monthly_precipitation       — total & avg PRCP + SNOW per year/month
  3. station_coverage            — active station count + record count per year

Each Gold table is written as Parquet, overwriting any previous version
(full-refresh pattern — acceptable because Gold tables are small aggregates).

Temperature values stored in Silver are in tenths of °C.
This job converts to °C for business users (÷ 10).

Usage
-----
spark-submit scripts/phase1/silver_to_gold.py \
    --silver_path s3://YOUR_BUCKET/silver/noaa_parquet/ \
    --gold_path   s3://YOUR_BUCKET/gold/
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
log = logging.getLogger("silver_to_gold")


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("ClimateInsights-Phase1-SilverToGold")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Phase 1: Silver → Gold")
    p.add_argument("--silver_path", required=True)
    p.add_argument("--gold_path",   required=True)
    return p.parse_args()


# ---------------------------------------------------------------------------
# Gold table builders
# ---------------------------------------------------------------------------

def build_annual_temperature_trends(silver: DataFrame) -> DataFrame:
    """
    Annual temperature statistics for TMAX and TMIN.
    One row per (year, element).
    Values converted from tenths-of-°C to °C.

    Schema:
        year            INT
        element         STRING   (TMAX | TMIN)
        avg_temp_c      DOUBLE
        min_temp_c      DOUBLE
        max_temp_c      DOUBLE
        record_count    LONG
    """
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
    """
    Monthly precipitation and snowfall totals.
    One row per (year, month, element).
    Precipitation in tenths-of-mm → mm (÷ 10).
    Snowfall already in mm (no conversion needed).

    Schema:
        year             INT
        month            INT
        element          STRING   (PRCP | SNOW)
        total_mm         DOUBLE
        avg_mm           DOUBLE
        station_count    LONG
        record_count     LONG
    """
    prcp_snow = silver.filter(F.col("element").isin("PRCP", "SNOW"))

    return (
        prcp_snow
        .withColumn(
            "value_mm",
            F.when(
                F.col("element") == "PRCP",
                F.round(F.col("value") / 10.0, 2)   # tenths of mm → mm
            ).otherwise(
                F.col("value").cast("double")         # SNOW already in mm
            )
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
    """
    Active station count and data completeness per year.
    One row per year.

    Schema:
        year                INT
        active_stations     LONG
        total_records       LONG
        elements_covered    ARRAY<STRING>
        countries_covered   LONG
    """
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


# ---------------------------------------------------------------------------
# Write helper
# ---------------------------------------------------------------------------

def write_gold(df: DataFrame, path: str, table_name: str) -> None:
    """Overwrite a Gold table and log row count."""
    log.info("Writing Gold table '%s' to %s …", table_name, path)
    (
        df.write
        .mode("overwrite")
        .parquet(path)
    )
    count = df.count()
    log.info("  → %s: {:,} rows written".format(count), table_name)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    log.info("Silver : %s", args.silver_path)
    log.info("Gold   : %s", args.gold_path)

    # ── Load full Silver layer (all years) ─────────────────────────────────
    log.info("Loading Silver layer …")
    silver = spark.read.parquet(args.silver_path).cache()
    total = silver.count()
    log.info("Silver total rows: {:,}".format(total))

    # ── Build and write each Gold table ────────────────────────────────────
    write_gold(
        build_annual_temperature_trends(silver),
        f"{args.gold_path.rstrip('/')}/annual_temperature_trends/",
        "annual_temperature_trends",
    )

    write_gold(
        build_monthly_precipitation(silver),
        f"{args.gold_path.rstrip('/')}/monthly_precipitation/",
        "monthly_precipitation",
    )

    write_gold(
        build_station_coverage(silver),
        f"{args.gold_path.rstrip('/')}/station_coverage/",
        "station_coverage",
    )

    silver.unpersist()
    log.info("Phase 1 Silver → Gold complete.")
    spark.stop()


if __name__ == "__main__":
    main()
