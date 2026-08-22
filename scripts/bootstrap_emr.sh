#!/bin/bash
# =============================================================================
# EMR Bootstrap Action — ClimateInsights Analytics Platform
# Runs on every node (master + core) at cluster launch time.
# Installs Python dependencies and configures the Spark environment.
# =============================================================================
set -euxo pipefail

echo "=== ClimateInsights EMR Bootstrap starting ==="

# ── 1. System packages ────────────────────────────────────────────────────────
sudo yum update -y -q
sudo yum install -y -q python3-pip python3-devel

# ── 2. Python libraries ───────────────────────────────────────────────────────
# Pin versions for reproducibility
sudo pip3 install --quiet \
    boto3==1.34.69 \
    pyarrow==14.0.2 \
    pandas==2.1.4

# ── 3. Spark / Hadoop tuning ─────────────────────────────────────────────────
# Raise the default shuffle partition count for large datasets.
# These are cluster-wide defaults; individual jobs can override via SparkConf.
SPARK_DEFAULTS="/etc/spark/conf/spark-defaults.conf"
sudo bash -c "cat >> ${SPARK_DEFAULTS}" <<'SPARK_EOF'

# ── ClimateInsights tuning ────────────────────────────────────────────────────
# Use Kryo serializer — significantly faster than default Java serializer
spark.serializer                            org.apache.spark.serializer.KryoSerializer

# Default parallelism for shuffles (matched to ~4× total vcores)
spark.sql.shuffle.partitions                200

# Parquet-specific: enable vectorised reader
spark.sql.parquet.enableVectorizedReader    true

# Compress shuffles with snappy
spark.io.compression.codec                 snappy

# Dynamic allocation disabled — fixed cluster, predictable workload
spark.dynamicAllocation.enabled            false

# Adaptive query execution (EMR 6.x+)
spark.sql.adaptive.enabled                 true
spark.sql.adaptive.coalescePartitions.enabled true
SPARK_EOF

echo "=== Spark defaults updated ==="

# ── 4. Log4j noise reduction ─────────────────────────────────────────────────
LOG4J="/etc/spark/conf/log4j.properties"
if [ -f "${LOG4J}" ]; then
    sudo sed -i 's/log4j.rootCategory=INFO/log4j.rootCategory=WARN/' "${LOG4J}" || true
fi

echo "=== ClimateInsights EMR Bootstrap complete ==="
