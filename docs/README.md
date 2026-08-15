# ClimateInsights Analytics Platform

AWS-native climate analytics pipeline built for **ClimateInsights Corp**.
Transforms 41,727 raw NOAA CSV files into a queryable lakehouse using
EMR + PySpark, S3, Athena, Step Functions, and EventBridge.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                     ClimateInsights Data Platform                       │
│                                                                         │
│  ┌──────────────────┐     ┌──────────────────┐     ┌────────────────┐  │
│  │   BRONZE Layer   │     │   SILVER Layer   │     │  GOLD Layer    │  │
│  │                  │     │                  │     │                │  │
│  │ s3://aws-bigdata │────▶│ s3://YOUR_BUCKET │────▶│ s3://YOUR_BUCK │  │
│  │ -blog/artifacts/ │     │ /silver/noaa_    │     │ /gold/         │  │
│  │                  │     │ parquet/         │     │                │  │
│  │ 41,727 CSV files │     │ Parquet, snappy  │     │ annual_temp_   │  │
│  │ 11.3 GB raw      │     │ year/month parts │     │ trends/        │  │
│  │ 347M+ rows       │     │ 347M+ rows       │     │ monthly_prcp/  │  │
│  │ No headers       │     │ QC-filtered      │     │ station_cov/   │  │
│  └──────────────────┘     └──────────────────┘     └────────────────┘  │
│           ▲                                                  │          │
│           │ Phase 1 (one-time backfill)                      │          │
│           │ Phase 2 (monthly increment)                      ▼          │
│                                                    ┌────────────────┐  │
│  ┌─────────────────────────────────────────────┐   │    Athena /    │  │
│  │             Orchestration Layer             │   │  QuickSight    │  │
│  │                                             │   │                │  │
│  │  EventBridge Scheduler ──▶ Step Functions  │   │ Self-service   │  │
│  │  (2nd of month, 06:00 UTC)   │              │   │ analytics for  │  │
│  │                              ▼              │   │ business users │  │
│  │                     ┌─────────────┐         │   └────────────────┘  │
│  │                     │  Transient  │         │                       │
│  │                     │ EMR Cluster │         │                       │
│  │                     │  (Spot)     │         │                       │
│  │                     │  ~15 min    │         │                       │
│  │                     └─────────────┘         │                       │
│  │                              │              │                       │
│  │                     SNS Notification        │                       │
│  │                     (email/SMS on done)     │                       │
│  └─────────────────────────────────────────────┘                       │
└─────────────────────────────────────────────────────────────────────────┘
```

### Why this architecture works

| Decision | Reason |
|---|---|
| Transient EMR clusters | Pay only during processing. Zero idle cost. |
| Spot instances (Core nodes) | Batch job is fault-tolerant and not latency-sensitive. 60–90% cheaper. |
| Parquet + Snappy | 5–10× faster Athena reads vs raw CSV. Columnar = predicate pushdown. |
| Partition by year/month | Monthly delivery → one new partition. Never reprocess historical data. |
| Idempotent Silver append | Spot reclaim → Step Functions retry → skip already-written partition. |
| Gold full overwrite | Gold tables are aggregates across all years. Safe to regenerate (~seconds). |
| No Managed Scaling | Single-job transient cluster with known data volume. Fixed sizing is predictable and cheaper. |

---

## Project Structure

```
ClimateInsights-Analytics-Platform/
│
├── scripts/
│   ├── bootstrap_emr.sh              # EMR bootstrap — installs Python deps, tunes Spark
│   ├── phase1/
│   │   ├── bronze_to_silver.py       # Historical backfill: CSV → Parquet (Silver)
│   │   └── silver_to_gold.py         # Historical aggregations → Gold tables
│   └── phase2/
│       ├── monthly_silver_append.py  # Monthly increment: idempotent Silver append
│       └── gold_full_refresh.py      # Monthly Gold regeneration (full overwrite)
│
├── configs/
│   ├── emr_cluster_phase1.json       # Phase 1 cluster spec (4 Core nodes)
│   └── emr_cluster_phase2.json       # Phase 2 cluster spec (2 Core nodes)
│
├── infra/
│   ├── cloudformation.yaml           # Full infrastructure stack (S3, IAM, SNS, SFN, EB)
│   ├── step_functions_pipeline.asl.json  # Step Functions state machine definition
│   └── eventbridge_scheduler.json    # EventBridge Scheduler rule (reference doc)
│
├── athena/
│   └── create_tables.sql             # DDL for Silver + 3 Gold tables, sample queries
│
└── docs/
    └── README.md                     # This file
```

---

## Prerequisites

- AWS CLI v2 installed and configured (`aws configure`)
- An AWS account with permissions to create: S3, EMR, IAM, Step Functions, EventBridge, SNS, SQS, Glue, Athena, CloudWatch
- Python 3.8+ locally (only needed if running tests — not required for deployment)

---

## Deployment Guide

### Step 1 — Deploy infrastructure via CloudFormation

```bash
# Replace values with your own
STACK_NAME="climateinsights-platform"
BUCKET_SUFFIX="$(aws sts get-caller-identity --query Account --output text)"
YOUR_EMAIL="your-email@company.com"

aws cloudformation deploy \
  --stack-name   "${STACK_NAME}" \
  --template-file infra/cloudformation.yaml \
  --capabilities  CAPABILITY_NAMED_IAM \
  --parameter-overrides \
    ProjectName="${STACK_NAME}" \
    BucketSuffix="${BUCKET_SUFFIX}" \
    NotificationEmail="${YOUR_EMAIL}" \
    Environment="production"
```

After deployment, note the output values:
```bash
aws cloudformation describe-stacks \
  --stack-name "${STACK_NAME}" \
  --query "Stacks[0].Outputs" \
  --output table
```

You'll need **DataBucketName** for all subsequent steps.

---

### Step 2 — Upload scripts to S3

```bash
DATA_BUCKET="$(aws cloudformation describe-stacks \
  --stack-name "${STACK_NAME}" \
  --query "Stacks[0].Outputs[?OutputKey=='DataBucketName'].OutputValue" \
  --output text)"

echo "Data bucket: ${DATA_BUCKET}"

# Upload all scripts
aws s3 sync scripts/ "s3://${DATA_BUCKET}/scripts/" \
  --exclude "*.pyc" --exclude "__pycache__/*"

echo "Scripts uploaded."
```

---

### Step 3 — Run Phase 1 Historical Backfill

This is a one-time operation. It processes 41,727 CSV files (11.3 GB) and should
take approximately 45 minutes on the configured cluster.

```bash
# Update the placeholder in the cluster config first
sed "s/REPLACE_WITH_YOUR_BUCKET/${DATA_BUCKET}/g" \
  configs/emr_cluster_phase1.json > /tmp/phase1_cluster.json

# Launch the cluster (both steps run automatically, then cluster terminates)
CLUSTER_ID=$(aws emr create-cluster \
  --cli-input-json file:///tmp/phase1_cluster.json \
  --query ClusterId \
  --output text)

echo "Phase 1 cluster launched: ${CLUSTER_ID}"
echo "Track progress:"
echo "  aws emr describe-cluster --cluster-id ${CLUSTER_ID}"
echo "  https://console.aws.amazon.com/emr/home#/clusters/${CLUSTER_ID}"
```

Monitor until the cluster reaches TERMINATED state (both steps succeeded):
```bash
aws emr wait cluster-terminated --cluster-id "${CLUSTER_ID}"
echo "Phase 1 complete!"
```

---

### Step 4 — Create Athena tables

```bash
# Set Athena results location first (one-time)
aws athena update-work-group \
  --work-group primary \
  --configuration "ResultConfiguration={OutputLocation=s3://${DATA_BUCKET}/athena-results/}"

# Run the DDL (replace bucket name in the SQL file first)
sed "s/REPLACE_WITH_YOUR_BUCKET/${DATA_BUCKET}/g" \
  athena/create_tables.sql > /tmp/create_tables.sql

# Split and run each statement via Athena CLI, or paste into Athena console
# Tip: Athena console → Query editor → paste create_tables.sql → Run
```

Verify tables are accessible:
```sql
-- Run in Athena console
SHOW TABLES IN climateinsights;

SELECT COUNT(*) FROM climateinsights.silver_noaa_observations
WHERE year = 2019 AND month = 6;
```

---

### Step 5 — Enable the Phase 2 monthly pipeline

The EventBridge Scheduler rule is deployed by CloudFormation and is **ENABLED** by default.
It will fire automatically on the 2nd of each month at 06:00 UTC.

To trigger it manually (e.g., to test or backfill a specific month):

```bash
STATE_MACHINE_ARN="$(aws cloudformation describe-stacks \
  --stack-name "${STACK_NAME}" \
  --query "Stacks[0].Outputs[?OutputKey=='StateMachineArn'].OutputValue" \
  --output text)"

SNS_TOPIC_ARN="$(aws cloudformation describe-stacks \
  --stack-name "${STACK_NAME}" \
  --query "Stacks[0].Outputs[?OutputKey=='NotificationTopicArn'].OutputValue" \
  --output text)"

# Trigger for a specific year/month
aws stepfunctions start-execution \
  --state-machine-arn "${STATE_MACHINE_ARN}" \
  --name "manual-backfill-2020-03" \
  --input "{
    \"bucket\":   \"${DATA_BUCKET}\",
    \"snsTopic\": \"${SNS_TOPIC_ARN}\",
    \"year\":     2020,
    \"month\":    3
  }"
```

---

## How the Monthly Pipeline Works

```
EventBridge fires (2nd of month, 06:00 UTC)
          │
          ▼
    Step Functions starts execution
          │
          ▼
    ┌─────────────────────────────────────┐
    │     CreateEMRCluster                │
    │     m5.xlarge master (on-demand)    │
    │     m5.xlarge × 2 core (spot)       │
    │     ~5 min to launch                │
    └─────────────────────────────────────┘
          │
          ▼
    ┌─────────────────────────────────────┐
    │     RunSilverAppend                 │
    │     monthly_silver_append.py        │
    │     • Check partition exists (skip) │
    │     • Read landing zone CSVs        │
    │     • Filter bad records            │
    │     • Write year=YYYY/month=MM/     │
    │     ~8–10 min                       │
    └─────────────────────────────────────┘
          │
          ▼
    ┌─────────────────────────────────────┐
    │     RunGoldRefresh                  │
    │     gold_full_refresh.py            │
    │     • Read full Silver (all years)  │
    │     • Recompute 3 Gold tables       │
    │     • Overwrite Gold output         │
    │     ~5 min                          │
    └─────────────────────────────────────┘
          │
          ▼
    NotifySuccess → SNS email to stakeholders
          │
          ▼
    Cluster auto-terminates
    Cost stops immediately
```

Total runtime: ~15–20 minutes per month.

---

## Data Schema

### Bronze (raw CSV, no headers)
| Column | Type | Notes |
|---|---|---|
| station_id | string | GHCN station ID, e.g. `USW00094728` |
| obs_date | string | YYYYMMDD format |
| element | string | TMAX, TMIN, PRCP, SNOW, … |
| value | int | Raw value (tenths of °C or mm) |
| m_flag | string | Measurement flag |
| q_flag | string | Quality flag — non-null = failed QC |
| s_flag | string | Source flag |
| obs_time | string | HHMM or blank |

### Silver (Parquet, partitioned by year/month)
Same columns as Bronze, plus:
| Column | Type | Notes |
|---|---|---|
| obs_date | date | Parsed from string |
| country_code | string | First 2 chars of station_id |
| year | int | Partition key |
| month | int | Partition key |

Quality filtered: q_flag is null/blank, value is not null, element in {TMAX, TMIN, PRCP, SNOW}.

### Gold Tables
**annual_temperature_trends** — `year, element, avg_temp_c, min_temp_c, max_temp_c, record_count`

**monthly_precipitation** — `year, month, element, total_mm, avg_mm, station_count, record_count`

**station_coverage** — `year, active_stations, total_records, elements_covered, countries_covered`

---

## Performance Results

| Metric | Before | After |
|---|---|---|
| Annual analysis runtime | 3.5 hours | ~45 min (Phase 1), ~15 min (Phase 2) |
| Monthly increment cost | N/A | ~$1–2 (transient spot cluster) |
| Athena query time (annual) | Minutes on CSV | Seconds on Parquet |
| Storage efficiency | 11.3 GB CSV | ~2–3 GB Parquet (Snappy) |

---

## Cost Estimate

| Component | Cost |
|---|---|
| Phase 1 EMR (one-time, ~45 min) | ~$0.50–0.80 |
| Phase 2 EMR (monthly, ~15 min) | ~$0.15–0.25/month |
| S3 storage (Silver + Gold) | ~$0.05–0.10/month |
| Athena queries | $5 per TB scanned (Gold = pennies) |
| Step Functions | ~$0.001/execution |
| **Total ongoing** | **~$0.30–0.50/month** |

---

## Operations

### Check pipeline execution history
```bash
aws stepfunctions list-executions \
  --state-machine-arn "${STATE_MACHINE_ARN}" \
  --status-filter SUCCEEDED \
  --max-results 10
```

### Check EMR logs on failure
```bash
aws s3 ls "s3://${DATA_BUCKET}/logs/emr/phase2/" --recursive
aws s3 cp "s3://${DATA_BUCKET}/logs/emr/phase2/CLUSTER_ID/steps/STEP_ID/stderr.gz" - | gunzip
```

### Disable the monthly schedule (e.g., during maintenance)
```bash
aws scheduler update-schedule \
  --name "climateinsights-monthly-trigger" \
  --state DISABLED \
  --schedule-expression "cron(0 6 2 * ? *)" \
  --flexible-time-window Mode=FLEXIBLE,MaximumWindowInMinutes=60 \
  --target Arn="${STATE_MACHINE_ARN}",RoleArn="SCHEDULER_ROLE_ARN"
```

### Backfill a missed month
```bash
aws stepfunctions start-execution \
  --state-machine-arn "${STATE_MACHINE_ARN}" \
  --name "backfill-2020-06-$(date +%s)" \
  --input "{\"bucket\":\"${DATA_BUCKET}\",\"snsTopic\":\"${SNS_TOPIC_ARN}\",\"year\":2020,\"month\":6}"
```

### Re-process a month (e.g., NOAA published a correction)
```bash
# Delete the existing Silver partition first (makes idempotency check fail → re-processes)
aws s3 rm "s3://${DATA_BUCKET}/silver/noaa_parquet/year=2020/month=3/" --recursive

# Then trigger the pipeline normally
aws stepfunctions start-execution --state-machine-arn "${STATE_MACHINE_ARN}" \
  --input "{\"bucket\":\"${DATA_BUCKET}\",\"snsTopic\":\"${SNS_TOPIC_ARN}\",\"year\":2020,\"month\":3}"
```

---

## Teardown

```bash
# 1. Empty the data bucket (CloudFormation cannot delete non-empty buckets)
aws s3 rm "s3://${DATA_BUCKET}/" --recursive

# 2. Delete the CloudFormation stack (removes all resources)
aws cloudformation delete-stack --stack-name "${STACK_NAME}"
aws cloudformation wait stack-delete-complete --stack-name "${STACK_NAME}"
echo "Stack deleted."
```

---

## Security Notes

- All S3 data encrypted at rest (AES-256 SSE)
- S3 bucket policy denies non-SSL access
- EMR instances use scoped IAM roles (least privilege)
- Step Functions role can only manage EMR + publish to this project's SNS topic
- No hardcoded credentials — all access via IAM instance profiles and roles
