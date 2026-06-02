# Lakehouse Architecture for E-Commerce Transactions

A production-grade AWS Lakehouse pipeline built with PySpark Delta Lake, AWS Glue, Step Functions, and GitHub Actions CI/CD.

---

## Architecture Overview

```
S3 raw/          →   AWS Glue + PySpark   →   S3 lakehouse-dwh/   →   Athena
(CSV/Excel)          (Delta Lake ETL)          (Delta Tables)          (Analytics)
                           ↑
                   AWS Step Functions
                   (Orchestration)
                           ↑
                   GitHub Actions
                   (CI/CD on main)
```

### S3 Zone Layout

```
s3://<bucket>/
├── raw/
│   ├── products/
│   ├── orders/
│   └── order_items/
├── lakehouse-dwh/
│   ├── products/          ← Delta table, partitioned by department
│   ├── orders/            ← Delta table, partitioned by date
│   └── order_items/       ← Delta table, partitioned by date
├── archived/
│   ├── products/
│   ├── orders/
│   └── order_items/
└── rejected/
    ├── products/
    ├── orders/
    └── order_items/
```

---

## Data Sources

| Dataset | Format | Rows | Primary Key | Partition By |
|---|---|---|---|---|
| Products | CSV | ~1,000 | `product_id` | `department` |
| Orders | Excel (.xlsx) | ~500 | `order_id` | `date` |
| Order Items | Excel (.xlsx) | ~2,768 | `id` | `date` |

### Schema

**products**
```
product_id      INTEGER  (PK, NOT NULL)
department_id   INTEGER
department      STRING
product_name    STRING
```

**orders**
```
order_num         INTEGER
order_id          INTEGER  (PK, NOT NULL)
user_id           INTEGER  (NOT NULL)
order_timestamp   TIMESTAMP
total_amount      DOUBLE
date              DATE
```

**order_items**
```
id                    INTEGER  (PK, NOT NULL)
order_id              INTEGER  (NOT NULL, FK → orders.order_id)
user_id               INTEGER
days_since_prior_order INTEGER
product_id            INTEGER  (FK → products.product_id)
add_to_cart_order     INTEGER
reordered             INTEGER
order_timestamp       TIMESTAMP
date                  DATE
```

---

## Project Structure

```
.
├── glue_jobs/
│   ├── common/
│   │   └── utils.py              # Shared validation, logging, Delta helpers
│   ├── glue_products.py          # Products ETL job
│   ├── glue_orders.py            # Orders ETL job
│   └── glue_order_items.py       # Order Items ETL job
├── step_functions/
│   └── state_machine.json        # Step Functions state machine definition
├── tests/
│   ├── conftest.py               # PySpark test fixtures
│   ├── test_products.py
│   ├── test_orders.py
│   └── test_order_items.py
├── scripts/
│   └── upload_raw_data.py        # Upload local data files to S3 raw zone
├── .github/
│   └── workflows/
│       └── ci.yml                # GitHub Actions CI/CD pipeline
├── Data/                         # Local sample data files
├── requirements.txt
└── README.md
```

---

## Validation Rules

Each Glue job enforces these rules before writing to Delta Lake:

| Rule | Products | Orders | Order Items |
|---|---|---|---|
| Non-null primary key | `product_id` | `order_id` | `id` |
| Non-null user reference | — | `user_id` | `user_id`, `order_id` |
| Valid timestamp format | — | `order_timestamp` | `order_timestamp` |
| Deduplication on PK | Yes | Yes | Yes |
| Referential integrity check | — | — | `order_id` exists in orders |

Rejected records are written to `s3://<bucket>/rejected/<dataset>/` with an error reason column.

---

## ETL Logic (Per Job)

1. Read raw file from S3 (`raw/<dataset>/`)
2. Cast and enforce schema
3. Validate — split into valid and rejected sets
4. Deduplicate on primary key (keep latest by timestamp)
5. Merge (upsert) into Delta table using primary key — idempotent re-runs
6. Write rejected records to rejected zone
7. Archive source file to `archived/<dataset>/`

---

## Orchestration (Step Functions)

The state machine runs these steps in order:

```
[Trigger] → [Glue: Products] ──┐
            [Glue: Orders]   ──┼─ (parallel) → [Archive Files] → [Glue Crawler] → [Athena Validation]
            [Glue: OrderItems] ┘
```

Failure paths: each Glue job state has a `Catch` that routes to an error logging state.

---

## CI/CD (GitHub Actions)

Triggers on push/PR to `main`.

Steps:
1. Install dependencies
2. Run unit tests (`pytest tests/`)
3. Lint PySpark scripts (`flake8 glue_jobs/`)
4. (Optional) Deploy updated Step Function definition to AWS

---

## Setup & Deployment

### Prerequisites

- AWS CLI configured with appropriate IAM permissions
- Python 3.9+
- An S3 bucket created

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Upload raw data to S3

```bash
python scripts/upload_raw_data.py --bucket <your-bucket-name>
```

### 3. Deploy Glue Jobs

Upload scripts from `glue_jobs/` to S3 and register them in AWS Glue via the console or CLI:

```bash
aws s3 cp glue_jobs/ s3://<bucket>/glue-scripts/ --recursive
```

### 4. Deploy Step Functions State Machine

```bash
aws stepfunctions create-state-machine \
  --name ecommerce-lakehouse-pipeline \
  --definition file://step_functions/state_machine.json \
  --role-arn arn:aws:iam::<account-id>:role/<step-functions-role>
```

### 5. Run the Pipeline

Trigger the state machine via the AWS Console or:

```bash
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:<region>:<account-id>:stateMachine:ecommerce-lakehouse-pipeline
```

---

## Environment Variables / Job Parameters

Each Glue job accepts these parameters:

| Parameter | Description |
|---|---|
| `--S3_BUCKET` | S3 bucket name |
| `--RAW_PREFIX` | S3 prefix for raw input files |
| `--DWH_PREFIX` | S3 prefix for Delta table output |
| `--ARCHIVED_PREFIX` | S3 prefix for archived raw files |
| `--REJECTED_PREFIX` | S3 prefix for rejected records |

---

## Technologies

| Technology | Version | Purpose |
|---|---|---|
| AWS Glue | 4.0 | Serverless Spark runtime |
| Apache Spark | 3.3 | Distributed data processing |
| Delta Lake | 2.3 | ACID table format on S3 |
| AWS Step Functions | — | Pipeline orchestration |
| Amazon Athena | — | SQL analytics on Delta tables |
| GitHub Actions | — | CI/CD automation |
| Python | 3.9+ | ETL scripting |
