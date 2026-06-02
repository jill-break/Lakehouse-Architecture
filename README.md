# Lakehouse Architecture for E-Commerce Transactions

A production-grade AWS Lakehouse pipeline that ingests raw e-commerce transactional data, cleans and deduplicates it using Delta Lake on AWS Glue, and exposes it for analytics through Amazon Athena. The entire infrastructure is provisioned with Terraform and deployed via GitHub Actions CI/CD.

---

## Architecture Overview

```
                        ┌─────────────────────────────────────────────┐
                        │              GitHub Actions CI/CD            │
                        │  lint → test → terraform apply → deploy      │
                        └─────────────────┬───────────────────────────┘
                                          │ push to main
                                          ▼
You drop files into S3 raw/
        │
        │  Upload products.csv
        │  Upload orders.xlsx
        │  Upload order_items.xlsx
        │  Upload raw/_READY  ◄── this fires the pipeline
        │
        ▼
  Amazon EventBridge
  (watches for _READY marker)
        │
        ▼
  AWS Step Functions  (sequential orchestration)
        │
        ├── 1. Glue: Products ETL
        ├── 2. Glue: Orders ETL
        ├── 3. Glue: Order Items ETL
        ├── 4. Glue: Generate Manifests
        ├── 5. Glue Crawler  (update Data Catalog)
        └── 6. Athena Validation query
                  │
                  ▼
          S3 lakehouse-dwh/   ←── Delta tables (ACID, partitioned)
                  │
                  ▼
          Amazon Athena  ←── SQL analytics
```

### S3 Zone Layout

```
s3://ecommerce-lakehouse-dev-<account-id>/
├── raw/
│   ├── products/          ← drop CSV here
│   ├── orders/            ← drop Excel here
│   ├── order_items/       ← drop Excel here
│   └── _READY             ← upload last to trigger the pipeline
├── lakehouse-dwh/
│   ├── products/          ← Delta table, partitioned by department
│   ├── orders/            ← Delta table, partitioned by date
│   └── order_items/       ← Delta table, partitioned by date
├── archived/              ← raw files moved here after ingestion
├── rejected/              ← bad records written here with rejection_reason
├── glue-scripts/          ← ETL scripts deployed by CI
└── athena-results/        ← Athena query output
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
ingested_at     TIMESTAMP
```

**orders**
```
order_num         INTEGER
order_id          INTEGER  (PK, NOT NULL)
user_id           INTEGER  (NOT NULL)
order_timestamp   TIMESTAMP
total_amount      DOUBLE
date              DATE      (partition key)
ingested_at       TIMESTAMP
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
date                  DATE      (partition key)
ingested_at           TIMESTAMP
```

---

## Project Structure

```
.
├── glue_jobs/
│   ├── common/
│   │   ├── __init__.py
│   │   └── utils.py                  # Shared validation, Delta helpers, archiving
│   ├── dist/
│   │   └── common.zip                # Packaged for Glue --extra-py-files
│   ├── glue_products.py              # Products ETL job
│   ├── glue_orders.py                # Orders ETL job
│   ├── glue_order_items.py           # Order Items ETL job
│   └── glue_generate_manifests.py    # Generates Delta symlink manifests for Athena
├── step_functions/
│   ├── state_machine.json            # Reference state machine definition
│   └── state_machine.json.tpl        # Terraform templatefile (job names injected)
├── terraform/
│   ├── main.tf                       # Root module — wires all modules together
│   ├── variables.tf
│   ├── outputs.tf
│   ├── terraform.tfvars.example
│   ├── bootstrap/                    # Separate state — OIDC provider + GitHub Actions role
│   │   ├── main.tf                   # Never destroyed, survives full infrastructure teardown
│   │   └── outputs.tf
│   └── modules/
│       ├── s3/                       # Bucket, versioning, encryption, lifecycle rules
│       ├── iam/                      # Glue, Step Functions, EventBridge roles
│       ├── glue/                     # Glue jobs, crawler, Data Catalog database
│       ├── eventbridge/              # S3 event → _READY trigger → Step Functions
│       ├── step_functions/           # State machine with sequential ETL flow
│       ├── sns/                      # Alert topic for pipeline failures
│       └── athena/                   # Workgroup + named validation queries
├── tests/
│   ├── conftest.py                   # Local PySpark session fixture
│   ├── test_products.py
│   ├── test_orders.py
│   └── test_order_items.py
├── scripts/
│   ├── generate_data.py              # Generate synthetic good or bad data to /tmp/
│   ├── upload_raw_data.py            # Generate + upload clean data locally + drop _READY
│   ├── upload_bad_data.py            # Generate + upload bad data locally + drop _READY
│   └── package_glue.py              # Package common/ into common.zip for CI
├── .github/
│   └── workflows/
│       ├── ci.yml                    # Auto: lint, test, terraform apply, deploy scripts
│       ├── destroy.yml               # Manual: destroy all AWS infrastructure
│       └── upload-data.yml           # Manual: upload good or bad data
├── Data/                             # Local sample data files (git-ignored)
├── requirements.txt
└── README.md
```

---

## Validation Rules

Each Glue job enforces these rules before writing to Delta Lake:

| Rule | Products | Orders | Order Items |
|---|---|---|---|
| Non-null primary key | `product_id` | `order_id` | `id` |
| Non-null required fields | `product_name` | `user_id` | `user_id`, `order_id` |
| Valid timestamp | — | `order_timestamp` | `order_timestamp` |
| Deduplication on PK | Yes (latest wins) | Yes (latest by timestamp) | Yes (latest by timestamp) |
| Referential integrity | — | — | `order_id` must exist in orders Delta table |

Rejected records are written to `s3://<bucket>/rejected/<dataset>/` as Parquet with a `rejection_reason` column.

---

## ETL Flow (Per Job)

```
1. List files in raw/<dataset>/
2. Read CSV or Excel into Spark DataFrame
3. Cast columns to correct types
4. Validate → split valid vs rejected
5. Deduplicate on PK (window function, keep latest)
6. Upsert (MERGE) into Delta table — idempotent reruns safe
7. Write rejected records to rejected zone
8. Archive source file to archived/<dataset>/
```

---

## Step Functions Pipeline (Sequential)

```
GlueProducts → GlueOrders → GlueOrderItems → GenerateManifests → Crawler → AthenaValidation
     │               │               │                │               │              │
  on fail         on fail         on fail          on fail         on fail        on fail
     └───────────────┴───────────────┴────────────────┴───────────────┴──────────────┘
                                         SNS alert → JobFailed state
```

Each step waits for the previous to complete before starting. This guarantees:
- Products exist before Orders run
- Orders exist before Order Items run (referential integrity check works)
- All Delta data exists before manifests are generated
- Manifests exist before Athena can query the tables

---

## Automatic Pipeline Trigger

The pipeline triggers automatically when a `_READY` marker file is uploaded to `s3://<bucket>/raw/`:

```
Upload data files → Upload raw/_READY → EventBridge detects it → Step Functions starts
```

The upload script handles this automatically:

```bash
python scripts/upload_raw_data.py --bucket <bucket-name>
```

This uploads all 3 data files then drops `raw/_READY` as the final signal. **One upload batch = one pipeline execution.**

---

## GitHub Actions Workflows

### Automatic (on push to `main`)

**Lakehouse CI/CD** (`ci.yml`)
1. Lint PySpark scripts with `flake8`
2. Format check with `black`
3. Run unit tests with `pytest`
4. `terraform apply` — provision / update infrastructure
5. Upload Glue ETL scripts to S3

### Manual (GitHub Actions → Run workflow)

**Destroy Infrastructure** (`destroy.yml`)
- Type `DESTROY` to confirm
- Empties the S3 bucket, deletes the Athena workgroup, runs `terraform destroy`
- Automatically re-applies the bootstrap module after destroy so CI can authenticate on the very next push — no manual steps needed

**Upload Data & Trigger Pipeline** (`upload-data.yml`)
- Choose `good` — generates 1,000 products, 500 orders, ~2,500 order items synthetically and uploads them
- Choose `bad` — generates the same datasets with deliberate errors injected, uploads them
- Both options upload `raw/_READY` as the final step to trigger exactly one pipeline execution

---

## Data Generation

Both good and bad data are **generated synthetically** — no local data files are needed. The CI runner generates data from scratch using `scripts/generate_data.py`:

```bash
python scripts/generate_data.py --type good  # clean records, seed=42
python scripts/generate_data.py --type bad   # same + deliberate errors, seed=99
```

Locally you can also run the upload scripts directly:

```bash
python scripts/upload_raw_data.py --bucket <bucket-name>   # good data
python scripts/upload_bad_data.py  --bucket <bucket-name>  # bad data
```

Both generate data, upload all files, then drop `raw/_READY` to fire the pipeline.

---

## Bad Data Test Cases

The bad data generator injects:

| Dataset | Bad Records | Rejection Reason |
|---|---|---|
| Products | 5 rows | `null product_id` (PK violation) |
| Orders | 5 rows | `null order_id` (PK violation) |
| Orders | 3 rows | `null user_id` (required field) |
| Orders | 2 rows | Invalid timestamp format |
| Order Items | 5 rows | `null id` (PK violation) |
| Order Items | 3 rows | `order_id = 999999999` (referential integrity — order doesn't exist) |

After the pipeline runs, check `s3://<bucket>/rejected/` for the rejected records.

---

## Terraform Infrastructure

All AWS resources are managed by Terraform in `terraform/modules/`:

| Module | Resources |
|---|---|
| `s3` | Bucket, versioning, encryption, lifecycle (archived → S3-IA → Glacier) |
| `iam` | Glue role, Step Functions role, EventBridge role |
| `bootstrap` | GitHub Actions OIDC provider + role (separate state, never destroyed) |
| `glue` | 4 Glue jobs, 1 crawler, Glue Data Catalog database |
| `eventbridge` | S3 notification, EventBridge rule watching `_READY`, rule → Step Functions |
| `step_functions` | State machine (sequential), CloudWatch log group, X-Ray tracing |
| `sns` | Alert topic + email subscription |
| `athena` | Workgroup (engine v3), 4 named queries pre-loaded |

### Bootstrap (first time on a new AWS account only)

The OIDC provider and GitHub Actions role live in a **separate Terraform state** (`terraform/bootstrap/`) so they survive a full infrastructure destroy. Run once locally:

```bash
cd terraform/bootstrap
terraform init
terraform apply
```

After that, CI handles everything — including automatically re-applying bootstrap after each destroy. You never need to run this again.

### Destroy & Rebuild Flow

```
Run Destroy workflow
    ↓
terraform destroy  (removes all 51 resources)
    ↓
terraform apply bootstrap  (OIDC + role re-created instantly)
    ↓
Push to main  (CI authenticates → rebuilds everything from scratch)
```

### GitHub Secrets Required

| Secret | Value |
|---|---|
| `AWS_ROLE_ARN` | `arn:aws:iam::<account-id>:role/ecommerce-lakehouse-dev-github-actions-role` |
| `AWS_REGION` | `us-east-1` |
| `ALERT_EMAIL` | Your email for SNS pipeline failure alerts |

---

## Querying the Data (Athena)

Open Athena in the AWS Console, select workgroup `ecommerce-lakehouse-dev`, database `ecommerce_lakehouse_dev`.

**Row counts:**
```sql
SELECT
  (SELECT COUNT(*) FROM products)    AS products,
  (SELECT COUNT(*) FROM orders)      AS orders,
  (SELECT COUNT(*) FROM order_items) AS order_items;
```

**Revenue by department:**
```sql
SELECT
    p.department,
    COUNT(DISTINCT o.order_id) AS order_count,
    ROUND(SUM(o.total_amount), 2) AS total_revenue,
    ROUND(AVG(o.total_amount), 2) AS avg_order_value
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p     ON oi.product_id = p.product_id
GROUP BY p.department
ORDER BY total_revenue DESC;
```

Pre-loaded named queries are available in the Athena console under **Saved queries**.

---

## Technologies

| Technology | Version | Purpose |
|---|---|---|
| AWS Glue | 4.0 | Serverless Spark runtime |
| Apache Spark | 3.3 | Distributed data processing |
| Delta Lake | 2.3 | ACID table format on S3 |
| Amazon EventBridge | — | S3 event-driven pipeline trigger |
| AWS Step Functions | — | Sequential ETL orchestration |
| AWS Glue Data Catalog | — | Metadata layer for Athena |
| Amazon Athena | Engine v3 | SQL analytics on Delta tables |
| Terraform | ≥1.5 | Infrastructure as Code |
| GitHub Actions | — | CI/CD automation |
| Python | 3.9+ | ETL scripting and tooling |
