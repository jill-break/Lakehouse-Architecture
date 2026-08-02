# Lakehouse Architecture for E-Commerce Transactions

A production-grade AWS Lakehouse pipeline that ingests raw e-commerce transactional data, cleans and deduplicates it using Delta Lake on AWS Glue, and exposes it for analytics through Amazon Athena. The entire infrastructure is provisioned with Terraform and deployed via GitHub Actions CI/CD.

---

## Architecture Overview

```
                        ┌─────────────────────────────────────────────┐
                        │              GitHub Actions CI/CD            │
                        │  secret scan → lint → test → tf apply        │
                        └─────────────────┬───────────────────────────┘
                                          │ push to main
                                          ▼
You drop files into S3 raw/
        │
        │  Upload products.csv
        │  Upload orders.csv
        │  Upload order_items.csv
        │  Upload raw/_READY  ◄── this fires the pipeline
        │
        ▼
  Amazon EventBridge
  (watches for _READY marker)
        │
        ▼
  AWS Step Functions
        │
        ├── 1. Parallel ─┬── Glue: Products ETL
        │                └── Glue: Orders ETL
        ├── 2. Glue: Order Items ETL   (FKs need both of the above)
        ├── 3. Glue: Delta maintenance (OPTIMIZE / Z-ORDER / VACUUM)
        ├── 4. Glue Crawler            (update Data Catalog, status checked)
        └── 5. Athena validation       (asserts the warehouse is not empty)
                  │
                  ▼
          S3 lakehouse-dwh/   ←── Delta tables (ACID)
                  │
                  ▼
          Amazon Athena  ←── SQL analytics
```

### S3 Zone Layout

```
s3://ecommerce-lakehouse-dev-<account-id>/
├── raw/
│   ├── products/          ← drop CSV here
│   ├── orders/            ← drop CSV here
│   ├── order_items/       ← drop CSV here
│   └── _READY             ← upload last to trigger the pipeline
├── lakehouse-dwh/
│   ├── products/          ← Delta table, unpartitioned, Z-ORDER on product_id
│   ├── orders/            ← Delta table, partitioned by date
│   └── order_items/       ← Delta table, partitioned by date
├── archived/              ← raw files moved here after ingestion
├── rejected/              ← bad records written here with rejection_reason
├── glue-scripts/          ← ETL scripts, deployed by Terraform
└── athena-results/        ← Athena query output
```

---

## Data Sources

| Dataset | Format | Rows | Primary Key | Partition By |
|---|---|---|---|---|
| Products | CSV | ~1,000 | `product_id` | none (Z-ORDER on `product_id`) |
| Orders | CSV | ~500 | `order_id` | `date` |
| Order Items | CSV | ~2,768 | `id` | `date` |

Everything in the raw zone is CSV. Spark reads CSV natively and in parallel across executors; a spreadsheet format would have to be pulled into the driver and parsed single-threaded, which caps throughput at driver memory no matter how many workers the job is given.

### Schema

**products**
```
product_id      INTEGER  (PK, NOT NULL)
department_id   INTEGER
department      STRING
product_name    STRING   (NOT NULL)
ingested_at     TIMESTAMP
```

**orders**
```
order_num         INTEGER
order_id          BIGINT   (PK, NOT NULL)
user_id           BIGINT   (NOT NULL)
order_timestamp   TIMESTAMP (NOT NULL)
total_amount      DOUBLE
date              DATE      (partition key, NOT NULL)
ingested_at       TIMESTAMP
```

**order_items**
```
id                     BIGINT   (PK, NOT NULL)
order_id               BIGINT   (NOT NULL, FK → orders.order_id)
user_id                BIGINT   (NOT NULL)
days_since_prior_order INTEGER
product_id             BIGINT   (FK → products.product_id)
add_to_cart_order      INTEGER
reordered              INTEGER
order_timestamp        TIMESTAMP (NOT NULL)
date                   DATE      (partition key, NOT NULL)
ingested_at            TIMESTAMP
```

Both foreign keys are enforced at ingestion, not just documented — see [Validation Rules](#validation-rules).

---

## Partitioning, and Why

The brief asks for the partitioning logic to be justified, so:

**`orders` and `order_items` — partitioned by `date`.** Every analytical query on this warehouse filters or groups by day, and archiving keeps each batch to roughly one day of data, so partitions align with how the data arrives *and* how it is read. Day grain is only viable because the maintenance job compacts each partition after every run; without it, ~500 orders/day spread across a handful of Spark output files would produce a small-file problem within a week. If daily volume stays this low in a real deployment, month grain would be the better trade.

**`products` — not partitioned.** 1,000 rows across 7 departments is ~143 rows per partition: the file-listing overhead costs more than the pruning saves. `department` is also mutable — reclassifying a product would move its row between partitions — and dimension tables are looked up by key, not scanned by department. Z-ORDER on `product_id` gives the lookup locality without the partition count.

**The partition column is never part of the merge key.** See [`upsert_to_delta`](glue_jobs/common/utils.py) for the two reasons that would corrupt the tables.

---

## Project Structure

```
.
├── glue_jobs/
│   ├── common/
│   │   ├── __init__.py
│   │   ├── etl.py                    # Config-driven ETL runner shared by all datasets
│   │   ├── logging_utils.py          # Structured JSON logging + CloudWatch metrics
│   │   └── utils.py                  # Validation, dedup, Delta merge, S3 archiving
│   ├── glue_products.py              # Products config + entrypoint
│   ├── glue_orders.py                # Orders config + entrypoint
│   ├── glue_order_items.py           # Order Items config, FK hooks + entrypoint
│   └── glue_maintenance.py           # OPTIMIZE / Z-ORDER / VACUUM
├── step_functions/
│   └── state_machine.json.tpl        # Terraform templatefile — the single definition
├── terraform/
│   ├── main.tf                       # Root module — wires all modules together
│   ├── variables.tf
│   ├── outputs.tf
│   ├── terraform.tfvars.example
│   ├── bootstrap/                    # Separate state — OIDC provider + CI roles
│   │   ├── main.tf                   # Applied by hand; CI cannot modify it
│   │   ├── variables.tf
│   │   ├── outputs.tf
│   │   └── terraform.tfvars.example
│   └── modules/
│       ├── s3/                       # Bucket, versioning, encryption, TLS-only, lifecycle
│       ├── iam/                      # Glue, Step Functions, EventBridge roles
│       ├── glue/                     # Glue jobs, crawler, catalog database, alarms
│       ├── eventbridge/              # S3 event → _READY trigger → Step Functions
│       ├── step_functions/           # State machine, CloudWatch log group, X-Ray
│       ├── sns/                      # Alert topic (KMS-encrypted) + email subscription
│       └── athena/                   # Workgroup + named validation queries
├── tests/
│   ├── conftest.py                   # Local PySpark + Delta session, moto credentials
│   ├── test_delta_merge.py           # Merge idempotency and partition-change regressions
│   ├── test_etl_pipeline.py          # CSV → validate → merge, end to end
│   ├── test_referential_integrity.py # Anti/semi-join FK checks
│   ├── test_data_quality.py          # Dedup determinism, rejects, circuit breaker
│   ├── test_s3_helpers.py            # Listing and archiving against moto
│   ├── test_logging_utils.py         # JSON log shape, metrics
│   ├── test_maintenance.py           # OPTIMIZE / VACUUM
│   ├── test_order_items_fk.py        # Both foreign keys
│   ├── test_products.py
│   ├── test_orders.py
│   └── test_order_items.py
├── scripts/
│   ├── generate_data.py              # Generate synthetic good or bad data as CSV
│   └── upload_data.py                # Generate + upload a batch + drop _READY
├── .github/
│   └── workflows/
│       ├── ci.yml                    # Secret scan, lint, test, tf checks, plan/apply
│       ├── destroy.yml               # Manual: destroy all AWS infrastructure
│       └── upload-data.yml           # Manual: upload good or bad data
├── docs/
│   ├── engineering-decisions.md      # Decision log, including what broke and why
│   ├── architecture-diagram.md
│   └── high-level-architecture.md
├── pyproject.toml                    # pytest, coverage and black configuration
├── requirements.txt                  # Pinned runtime deps for the helper scripts
├── requirements-dev.txt              # Pinned dev/CI deps (Spark, Delta, pytest, moto)
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
| Non-null partition key | n/a | `date` | `date` |
| Deduplication on PK | Yes (deterministic) | Yes (latest by timestamp) | Yes (latest by timestamp) |
| Referential integrity | — | — | `order_id` → orders, `product_id` → products |
| Header conforms to schema | Yes | Yes | Yes |

Rejected records are written to `s3://<bucket>/rejected/<dataset>/<timestamp>_<run_id>/` as Parquet with a `rejection_reason` column.

**Circuit breaker.** Counting rejects without acting on the count means a fully malformed upstream export produces an empty merge, a green pipeline and no alert. Each job fails if the rejection rate exceeds `max_rejection_rate` (default 5%), and publishes `RowsIngested`, `RowsRejected` and `RejectionRate` to the `Lakehouse/ETL` CloudWatch namespace, with an alarm on the rate.

---

## ETL Flow (Per Job)

```
1. List files in raw/<dataset>/ once  ── nothing? log and exit cleanly
2. Read exactly those keys with spark.read.csv (header validated against schema)
3. Cast timestamp/date columns, cache
4. Validate → split valid vs rejected (PK, required, timestamp, partition, FKs)
5. Deduplicate on PK (window function, deterministic tiebreak)
6. Fail if the rejection rate exceeds the threshold
7. Upsert (MERGE on PK) into the Delta table — idempotent reruns safe
8. Write rejected records to the rejected zone
9. Archive exactly the keys listed in step 1
10. Publish row-count metrics
```

Steps 1 and 9 use the same key list on purpose: a file that lands mid-run stays in the raw zone for the next execution rather than being archived without ever being ingested.

All three jobs share one implementation of this sequence — [`common/etl.py`](glue_jobs/common/etl.py) — driven by a per-dataset `DatasetConfig`. Each job module is a config, optional validation hooks, and a `main()`; nothing executes at import time, which is what makes the pipeline testable without a Glue runtime.

---

## Step Functions Pipeline

```
┌─ Parallel ─────────────┐
│  GlueProducts          │
│  GlueOrders            │──► GlueOrderItems ──► DeltaMaintenance ──► Crawler ──► AthenaValidation ──► Success
└────────────────────────┘         │                   │                 │              │
        │                          │                   │                 │              │
     on fail                    on fail             on fail       failed/timed out   0 rows
        └──────────────────────────┴───────────────────┴─────────────────┴──────────────┘
                                        JobFailed → SNS alert → Fail
```

- Products and Orders share no dependency, so they run concurrently — one Glue cold start saved.
- Order Items waits for both: its foreign keys reference the orders *and* products tables.
- The crawler poll loop reads `LastCrawl.Status`, not just `State` — a crawler returns to `READY` whether it succeeded or failed. It also has an attempt cap, and the state machine has a top-level `TimeoutSeconds`.
- The Athena step asserts a non-zero row count instead of merely running a query.
- Failure alerts serialise the error object with `States.JsonToString` before formatting; passing an object straight to `States.Format` raises at runtime and the alert never leaves.

---

## Automatic Pipeline Trigger

The pipeline triggers when a `_READY` marker file is uploaded to `s3://<bucket>/raw/`:

```
Upload data files → Upload raw/_READY → EventBridge detects it → Step Functions starts
```

```bash
python scripts/upload_data.py --bucket <bucket-name> --type good
```

This uploads all three data files then drops `raw/_READY` as the final signal. **One upload batch = one pipeline execution** — watching per-file prefixes would fire one execution per file.

---

## Development

```bash
python -m venv .venv
source .venv/Scripts/activate          # Windows: .venv\Scripts\activate
pip install -r requirements-dev.txt
```

Run the suite (needs a JDK 17 on PATH for local Spark):

```bash
pytest                                  # everything, with the 70% coverage gate
pytest -m integration                   # just the end-to-end CSV → Delta runs
pytest tests/test_delta_merge.py -v     # the merge regressions
```

Lint exactly as CI does:

```bash
flake8 glue_jobs/ tests/ scripts/
black --check glue_jobs/ tests/ scripts/
terraform fmt -check -recursive
```

The first test run downloads the Delta JARs from Maven Central into `~/.ivy2`; later runs are offline. On Windows, local Spark also needs `winutils.exe` and `HADOOP_HOME` set — running the suite in a Linux container avoids that entirely.

---

## GitHub Actions Workflows

### Automatic (pull requests and pushes to `main`)

**Lakehouse CI/CD** (`ci.yml`)
1. `gitleaks` over history and the working tree
2. `flake8` + `black` over `glue_jobs/`, `tests/` and `scripts/`
3. `pytest` with a 70% coverage floor
4. `terraform fmt -check`, `validate`, `tflint`, `tfsec`
5. Pull request → `terraform plan` with the **read-only** role, posted as a PR comment
6. Push to `main` → `terraform apply` with the **deploy** role (which also uploads the Glue scripts)

The workflow declares a `concurrency` group, so two pushes to `main` queue rather than applying against the same state simultaneously.

### Manual (GitHub Actions → Run workflow)

**Destroy Infrastructure** (`destroy.yml`)
- Type `DESTROY` to confirm
- Empties the S3 bucket, deletes the Athena workgroup, runs `terraform destroy`
- Bootstrap lives in a separate state that destroy never touches, so CI can still authenticate on the next push

**Upload Data & Trigger Pipeline** (`upload-data.yml`)
- Choose `good` — 1,000 products, 500 orders, ~2,500 order items, all clean
- Choose `bad` — the same datasets with deliberate errors injected
- Both upload `raw/_READY` last, triggering exactly one pipeline execution

---

## Data Generation

Both good and bad data are **generated synthetically** — no local data files are needed:

```bash
python scripts/generate_data.py --type good   # clean records, seed=42
python scripts/generate_data.py --type bad    # same + deliberate errors, seed=99

python scripts/upload_data.py --bucket <bucket-name> --type good
python scripts/upload_data.py --bucket <bucket-name> --type bad
```

### Bad Data Test Cases

| Dataset | Bad Records | Rejection Reason |
|---|---|---|
| Products | 5 rows | `null product_id` (PK violation) |
| Orders | 5 rows | `null order_id` (PK violation) |
| Orders | 3 rows | `null user_id` (required field) |
| Orders | 2 rows | Invalid timestamp format |
| Order Items | 5 rows | `null id` (PK violation) |
| Order Items | 3 rows | `order_id = 999999999` (referential integrity — order doesn't exist) |

After the pipeline runs, check `s3://<bucket>/rejected/` for the rejected records. Note that the bad batch's rejection rate is deliberately under the 5% circuit-breaker threshold — a batch dirtier than that is meant to fail the job.

---

## Terraform Infrastructure

| Module | Resources |
|---|---|
| `s3` | Bucket, versioning, encryption, TLS-only policy, public access block, lifecycle (archived → S3-IA → Glacier) |
| `iam` | Glue role, Step Functions role, EventBridge role |
| `bootstrap` | OIDC provider, deploy + plan roles, permissions boundary (separate state) |
| `glue` | 4 Glue jobs, 1 crawler, catalog database, script uploads, rejection-rate alarms |
| `eventbridge` | S3 notification, EventBridge rule watching `_READY`, rule → Step Functions |
| `step_functions` | State machine, CloudWatch log group, X-Ray tracing |
| `sns` | KMS-encrypted alert topic + email subscription |
| `athena` | Workgroup (engine v3), 4 named queries pre-loaded |

State is stored in S3 with `use_lockfile = true`, so concurrent applies block instead of corrupting the state file.

### Bootstrap (first time on a new AWS account only)

The OIDC provider and the two CI roles live in a **separate Terraform state** (`terraform/bootstrap/`) so they survive a full infrastructure destroy — and so CI, which cannot modify them, cannot rewrite its own trust policy. Run once, locally, with credentials that can create IAM:

```bash
cd terraform/bootstrap
cp terraform.tfvars.example terraform.tfvars   # set github_owner / github_repo
terraform init
terraform apply
terraform output          # copy the two role ARNs into GitHub secrets
```

### GitHub Secrets Required

| Secret | Value |
|---|---|
| `AWS_DEPLOY_ROLE_ARN` | `terraform output -raw deploy_role_arn` — assumable only from `main` |
| `AWS_PLAN_ROLE_ARN` | `terraform output -raw plan_role_arn` — read-only, pull requests |
| `ALERT_EMAIL` | Your email for SNS pipeline failure alerts |

The deploy role is scoped to `repo:<owner>/<repo>:ref:refs/heads/main` and carries a least-privilege policy plus a permissions boundary. A wildcard subject (`repo:<owner>/<repo>:*`) would let any branch — from anyone who can push one — assume it.

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
| AWS Step Functions | — | ETL orchestration |
| AWS Glue Data Catalog | — | Metadata layer for Athena |
| Amazon Athena | Engine v3 | SQL analytics on native Delta tables |
| Terraform | ≥1.10 | Infrastructure as Code (S3 native state locking) |
| GitHub Actions | — | CI/CD automation |
| Python | 3.10 | Matches the Glue 4.0 runtime |
