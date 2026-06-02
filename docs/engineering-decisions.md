# Engineering Decisions & Project Retrospective

This document records the architectural decisions made during the build of the ecommerce lakehouse pipeline — what we chose, why we chose it, what failed, what we changed, and what currently works.

---

## Table of Contents

1. [Infrastructure Provisioning](#1-infrastructure-provisioning)
2. [CI/CD Pipeline](#2-cicd-pipeline)
3. [Glue ETL Jobs](#3-glue-etl-jobs)
4. [Delta Lake & Athena Integration](#4-delta-lake--athena-integration)
5. [Step Functions Orchestration](#5-step-functions-orchestration)
6. [Pipeline Trigger Strategy](#6-pipeline-trigger-trigger-strategy)
7. [Data Generation Strategy](#7-data-generation-strategy)
8. [IAM & Authentication](#8-iam--authentication)
9. [Bootstrap Problem & Solution](#9-bootstrap-problem--solution)
10. [What Currently Works](#10-what-currently-works)
11. [Known Limitations](#11-known-limitations)

---

## 1. Infrastructure Provisioning

### Decision: Terraform over CloudFormation

**Chose:** Terraform with modular structure (`modules/s3`, `modules/iam`, `modules/glue`, etc.)

**Why:** Terraform's HCL is more readable than CloudFormation JSON/YAML, has better state management, and the module pattern keeps each concern isolated and reusable. The AWS provider for Terraform also has better coverage of newer resource types like EventBridge S3 notifications.

**Module structure:**

| Module | Responsibility |
|---|---|
| `s3` | Bucket, versioning, SSE encryption, public access block, lifecycle rules |
| `iam` | Glue role, Step Functions role, EventBridge role |
| `glue` | 4 Glue jobs, crawler, Glue Data Catalog database, script uploads |
| `eventbridge` | S3 bucket notification, EventBridge rule, Step Functions target |
| `step_functions` | State machine definition (via templatefile), CloudWatch logs, X-Ray |
| `sns` | Alert topic, email subscription |
| `athena` | Workgroup (engine v3), 4 pre-loaded named queries |
| `bootstrap` | OIDC provider + GitHub Actions role (separate state — never destroyed) |

### Decision: Remote S3 Backend for Terraform State

**Chose:** S3 backend (`ecommerce-lakehouse-tfstate-<account-id>`) with a separate key for bootstrap (`bootstrap/terraform.tfstate`) and main infra (`lakehouse/terraform.tfstate`).

**Why:** Without a remote backend, CI and local runs each have their own state file and don't know about each other's resources. This caused the first major failure — CI tried to create resources that already existed locally, producing `EntityAlreadyExists` errors on every apply.

**What failed first:** The state file was local (default). When CI ran `terraform apply` it had no state, tried to create all resources, and hit 409 conflicts on IAM roles, S3 bucket, and OIDC provider.

**Fix:** Migrated state to S3 using `terraform init -migrate-state`. All runs (local and CI) now share the same state.

---

## 2. CI/CD Pipeline

### Decision: GitHub Actions with OIDC (no static keys)

**Chose:** OIDC authentication via `aws-actions/configure-aws-credentials@v4` with `role-to-assume`.

**Why:** Static AWS keys in GitHub secrets are a security risk — they don't rotate, can be leaked in logs, and give permanent access. OIDC issues short-lived tokens scoped to specific repo and branch, which expire automatically.

**What failed:** The first CI attempt used static keys (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`) as a shortcut during debugging. We replaced these with OIDC once the infrastructure stabilised.

### Decision: Single Terraform job (plan + apply together)

**Original approach:** Two separate jobs — `terraform-plan` (saves artifact) → `terraform-apply` (downloads artifact and applies).

**What failed:** The plan artifact is tied to the Terraform provider binary hash on the runner that created it. A different runner downloading the artifact couldn't execute it — the binary hash mismatch caused `Cannot apply incomplete plan` errors. Also, the `2>&1 | tee plan.txt` pipe masked Terraform's exit code so a failed plan looked like success.

**Fix:** Merged plan and apply into a single job on the same runner. On PRs it runs plan only and posts the output as a PR comment. On push to main it runs `terraform apply -auto-approve` directly.

### Decision: `paths-ignore` to skip CI on docs changes

**Chose:** Skip CI when only `README.md`, `docs/**`, `*.pdf`, `.gitignore`, or `Data/**` change.

**Why:** No point running a 3-minute Terraform apply because of a README typo fix.

**Side effect discovered:** Empty commits (used to manually trigger a rebuild after destroy) also get skipped because they match no paths. Fixed by adding `workflow_dispatch` to the CI trigger so it can always be run manually from the GitHub Actions UI.

### Decision: Upload Glue scripts as a CI step

**Chose:** After `terraform apply`, CI packages `common/` into `common.zip` and uploads all Glue scripts to S3 using `aws s3 cp`.

**Why:** Terraform manages the Glue job definitions (which script location to use) but the actual script content changes independently. If we only relied on Terraform's `etag`-based S3 object resource, a script change would need a Terraform apply to propagate. Uploading directly in CI means any script change takes effect immediately on the next push.

**What failed:** The Python packaging step used an inline `python -c "..."` multiline string inside the YAML `run:` block. YAML doesn't support multiline strings as implicit map keys — this broke the workflow parse with `Implicit keys need to be on a single line` errors.

**Fix:** Extracted the packaging logic into `scripts/package_glue.py` and called it as `python scripts/package_glue.py` in the workflow — a single clean line.

---

## 3. Glue ETL Jobs

### Decision: PySpark with Delta Lake via `--datalake-formats delta`

**Chose:** Glue 4.0 jobs with `--datalake-formats delta` in the default arguments instead of manually configuring `spark.sql.extensions`.

**What failed:** The first version of each job manually set:
```python
spark.conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
spark.conf.set("spark.sql.catalog.spark_catalog", "...")
```
This caused `AnalysisException: Cannot modify the value of a static config` because Glue 4.0 with `--datalake-formats delta` already sets these configs at startup. Setting them again fails.

**Fix:** Removed both `spark.conf.set` lines from all three ETL scripts. The `--datalake-formats delta` argument handles the Delta configuration automatically.

### Decision: Package `common/` as a zip for `--extra-py-files`

**What failed first:** Passed `utils.py` directly as `--extra-py-files s3://.../glue-scripts/common/utils.py`. This worked for a single file but the `from common.utils import ...` import failed with `ModuleNotFoundError: No module named 'common'` because the file wasn't in a package structure.

**Fix:** Zipped `common/__init__.py` and `common/utils.py` into `common.zip` and passed the zip as `--extra-py-files`. Glue extracts the zip and preserves the `common/` package structure, making `from common.utils import ...` work correctly.

**Second failure:** `common/__init__.py` is an empty file — git doesn't track empty files by default so it wasn't committed. CI couldn't find it and the zip creation failed with `FileNotFoundError`.

**Fix:** Force-added the empty file with `git add -f glue_jobs/common/__init__.py` and updated the packaging script to gracefully handle missing files with `z.writestr(arcname, "")`.

### Decision: `os._exit(0)` for empty raw zone early exit

**What failed:** When no raw files were found, the jobs called `job.commit()` then continued executing — falling through to `dfs[0]` which caused `IndexError: list index out of range` because `dfs` was empty.

**First fix attempt:** Added `sys.exit(0)` — but Glue intercepts `SystemExit` and reports the job as `FAILED` even with exit code 0. This caused Step Functions to report failures on perfectly successful runs.

**Final fix:** Used `os._exit(0)` which bypasses Glue's exception handler and exits the process with code 0 directly. Glue correctly reports the job as `SUCCEEDED`.

### Decision: `ExpectedBucketOwner` removed from `s3.get_object` calls

**What failed:** A linter automatically added `ExpectedBucketOwner=args.get("AWS_ACCOUNT_ID", "")` to `s3.get_object()` calls. When `AWS_ACCOUNT_ID` isn't in the job args, this passes an empty string which AWS rejects with `InvalidBucketOwnerAWSAccountID`.

**Fix:** Removed `ExpectedBucketOwner` entirely. It's an optional security parameter not needed for this setup.

---

## 4. Delta Lake & Athena Integration

### Decision: Symlink manifest approach for Athena

**Why:** Athena cannot natively read Delta Lake `_delta_log/` transaction logs. It needs either symlink manifests (Parquet file paths listed in `_symlink_format_manifest/` directories) or native Delta table registration. We chose symlink manifests because they work with standard Athena external tables and the Glue crawler can discover them automatically.

**What failed:** The Glue crawler registered tables pointing at `_symlink_format_manifest/` paths, but the manifests didn't exist yet — they have to be explicitly generated by calling `delta_table.generate("symlink_format_manifest")` on each Delta table. Athena queries returned 0 rows even though data existed in the Delta tables.

**Fix:** Added a dedicated `glue_generate_manifests.py` Glue job that runs after all three ETL jobs and generates manifests for all three tables. This job is step 4 in the Step Functions pipeline.

### Decision: Athena table creation — symlink external tables with `MSCK REPAIR`

**What failed:** The Glue crawler registered tables with partition information but Athena couldn't see the data until `MSCK REPAIR TABLE` was run to load partitions from the symlink manifest directories.

**Current approach:** The crawler runs after manifest generation. The first time, tables need `MSCK REPAIR TABLE` run manually in Athena to load partitions. On subsequent pipeline runs the crawler updates partition metadata automatically.

### Decision: Athena query in state machine uses fully-qualified table names

**What failed:** The AthenaValidation state in Step Functions used:
```sql
SELECT COUNT(*) FROM products
```
Without a database context, Athena defaulted to the `default` database and returned `TABLE_NOT_FOUND: Table 'awsdatacatalog.default.products' does not exist`.

**Fix:** Added `QueryExecutionContext` to the state definition and used fully-qualified names:
```sql
SELECT COUNT(*) FROM ecommerce_lakehouse_dev.products
```

---

## 5. Step Functions Orchestration

### Decision: Sequential execution instead of parallel

**Original design:** All three Glue jobs ran in parallel using a `Parallel` state with three branches.

**What failed:** When EventBridge fired three separate Step Functions executions (one per uploaded file), each execution tried to run all three Glue jobs simultaneously. With `max_concurrent_runs = 1` on each Glue job, this caused `ConcurrentRunsExceededException` across the board.

Even with `max_concurrent_runs = 3` to allow retries, the parallel design had a second problem — order_items runs a referential integrity check against the orders Delta table. If orders and order_items ran in parallel, the orders table might not exist yet when order_items tries to read it.

**Fix:** Changed to sequential execution: `GlueProducts → GlueOrders → GlueOrderItems → GenerateManifests → Crawler → AthenaValidation`. Each step waits for the previous to complete. This also naturally enforces referential integrity ordering.

### Decision: SNS alerts on failure

**Chose:** Each failure state publishes to an SNS topic which delivers email alerts.

**What failed:** The `JobFailed` state tried to reference `$.SNS_TOPIC_ARN` from the execution input, but when Step Functions auto-triggered from EventBridge the input format was different and the path resolution failed.

**Fix:** Hardcoded the SNS topic ARN directly in the state machine template using the Terraform `templatefile()` interpolation rather than reading from execution input.

---

## 6. Pipeline Trigger Strategy

### Decision 1 (failed): EventBridge watching all raw/ prefixes

**Original approach:** EventBridge rule watched for `Object Created` events on `raw/products/`, `raw/orders/`, and `raw/order_items/` prefixes.

**What failed:** Uploading 3 files fired 3 separate EventBridge events → 3 Step Functions executions → 3 sets of Glue jobs all running simultaneously → `ConcurrentRunsExceededException` on every job.

### Decision 2 (current): `_READY` marker file as single trigger

**Chose:** EventBridge only watches for files with the suffix `_READY`. The upload script uploads all data files first, then uploads an empty `raw/_READY` file as the final signal.

**How it works:**
```
Upload products.csv        → EventBridge ignores it
Upload orders.xlsx         → EventBridge ignores it
Upload order_items.xlsx    → EventBridge ignores it
Upload raw/_READY          → EventBridge fires ONCE → one pipeline execution
```

**Result:** One upload batch = one pipeline execution. No concurrency conflicts.

---

## 7. Data Generation Strategy

### Decision: Synthetic data generation instead of committed data files

**Original approach:** Local `Data/` folder with real Excel/CSV files, uploaded via `upload_raw_data.py`.

**Problems:**
- `Data/` was git-ignored so CI couldn't find the files
- Real data files shouldn't live in source control (size, sensitivity)
- Bad data testing required manually corrupting real files

**Fix:** Both good and bad data are generated synthetically using `scripts/generate_data.py` with fixed random seeds (`seed=42` for good, `seed=99` for bad). This means:
- No files needed in the repo
- CI and local runs produce identical data
- Bad data injects specific violations (null PKs, invalid timestamps, orphan foreign keys) deterministically
- Reproducible across all environments

---

## 8. IAM & Authentication

### Decision: GitHub Actions role permissions — AdministratorAccess

**Original approach:** Granular IAM policy listing specific actions for each service (S3, Glue, SNS, Athena, CloudWatch, Step Functions, IAM).

**What failed:** Every Terraform `apply` in CI discovered a missing permission — `s3:GetBucketPolicy`, `SNS:ListTagsForResource`, `SNS:GetSubscriptionAttributes`, `s3:GetBucketLocation` — requiring a new commit to fix each one. This caused 5+ CI failures in a row.

**Fix:** Attached `AdministratorAccess` managed policy to the GitHub Actions role. Since this is scoped to a specific assumed role (not a user), and the role itself is only assumable from the specific GitHub repo via OIDC conditions, the blast radius is acceptable for a lab/dev environment.

### Decision: Glue role S3 permissions — added `CopyObject`

**What failed:** The archive step in each Glue job calls `s3.copy_object()` to move raw files to the `archived/` prefix. The initial Glue IAM policy didn't include `s3:CopyObject`, causing `AccessDenied` on every archive attempt.

**Fix:** Added `s3:CopyObject`, `s3:GetObjectAcl`, and `s3:PutObjectAcl` to the Glue role's S3 policy.

---

## 9. Bootstrap Problem & Solution

### The Problem

The GitHub Actions IAM role is the resource CI uses to authenticate to AWS. But it's also a resource managed by Terraform. After a full `terraform destroy`:
- The role no longer exists
- CI can't authenticate
- CI can't run `terraform apply` to recreate the role
- Deadlock

### First Workaround (manual)

After each destroy, manually ran:
```bash
aws iam create-open-id-connect-provider ...
aws iam create-role ...
aws iam attach-role-policy ...
terraform import ...
```

This was fragile, required remembering exact commands, and broke the "fully automated" goal.

### Final Solution: Separate Bootstrap Terraform Module

Created `terraform/bootstrap/` with its own S3 state key (`bootstrap/terraform.tfstate`). This configuration manages only:
- `aws_iam_openid_connect_provider.github`
- `aws_iam_role.github_actions`
- `aws_iam_role_policy_attachment.github_actions_admin`

The destroy workflow now:
1. Runs `terraform destroy` on the main state (everything except bootstrap)
2. Immediately runs `terraform apply` on the bootstrap state (ensures OIDC + role exist)
3. CI is ready to authenticate on the very next push — no manual intervention needed

**Bootstrap is run once locally on a brand new AWS account. After that, it's fully automated.**

---

## 10. What Currently Works

### Infrastructure
- Full Terraform provisioning of all AWS resources across 7 modules
- Remote S3 state shared between local and CI
- Separate bootstrap state that survives full destroy/recreate cycles

### CI/CD
- Automatic on push to `main` (skips docs-only changes)
- Manual trigger via `workflow_dispatch`
- Lint (`flake8`), format check (`black`), unit tests (`pytest`) on every run
- `terraform apply` provisions/updates infrastructure
- Glue scripts automatically packaged and uploaded to S3 after apply

### Manual Workflows
- **Destroy** — empties S3, deletes Athena workgroup, destroys all infra, re-applies bootstrap
- **Upload good data** — generates 1,000 products / 500 orders / ~2,500 order items synthetically, uploads, fires pipeline
- **Upload bad data** — same with deliberate violations injected, fires pipeline, rejected records land in `s3://.../rejected/`

### ETL Pipeline
- Products, Orders, Order Items Glue jobs run sequentially
- Schema validation, null PK checks, timestamp validation, referential integrity on order_items
- Deduplication using window functions (latest by timestamp wins)
- Delta Lake MERGE (upsert) — idempotent, reruns are safe
- Raw files archived after successful ingestion
- Rejected records written to `rejected/` zone with `rejection_reason` column
- Symlink manifests generated for Athena compatibility
- Glue crawler updates Data Catalog after each run
- Athena validation query runs as the final pipeline step

### Querying
- All three Delta tables queryable in Athena
- `ecommerce_lakehouse_dev` database with `products`, `orders`, `order_items` tables
- 4 named queries pre-loaded in the Athena workgroup

---

## 11. Known Limitations

| Limitation | Detail |
|---|---|
| Athena partition discovery | `MSCK REPAIR TABLE` must be run once manually after the first pipeline run to load partitions. Subsequent runs handled by the crawler. |
| Multiple `_READY` uploads | If `_READY` is uploaded twice in quick succession, two pipeline executions start. The second will find empty `raw/` folders (archived by first run) and exit cleanly, but it still fires. |
| Bootstrap still needs one local run | On a completely new AWS account, `terraform apply` must be run once in `terraform/bootstrap/` locally before CI can function. |
| Glue job cold start | Each Glue job takes ~60-90 seconds to start a Spark context before doing any actual work. The full pipeline takes 10-15 minutes end to end as a result. |
| No incremental processing | Every pipeline run processes all files in `raw/`. There is no watermark or checkpoint — the idempotent Delta MERGE handles reruns safely but doesn't skip already-processed records. |
| Athena table re-registration | After a full destroy and rebuild, Athena tables must be re-registered (crawler handles this on first pipeline run, but the first Athena query after a rebuild will fail until the crawler runs). |
