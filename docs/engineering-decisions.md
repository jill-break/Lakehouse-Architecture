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
12. [Second Pass: Correctness, Security and Test Coverage](#12-second-pass-correctness-security-and-test-coverage)

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
- **Destroy** — empties S3, deletes Athena workgroup, destroys all infra (bootstrap is a separate state and is left alone)
- **Upload good data** — generates 1,000 products / 500 orders / ~2,500 order items synthetically, uploads, fires pipeline
- **Upload bad data** — same with deliberate violations injected, fires pipeline, rejected records land in `s3://.../rejected/`

### ETL Pipeline
- Products and Orders run concurrently; Order Items follows both (see §12)
- Schema validation, null PK checks, timestamp validation, referential integrity on both of order_items' foreign keys
- Deduplication using window functions (latest by timestamp, deterministic tiebreak)
- Delta Lake MERGE (upsert) on the primary key — idempotent, reruns are safe
- Raw files archived after successful ingestion
- Rejected records written to `rejected/` zone with `rejection_reason` column
- Maintenance job compacts, Z-ORDERs and vacuums the tables after each load
- Glue crawler updates Data Catalog after each run, and its outcome is checked
- Athena validation query runs as the final pipeline step and asserts a non-zero count

### Querying
- All three Delta tables queryable in Athena
- `ecommerce_lakehouse_dev` database with `products`, `orders`, `order_items` tables
- 4 named queries pre-loaded in the Athena workgroup

---

## 11. Known Limitations

| Limitation | Detail |
|---|---|
| Multiple `_READY` uploads | If `_READY` is uploaded twice in quick succession, two pipeline executions start. The second finds empty `raw/` folders and now genuinely exits cleanly (see §12) — but it still fires. |
| Bootstrap still needs one local run | On a completely new AWS account, `terraform apply` must be run once in `terraform/bootstrap/` locally before CI can function. This is now deliberate: CI has no permission to modify its own roles. |
| Glue job cold start | Each Glue job takes ~60-90 seconds to start a Spark context before doing any actual work. Running Products and Orders concurrently removed one of those; the full pipeline is still cold-start dominated. |
| No incremental processing | Every pipeline run processes all files in `raw/`. There is no watermark or checkpoint — the idempotent Delta MERGE handles reruns safely but doesn't skip already-processed records. |
| Athena table re-registration | After a full destroy and rebuild, the first Athena query fails until the crawler has run once. |
| Manifests unverified | The maintenance job can still emit symlink manifests, but they are off by default on the assumption that Athena engine v3 reads the crawler's native Delta tables. That assumption has not been re-tested against a live catalog. |
| Five tfsec findings waived | Every one is "use a customer-managed KMS key" (S3, SNS, two log groups) or "enable S3 access logging". Each is suppressed inline with the reasoning next to the resource, so anything *new* still fails the build. Two are rated HIGH by tfsec; both are about AWS-managed vs customer-managed key custody, not about data being unencrypted. Revisit before this holds anything regulated. |

---

## 12. Second Pass: Correctness, Security and Test Coverage

A senior review of the first cut found five critical and seven high-severity defects. Two of them corrupted data silently, which is the failure mode that matters most here — the pipeline stays green while the numbers drift. This section records what was wrong and what changed.

### Decision: the partition column does not belong in the merge predicate

**Original design:** `MERGE ON target.date = source.date AND target.order_id = source.order_id`, on the reasoning that naming the partition column lets Delta prune files.

**What failed:** two things, both silent.

`NULL = NULL` is UNKNOWN in SQL, not TRUE. `date` is derived with `to_date()`, which yields null for anything unparseable, and nothing rejected null partition values. So a row with a null date could never match its own target row: `whenNotMatchedInsertAll()` fired on *every* run and the table grew without bound — worst for exactly the dirty records the pipeline exists to catch.

Separately, when a record legitimately changes partition — an order date corrected, a product reclassified — the target row lives in a different partition, the predicate can't see it, and the merge inserts a second row with the same primary key. Every aggregate then over-counts.

**Fix:** merge on the primary key alone, reject null partition values before the merge as defence in depth, and cover both cases with regression tests (`test_null_partition_value_does_not_duplicate`, `test_partition_value_change_updates_rather_than_duplicates`). Pruning is given up deliberately: a correct scan beats a fast wrong answer.

### Decision: read CSV with Spark instead of Excel with pandas

**Original design:** orders and order_items were `.xlsx`, read with `pandas.read_excel` on the driver and handed to `spark.createDataFrame`.

**What failed:** nothing, at 2,768 rows — which is why it survived. But every byte went through the driver's heap and was parsed single-threaded by openpyxl (roughly 10× the file size in RAM), so `glue_num_workers` had no effect on the read at all. It was a single-node job wearing a Spark costume, and it would OOM the driver at real scale.

The union across files was positional (`DataFrame.union`), so two files with the same columns in a different order would have loaded `user_id` values into `order_id` — both integers, so nothing would have complained.

**Fix:** the generator writes CSV (the brief specifies CSV, and this project controls its own input format), and the jobs read it with `spark.read.csv` across all keys in one call. `enforceSchema=false` makes Spark check each file's header against the declared schema and refuse a mismatched one, which is a stronger guarantee than `unionByName` — a reordered or renamed column now fails the run and names the file.

### Decision: list once, read that list, archive that list

**What failed:** the products job read with a prefix glob and then archived whatever a *later* `list_objects_v2` call returned. A file uploaded between those two moments was copied to `archived/` and deleted from `raw/` without ever being in the DataFrame that got merged. Silent data loss, discoverable only by scanning the archive.

The same job also had no empty-prefix guard, so the second run of an unchanged pipeline — products archives its own sources — raised `AnalysisException`, caught to `JobFailed` and paged on-call. §10 previously claimed this case "exits cleanly"; it did not.

**Fix:** `list_raw_keys()` in `common/utils.py`, called once per run. The same key list feeds the read and the archive, and an empty list logs and returns instead of raising. Both behaviours are covered by moto tests.

### Decision: referential integrity via anti-join, and both foreign keys

**What failed:** the check collected every distinct `order_id` in the warehouse into a Python list on the driver and embedded it in the plan as a literal `IN (...)`. Fine at 500 orders; a multi-hundred-MB driver allocation and an uncompilable plan at 10M. And `product_id → products`, documented as a foreign key in the README, was never enforced at all — an item referencing a nonexistent product flowed into the warehouse and then silently vanished from the revenue-by-department join.

**Fix:** `reject_orphans()` uses `left_anti`/`left_semi` joins — nothing touches the driver — and order_items now checks both foreign keys.

### Decision: sequential was over-correction; parallel where the dependencies allow

§5 abandoned the `Parallel` state for two reasons. The first (EventBridge firing one execution per file) was solved by the `_READY` sentinel and no longer applies. The second — order_items reads the orders table — is real, and adding the `product_id` foreign key made it *more* constraining, not less.

**Fix:** Products and Orders run concurrently in a `Parallel` state; Order Items follows both. That keeps every dependency intact and removes one Glue cold start.

### Decision: the failure alert has to actually send

**What failed:** every `Catch` writes to `ResultPath: "$.error"`, so `$.error` is an object. `States.Format` only interpolates strings, numbers and booleans — passing an object raises `States.Runtime`. `JobFailed` had no `Catch` of its own, so the execution aborted there and the SNS publish never happened. The one requirement whose entire purpose is observability didn't work.

**Fix:** `States.JsonToString($.error)` before formatting, plus a `Catch` on the publish itself so a failed alert still reaches the `Fail` state.

The crawler poll had the matching problem: a Glue crawler returns to `State: READY` whether it succeeded or failed, and only `State` was inspected, so a failed crawl handed off to Athena against a stale catalog. The loop also had no iteration cap. Both are fixed, and the state machine has a top-level `TimeoutSeconds`.

### Decision: least privilege, and CI cannot modify CI

**What failed:** the GitHub Actions role had `AdministratorAccess` and a trust policy of `repo:owner/repo:*` — every branch, tag and environment. Anyone who could push a branch had admin over the account. §8 justified this as acceptable for a lab; it is the exact shortcut behind a long list of real breaches, and it would fail any security review.

**Fix:** two roles. A deploy role trusted only from `ref:refs/heads/main`, carrying a policy scoped to project-prefixed resource ARNs, and a read-only plan role for pull requests. Both carry a permissions boundary that caps them regardless of what the inline policy later says, and the deploy policy explicitly denies `iam:*` on the CI roles themselves — CI cannot widen its own trust policy. Bootstrap is applied by a human, and the destroy workflow no longer re-applies it.

The S3 backend also had no lock (`use_lockfile = true` now, on Terraform ≥1.10) and the workflows had no `concurrency` group, so two pushes to `main` could `terraform apply -auto-approve` against the same state simultaneously.

### Decision: fail on bad data instead of quietly merging nothing

Rejected rows were counted and never acted on. A malformed upstream export where every row failed validation produced an empty merge, a successful pipeline, a green Athena check and no alert. Each job now fails when the rejection rate exceeds a configurable ceiling (5% by default), publishes `RowsIngested` / `RowsRejected` / `RejectionRate` to CloudWatch, and there is an alarm on the rate. Logging is structured JSON, so CloudWatch Logs Insights can filter by level and query individual fields.

### Decision: one implementation of the ETL sequence

The list → read → validate → dedup → merge → reject → archive sequence was copy-pasted across three jobs with small variations, and every job executed at import time — `SparkContext()` on line 48 — which is why the test suite could only reach three tiny helpers. The sequence now lives once in `common/etl.py`, driven by a per-dataset `DatasetConfig`; each job file is a config, optional validation hooks and a `main()`. The three data generators collapsed into one, dead code in `utils.py` was deleted, and the stale `state_machine.json` (which had already drifted to a wrong job name and a missing state) is gone — the `.tpl` is the only definition.

Terraform is now the single owner of everything under `glue-scripts/`: it builds `common.zip` with `archive_file` at plan time and uploads every job script, so CI no longer uploads the same objects after apply.

### What the tests cover now

72 tests, 86% line coverage, with a 70% floor enforced in CI. The three merge regressions the review called non-negotiable are there, along with an end-to-end run from CSV on disk through validation to a queried Delta table, moto-backed tests for the S3 listing/archiving race, and coverage of the anti-join foreign keys, the dedup tiebreak, the rejection circuit breaker and the maintenance job.

Two gaps remain honest ones: `run_dataset_etl`'s S3 read path can't be exercised locally without an S3-backed Spark, and the Glue bootstrap blocks are excluded from coverage because they instantiate a `SparkContext` and a Glue `Job` that only exist inside the Glue runtime.
