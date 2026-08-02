# Lakehouse Architecture Diagram

## Full System Architecture

```mermaid
flowchart TD
    subgraph DEV["Local / CI Environment"]
        GD["scripts/generate_data.py\nGenerates synthetic data\ngood or bad"]
        UL["scripts/upload_data.py\nUploads batch + _READY"]
        GH["GitHub Actions CI/CD\nci.yml"]
    end

    subgraph S3_RAW["S3 — Raw Zone (CSV only)"]
        R1["raw/products/\nproducts.csv"]
        R2["raw/orders/\norders.csv"]
        R3["raw/order_items/\norder_items.csv"]
        RDY["raw/_READY\n← trigger marker"]
    end

    subgraph TRIGGER["Event Layer"]
        EB["Amazon EventBridge\nWatches for _READY suffix"]
    end

    subgraph SFN["AWS Step Functions — Pipeline"]
        direction TB
        J1["Parallel branch A\nGlueProducts ETL"]
        J2["Parallel branch B\nGlueOrders ETL"]
        J3["GlueOrderItems ETL\nFKs need both branches"]
        J4["DeltaMaintenance\nOPTIMIZE · Z-ORDER · VACUUM"]
        J5["Glue Crawler\nLastCrawl.Status checked\nAttempt-capped poll loop"]
        J6["Athena Validation\nAsserts non-zero row count"]
        FAIL["JobFailed State\nStates.JsonToString → SNS"]

        J1 & J2 -->|both succeed| J3
        J3 -->|success| J4
        J4 -->|success| J5
        J5 -->|SUCCEEDED| J6
        J6 -->|rows > 0| DONE["✅ PipelineSuccess"]

        J1 -->|failure| FAIL
        J2 -->|failure| FAIL
        J3 -->|failure| FAIL
        J4 -->|failure| FAIL
        J5 -->|FAILED or timeout| FAIL
        J6 -->|0 rows| FAIL
    end

    subgraph GLUE_ETL["Glue ETL Logic (shared: common/etl.py)"]
        direction TB
        E0["List raw keys once\nEmpty? log and exit cleanly"]
        E1["spark.read.csv on those keys\nHeader validated against schema"]
        E2["Cast timestamp/date, cache"]
        E3["Validate\nNull PK · required · timestamp\npartition · FKs (anti-join)"]
        E4["Deduplicate\nWindow fn, deterministic tiebreak"]
        E4b{"Rejection rate\nover threshold?"}
        E5["MERGE into Delta on PK\nIdempotent, partition-agnostic"]
        E6["Write rejected\nto rejected/ zone"]
        E7["Archive the SAME keys\nlisted in step 1"]
        E8["Publish CloudWatch metrics"]
        E0 --> E1 --> E2 --> E3 --> E4 --> E4b
        E4b -->|"yes"| EFAIL["Fail the job"]
        E4b -->|"no"| E5 --> E6 --> E7 --> E8
    end

    subgraph S3_DWH["S3 — Lakehouse DWH Zone"]
        DT1["lakehouse-dwh/products/\nDelta Table\nUnpartitioned · Z-ORDER product_id"]
        DT2["lakehouse-dwh/orders/\nDelta Table\nPartitioned by date"]
        DT3["lakehouse-dwh/order_items/\nDelta Table\nPartitioned by date"]
    end

    subgraph S3_OTHER["S3 — Other Zones"]
        ARC["archived/\nRaw files after ingestion"]
        REJ["rejected/\nBad records + rejection_reason"]
        ATH["athena-results/\nQuery output"]
    end

    subgraph CATALOG["AWS Glue Data Catalog"]
        DB["Database: ecommerce_lakehouse_dev"]
        T1["Table: products"]
        T2["Table: orders"]
        T3["Table: order_items"]
        DB --> T1 & T2 & T3
    end

    subgraph ANALYTICS["Analytics Layer"]
        ATHENA["Amazon Athena\nWorkgroup: ecommerce-lakehouse-dev\nEngine v3 — native Delta"]
        Q1["Named Query:\nRevenue by Department"]
        Q2["Named Query:\nRow Count Validation"]
        ATHENA --> Q1 & Q2
    end

    subgraph ALERTS["Alerting"]
        SNS["Amazon SNS\necommerce-lakehouse-dev-pipeline-alerts\nKMS encrypted"]
        CW["CloudWatch Alarm\nRejectionRate per dataset"]
        EMAIL["Email Alert"]
        SNS --> EMAIL
        CW --> SNS
    end

    subgraph IaC["Infrastructure as Code"]
        TF["Terraform\n7 modules · S3 state with lockfile"]
        BS["terraform/bootstrap/\nOIDC + deploy role (main only)\n+ read-only plan role\nSeparate state — applied by hand"]
        TF -.->|"CI cannot modify"| BS
    end

    %% Data flow
    GD --> UL
    UL -->|"Upload data files"| R1 & R2 & R3
    UL -->|"Upload last"| RDY
    GH -->|"terraform apply\n(also uploads Glue scripts)"| TF

    RDY -->|"Object Created event"| EB
    EB -->|"StartExecution"| SFN

    J1 -.->|"runs"| GLUE_ETL
    J2 -.->|"runs"| GLUE_ETL
    J3 -.->|"runs"| GLUE_ETL

    GLUE_ETL -->|"MERGE"| DT1 & DT2 & DT3
    GLUE_ETL -->|"archive"| ARC
    GLUE_ETL -->|"rejected records"| REJ
    E8 --> CW

    J4 -->|"compact + vacuum"| DT1 & DT2 & DT3
    J5 -->|"crawl Delta tables"| CATALOG
    J6 -->|"query"| ATHENA
    J6 -->|"results"| ATH

    CATALOG -->|"schema + partitions"| ATHENA
    ATHENA -->|"reads native Delta"| DT1 & DT2 & DT3

    FAIL --> SNS
```

---

## CI/CD Pipeline Flow

```mermaid
flowchart LR
    subgraph TRIGGERS["Triggers"]
        PUSH["Push to main\nexcluding docs changes"]
        PR["Pull Request\nto main"]
        MANUAL["Manual\nworkflow_dispatch"]
    end

    subgraph CI["GitHub Actions — Lakehouse CI/CD (concurrency-grouped)"]
        SCAN["Secret Scan\ngitleaks: history + tree"]
        LINT["Lint\nflake8 + black\nglue_jobs · tests · scripts"]
        TEST["Tests\npytest · 70% coverage floor"]
        TFCHECK["Terraform Checks\nfmt · validate · tflint · tfsec"]
        TFPLAN["Terraform Plan\nPR only · READ-ONLY role\n→ posted as comment"]
        TFAPPLY["Terraform Apply\nmain only · DEPLOY role\n→ uploads Glue scripts"]
    end

    PUSH & MANUAL --> SCAN
    PR --> SCAN
    SCAN --> LINT
    SCAN --> TEST
    SCAN --> TFCHECK
    LINT & TEST & TFCHECK -->|PR from this repo| TFPLAN
    LINT & TEST & TFCHECK -->|main or manual| TFAPPLY
```

---

## Destroy & Rebuild Cycle

```mermaid
flowchart TD
    D1["Run Destroy Workflow\nType DESTROY to confirm"]
    D2["Empty S3 bucket\naws s3 rm --recursive"]
    D3["Delete Athena Workgroup\n--recursive-delete-option"]
    D4["terraform destroy\nRemoves the main stack"]
    D5["Bootstrap untouched\nSeparate state, outside CI's permissions"]
    D6["CI can still authenticate"]
    D7["Push to main\nor Run workflow manually"]
    D8["terraform apply\nRebuilds everything from scratch"]
    D9["Glue scripts uploaded\nby Terraform itself"]
    D10["Run Upload Data workflow\nChoose good or bad"]
    D11["Pipeline executes\nEnd-to-end ETL"]

    D1 --> D2 --> D3 --> D4 --> D5 --> D6
    D6 --> D7 --> D8 --> D9 --> D10 --> D11
```

---

## Data Validation & Rejection Flow

```mermaid
flowchart TD
    RAW["Raw CSV file"]
    READ["spark.read.csv\nenforceSchema=false"]
    HDR{"Header matches\nthe schema?"}
    CAST["Cast timestamp/date columns"]

    NullPK{"Null primary key?"}
    NullReq{"Null required field?"}
    BadTS{"Invalid timestamp?"}
    NullPart{"Null partition key?"}
    RefInt{"Referential integrity\norder_id → orders\nproduct_id → products"}
    Dedup["Deduplicate\nLatest by timestamp, hash tiebreak"]
    Gate{"Rejection rate\nwithin threshold?"}
    Merge["MERGE into Delta on PK\nIdempotent upsert"]
    Archive["Archive source file\nto archived/"]

    Rejected["rejected/ zone\nParquet with rejection_reason"]
    JobFail["Fail the job\n→ SNS alert"]

    RAW --> READ --> HDR
    HDR -->|"no"| JobFail
    HDR -->|"yes"| CAST --> NullPK
    NullPK -->|"yes"| Rejected
    NullPK -->|"no"| NullReq
    NullReq -->|"yes"| Rejected
    NullReq -->|"no"| BadTS
    BadTS -->|"yes"| Rejected
    BadTS -->|"no"| NullPart
    NullPart -->|"yes"| Rejected
    NullPart -->|"no"| RefInt
    RefInt -->|"orphan"| Rejected
    RefInt -->|"valid"| Dedup --> Gate
    Gate -->|"no"| JobFail
    Gate -->|"yes"| Merge --> Archive
```

---

## Terraform Module Dependencies

```mermaid
flowchart TD
    BS["bootstrap/\nOIDC + deploy role + plan role\n+ permissions boundary\nSeparate state"]

    S3["module.s3\nBucket + config + TLS policy"]
    IAM["module.iam\nGlue + SFN + EventBridge roles"]
    SNS["module.sns\nAlert topic"]
    GLUE["module.glue\nJobs + Crawler + Catalog + Alarms"]
    ATHENA["module.athena\nWorkgroup + named queries"]
    SFN["module.step_functions\nState machine"]
    EB["module.eventbridge\nRule + Target"]

    S3 --> IAM
    S3 --> GLUE
    S3 --> EB
    S3 --> ATHENA
    IAM --> GLUE
    IAM --> SFN
    IAM --> EB
    SNS --> GLUE
    SNS --> SFN
    GLUE --> ATHENA
    GLUE --> SFN
    ATHENA -->|"workgroup + database\ninto the definition"| SFN
    SFN --> EB
```
