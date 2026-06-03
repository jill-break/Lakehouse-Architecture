# Lakehouse Architecture Diagram

## Full System Architecture

```mermaid
flowchart TD
    subgraph DEV["Local / CI Environment"]
        GD["scripts/generate_data.py\nGenerates synthetic data\ngood or bad"]
        UL["scripts/upload_raw_data.py\nor upload_bad_data.py"]
        GH["GitHub Actions CI/CD\nci.yml"]
    end

    subgraph S3_RAW["S3 — Raw Zone"]
        R1["raw/products/\nproducts.csv"]
        R2["raw/orders/\norders.xlsx"]
        R3["raw/order_items/\norder_items.xlsx"]
        RDY["raw/_READY\n← trigger marker"]
    end

    subgraph TRIGGER["Event Layer"]
        EB["Amazon EventBridge\nWatches for _READY suffix"]
    end

    subgraph SFN["AWS Step Functions — Sequential Pipeline"]
        direction TB
        J1["1. GlueProducts\nETL Job"]
        J2["2. GlueOrders\nETL Job"]
        J3["3. GlueOrderItems\nETL Job"]
        J4["4. GenerateManifests\nGlue Job"]
        J5["5. Glue Crawler\nUpdate Data Catalog"]
        J6["6. Athena Validation\nSELECT COUNT on all tables"]
        FAIL["JobFailed State\nSNS Alert"]

        J1 -->|success| J2
        J2 -->|success| J3
        J3 -->|success| J4
        J4 -->|success| J5
        J5 -->|success| J6
        J6 -->|success| DONE["✅ PipelineSuccess"]

        J1 -->|failure| FAIL
        J2 -->|failure| FAIL
        J3 -->|failure| FAIL
        J4 -->|failure| FAIL
        J5 -->|failure| FAIL
        J6 -->|failure| FAIL
    end

    subgraph GLUE_ETL["Glue ETL Logic (per job)"]
        direction TB
        E1["Read raw file\nCSV or Excel"]
        E2["Cast schema\nEnforce types"]
        E3["Validate\nNull PK · Timestamp · Ref Integrity"]
        E4["Deduplicate\nWindow fn, keep latest"]
        E5["MERGE into Delta table\nUpsert on PK — idempotent"]
        E6["Write rejected\nto rejected/ zone"]
        E7["Archive source file\nto archived/ zone"]
        E1 --> E2 --> E3 --> E4 --> E5 --> E6 --> E7
    end

    subgraph S3_DWH["S3 — Lakehouse DWH Zone"]
        DT1["lakehouse-dwh/products/\nDelta Table\nPartitioned by department"]
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
        ATHENA["Amazon Athena\nWorkgroup: ecommerce-lakehouse-dev\nEngine v3"]
        Q1["Named Query:\nRevenue by Department"]
        Q2["Named Query:\nRow Count Validation"]
        ATHENA --> Q1 & Q2
    end

    subgraph ALERTS["Alerting"]
        SNS["Amazon SNS\necommerce-lakehouse-dev-pipeline-alerts"]
        EMAIL["Email Alert\ncourage.dei@amalitechtraining.org"]
        SNS --> EMAIL
    end

    subgraph IaC["Infrastructure as Code"]
        TF["Terraform\n7 modules"]
        BS["terraform/bootstrap/\nOIDC Provider + GitHub Actions Role\nSeparate state — never destroyed"]
        TF --> BS
    end

    %% Data flow
    GD --> UL
    UL -->|"Upload data files"| R1 & R2 & R3
    UL -->|"Upload last"| RDY
    GH -->|"terraform apply\n+ upload scripts"| TF

    RDY -->|"Object Created event"| EB
    EB -->|"StartExecution"| SFN

    J1 -.->|"runs"| GLUE_ETL
    J2 -.->|"runs"| GLUE_ETL
    J3 -.->|"runs"| GLUE_ETL

    GLUE_ETL -->|"MERGE"| DT1 & DT2 & DT3
    GLUE_ETL -->|"archive"| ARC
    GLUE_ETL -->|"rejected records"| REJ

    J4 -->|"generate symlink manifests"| DT1 & DT2 & DT3
    J5 -->|"crawl Delta tables"| CATALOG
    J6 -->|"query"| ATHENA
    J6 -->|"results"| ATH

    CATALOG -->|"schema + partitions"| ATHENA
    ATHENA -->|"reads via symlink manifests"| DT1 & DT2 & DT3

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

    subgraph CI["GitHub Actions — Lakehouse CI/CD"]
        LINT["Lint\nflake8 + black"]
        TEST["Unit Tests\npytest 11 tests"]
        TFINIT["Terraform Init\nS3 backend"]
        TFVAL["Terraform Validate"]
        TFPLAN["Terraform Plan\nPR only → post as comment"]
        TFAPPLY["Terraform Apply\nmain + manual only"]
        UPLOAD["Upload Glue Scripts\npackage_glue.py + aws s3 cp"]
    end

    PUSH & MANUAL --> LINT
    PR --> LINT
    LINT --> TEST --> TFINIT --> TFVAL
    TFVAL -->|PR| TFPLAN
    TFVAL -->|main or manual| TFAPPLY --> UPLOAD
```

---

## Destroy & Rebuild Cycle

```mermaid
flowchart TD
    D1["Run Destroy Workflow\nType DESTROY to confirm"]
    D2["Empty S3 bucket\naws s3 rm --recursive"]
    D3["Delete Athena Workgroup\n--recursive-delete-option"]
    D4["terraform destroy\nRemoves all 51 resources"]
    D5["terraform apply bootstrap\nRecreates OIDC Provider + GitHub Actions Role"]
    D6["CI is ready\nBootstrap state untouched"]
    D7["Push to main\nor Run workflow manually"]
    D8["terraform apply\nRebuilds all 51 resources from scratch"]
    D9["Upload Glue Scripts\nto S3"]
    D10["Run Upload Data workflow\nChoose good or bad"]
    D11["Pipeline executes\nEnd-to-end ETL"]

    D1 --> D2 --> D3 --> D4 --> D5 --> D6
    D6 --> D7 --> D8 --> D9 --> D10 --> D11
```

---

## Data Validation & Rejection Flow

```mermaid
flowchart TD
    RAW["Raw file\nCSV or Excel"]
    READ["Read into Spark DataFrame"]
    CAST["Cast to correct types"]

    NullPK{"Null primary key?"}
    NullReq{"Null required field?"}
    BadTS{"Invalid timestamp?"}
    RefInt{"Referential integrity\norder_id exists in orders?"}
    Dedup["Deduplicate\nKeep latest by timestamp"]
    Merge["MERGE into Delta table\nIdempotent upsert"]
    Archive["Archive source file\nto archived/"]

    Rejected["rejected/ zone\nParquet with rejection_reason"]

    RAW --> READ --> CAST --> NullPK
    NullPK -->|"yes"| Rejected
    NullPK -->|"no"| NullReq
    NullReq -->|"yes"| Rejected
    NullReq -->|"no"| BadTS
    BadTS -->|"yes"| Rejected
    BadTS -->|"no"| RefInt
    RefInt -->|"orphan"| Rejected
    RefInt -->|"valid"| Dedup --> Merge --> Archive
```

---

## Terraform Module Dependencies

```mermaid
flowchart TD
    BS["bootstrap/\nOIDC + GitHub Actions Role\nSeparate state"]

    S3["module.s3\nBucket + config"]
    IAM["module.iam\nGlue + SFN + EventBridge roles"]
    SNS["module.sns\nAlert topic"]
    GLUE["module.glue\nJobs + Crawler + Catalog"]
    EB["module.eventbridge\nRule + Target"]
    SFN["module.step_functions\nState machine"]
    ATHENA["module.athena\nWorkgroup + named queries"]

    S3 --> IAM
    S3 --> GLUE
    S3 --> EB
    S3 --> ATHENA
    IAM --> GLUE
    IAM --> SFN
    IAM --> EB
    SNS --> SFN
    SNS --> EB
    GLUE --> SFN
    GLUE --> EB
```
