# ─── Glue Data Catalog database ──────────────────────────────────────────────
resource "aws_glue_catalog_database" "lakehouse" {
  name        = replace("${var.project_name}_${var.environment}", "-", "_")
  description = "Glue Data Catalog database for the ecommerce lakehouse"
}

# ─── Upload ETL scripts to S3 ────────────────────────────────────────────────
resource "aws_s3_object" "glue_utils" {
  bucket = var.bucket_name
  key    = "glue-scripts/common.zip"
  source = "${path.root}/../glue_jobs/dist/common.zip"
  etag   = filemd5("${path.root}/../glue_jobs/dist/common.zip")
}

resource "aws_s3_object" "glue_products_script" {
  bucket = var.bucket_name
  key    = "glue-scripts/glue_products.py"
  source = "${path.root}/../glue_jobs/glue_products.py"
  etag   = filemd5("${path.root}/../glue_jobs/glue_products.py")
}

resource "aws_s3_object" "glue_orders_script" {
  bucket = var.bucket_name
  key    = "glue-scripts/glue_orders.py"
  source = "${path.root}/../glue_jobs/glue_orders.py"
  etag   = filemd5("${path.root}/../glue_jobs/glue_orders.py")
}

resource "aws_s3_object" "glue_order_items_script" {
  bucket = var.bucket_name
  key    = "glue-scripts/glue_order_items.py"
  source = "${path.root}/../glue_jobs/glue_order_items.py"
  etag   = filemd5("${path.root}/../glue_jobs/glue_order_items.py")
}

# ─── Common Glue job defaults ─────────────────────────────────────────────────
locals {
  glue_version    = "4.0"
  python_version  = "3"
  delta_connector = "io.delta:delta-core_2.12:2.3.0"

  common_default_args = {
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--enable-auto-scaling"              = "true"
    "--additional-python-modules"        = "boto3,pandas,openpyxl"
    "--datalake-formats"                 = "delta"
    "--extra-py-files"                   = "s3://${var.bucket_name}/glue-scripts/common.zip"
  }
}

# ─── Products Glue job ───────────────────────────────────────────────────────
resource "aws_glue_job" "products" {
  name         = "${var.project_name}-${var.environment}-products"
  role_arn     = var.glue_role_arn
  glue_version = local.glue_version
  worker_type  = var.glue_worker_type
  number_of_workers = var.glue_num_workers
  timeout      = 60
  max_retries  = 1

  command {
    name            = "glueetl"
    script_location = "s3://${var.bucket_name}/glue-scripts/glue_products.py"
    python_version  = local.python_version
  }

  default_arguments = merge(local.common_default_args, {
    "--S3_BUCKET"       = var.bucket_name
    "--RAW_PREFIX"      = "raw/products"
    "--DWH_PREFIX"      = "lakehouse-dwh/products"
    "--ARCHIVED_PREFIX" = "archived/products"
    "--REJECTED_PREFIX" = "rejected/products"
    "--TempDir"         = "s3://${var.bucket_name}/temp/"
  })

  execution_property {
    max_concurrent_runs = 3
  }

  depends_on = [aws_s3_object.glue_products_script, aws_s3_object.glue_utils]
}

# ─── Orders Glue job ─────────────────────────────────────────────────────────
resource "aws_glue_job" "orders" {
  name         = "${var.project_name}-${var.environment}-orders"
  role_arn     = var.glue_role_arn
  glue_version = local.glue_version
  worker_type  = var.glue_worker_type
  number_of_workers = var.glue_num_workers
  timeout      = 60
  max_retries  = 1

  command {
    name            = "glueetl"
    script_location = "s3://${var.bucket_name}/glue-scripts/glue_orders.py"
    python_version  = local.python_version
  }

  default_arguments = merge(local.common_default_args, {
    "--S3_BUCKET"       = var.bucket_name
    "--RAW_PREFIX"      = "raw/orders"
    "--DWH_PREFIX"      = "lakehouse-dwh/orders"
    "--ARCHIVED_PREFIX" = "archived/orders"
    "--REJECTED_PREFIX" = "rejected/orders"
    "--TempDir"         = "s3://${var.bucket_name}/temp/"
  })

  execution_property {
    max_concurrent_runs = 3
  }

  depends_on = [aws_s3_object.glue_orders_script, aws_s3_object.glue_utils]
}

# ─── Order Items Glue job ────────────────────────────────────────────────────
resource "aws_glue_job" "order_items" {
  name         = "${var.project_name}-${var.environment}-order-items"
  role_arn     = var.glue_role_arn
  glue_version = local.glue_version
  worker_type  = var.glue_worker_type
  number_of_workers = var.glue_num_workers
  timeout      = 60
  max_retries  = 1

  command {
    name            = "glueetl"
    script_location = "s3://${var.bucket_name}/glue-scripts/glue_order_items.py"
    python_version  = local.python_version
  }

  default_arguments = merge(local.common_default_args, {
    "--S3_BUCKET"          = var.bucket_name
    "--RAW_PREFIX"         = "raw/order_items"
    "--DWH_PREFIX"         = "lakehouse-dwh/order_items"
    "--ARCHIVED_PREFIX"    = "archived/order_items"
    "--REJECTED_PREFIX"    = "rejected/order_items"
    "--ORDERS_DWH_PREFIX"  = "lakehouse-dwh/orders"
    "--TempDir"            = "s3://${var.bucket_name}/temp/"
  })

  execution_property {
    max_concurrent_runs = 3
  }

  depends_on = [aws_s3_object.glue_order_items_script, aws_s3_object.glue_utils]
}

# ─── Glue Crawler ────────────────────────────────────────────────────────────
resource "aws_glue_crawler" "lakehouse" {
  name          = "${var.project_name}-${var.environment}-crawler"
  role          = var.glue_role_arn
  database_name = aws_glue_catalog_database.lakehouse.name
  description   = "Crawls Delta tables in the lakehouse-dwh zone and updates the Glue Data Catalog"

  delta_target {
    delta_tables = [
      "s3://${var.bucket_name}/lakehouse-dwh/products/",
      "s3://${var.bucket_name}/lakehouse-dwh/orders/",
      "s3://${var.bucket_name}/lakehouse-dwh/order_items/",
    ]
    write_manifest = false
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }

  recrawl_policy {
    recrawl_behavior = "CRAWL_EVERYTHING"
  }

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
      Tables     = { AddOrUpdateBehavior = "MergeNewColumns" }
    }
  })
}

# ─── Manifest generation Glue job ────────────────────────────────────────────
resource "aws_glue_job" "generate_manifests" {
  name              = "${var.project_name}-${var.environment}-generate-manifests"
  role_arn          = var.glue_role_arn
  glue_version      = local.glue_version
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_num_workers
  timeout           = 30
  max_retries       = 0

  command {
    name            = "glueetl"
    script_location = "s3://${var.bucket_name}/glue-scripts/glue_generate_manifests.py"
    python_version  = local.python_version
  }

  default_arguments = merge(local.common_default_args, {
    "--S3_BUCKET" = var.bucket_name
    "--TempDir"   = "s3://${var.bucket_name}/temp/"
  })

  execution_property {
    max_concurrent_runs = 1
  }
}

# ─── CloudWatch Log Group for Glue jobs ──────────────────────────────────────
resource "aws_cloudwatch_log_group" "glue" {
  name              = "/aws-glue/jobs/${var.project_name}-${var.environment}"
  retention_in_days = 14
}
