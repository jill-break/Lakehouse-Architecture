# ─── Glue Data Catalog database ──────────────────────────────────────────────
resource "aws_glue_catalog_database" "lakehouse" {
  name        = replace("${var.project_name}_${var.environment}", "-", "_")
  description = "Glue Data Catalog database for the ecommerce lakehouse"
}

# ─── Package and upload ETL scripts ──────────────────────────────────────────
# Terraform is the single owner of everything under glue-scripts/. CI used to
# upload the same objects after apply, which meant two owners for one artifact,
# perpetual drift in the plan, and one script (the maintenance job) that no
# apply ever created.
locals {
  glue_jobs_dir = "${path.root}/../glue_jobs"
  job_scripts   = fileset("${path.root}/../glue_jobs", "*.py")
  common_files  = fileset("${path.root}/../glue_jobs/common", "*.py")
}

data "archive_file" "common" {
  type        = "zip"
  output_path = "${path.root}/../glue_jobs/dist/common.zip"

  dynamic "source" {
    for_each = local.common_files
    content {
      content  = file("${local.glue_jobs_dir}/common/${source.value}")
      filename = "common/${source.value}"
    }
  }
}

resource "aws_s3_object" "glue_common" {
  bucket = var.bucket_name
  key    = "glue-scripts/common.zip"
  source = data.archive_file.common.output_path
  etag   = data.archive_file.common.output_md5
}

resource "aws_s3_object" "glue_scripts" {
  for_each = local.job_scripts

  bucket = var.bucket_name
  key    = "glue-scripts/${each.value}"
  source = "${local.glue_jobs_dir}/${each.value}"
  etag   = filemd5("${local.glue_jobs_dir}/${each.value}")
}

# ─── Common Glue job defaults ─────────────────────────────────────────────────
locals {
  glue_version   = "4.0"
  python_version = "3"

  common_default_args = {
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--enable-auto-scaling"              = "true"
    "--additional-python-modules"        = "boto3"
    "--datalake-formats"                 = "delta"
    "--extra-py-files"                   = "s3://${var.bucket_name}/glue-scripts/common.zip"
    "--TempDir"                          = "s3://${var.bucket_name}/temp/"
    "--MAX_REJECTION_RATE"               = tostring(var.max_rejection_rate)
  }
}

# ─── Products Glue job ───────────────────────────────────────────────────────
resource "aws_glue_job" "products" {
  name              = "${var.project_name}-${var.environment}-products"
  role_arn          = var.glue_role_arn
  glue_version      = local.glue_version
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_num_workers
  timeout           = 60
  max_retries       = 1

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
  })

  execution_property {
    max_concurrent_runs = 3
  }

  depends_on = [aws_s3_object.glue_scripts, aws_s3_object.glue_common]
}

# ─── Orders Glue job ─────────────────────────────────────────────────────────
resource "aws_glue_job" "orders" {
  name              = "${var.project_name}-${var.environment}-orders"
  role_arn          = var.glue_role_arn
  glue_version      = local.glue_version
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_num_workers
  timeout           = 60
  max_retries       = 1

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
  })

  execution_property {
    max_concurrent_runs = 3
  }

  depends_on = [aws_s3_object.glue_scripts, aws_s3_object.glue_common]
}

# ─── Order Items Glue job ────────────────────────────────────────────────────
resource "aws_glue_job" "order_items" {
  name              = "${var.project_name}-${var.environment}-order-items"
  role_arn          = var.glue_role_arn
  glue_version      = local.glue_version
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_num_workers
  timeout           = 60
  max_retries       = 1

  command {
    name            = "glueetl"
    script_location = "s3://${var.bucket_name}/glue-scripts/glue_order_items.py"
    python_version  = local.python_version
  }

  default_arguments = merge(local.common_default_args, {
    "--S3_BUCKET"           = var.bucket_name
    "--RAW_PREFIX"          = "raw/order_items"
    "--DWH_PREFIX"          = "lakehouse-dwh/order_items"
    "--ARCHIVED_PREFIX"     = "archived/order_items"
    "--REJECTED_PREFIX"     = "rejected/order_items"
    "--ORDERS_DWH_PREFIX"   = "lakehouse-dwh/orders"
    "--PRODUCTS_DWH_PREFIX" = "lakehouse-dwh/products"
  })

  execution_property {
    max_concurrent_runs = 3
  }

  depends_on = [aws_s3_object.glue_scripts, aws_s3_object.glue_common]
}

# ─── Delta maintenance Glue job (OPTIMIZE / Z-ORDER / VACUUM) ────────────────
resource "aws_glue_job" "maintenance" {
  name              = "${var.project_name}-${var.environment}-maintenance"
  role_arn          = var.glue_role_arn
  glue_version      = local.glue_version
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_num_workers
  timeout           = 30
  max_retries       = 0

  command {
    name            = "glueetl"
    script_location = "s3://${var.bucket_name}/glue-scripts/glue_maintenance.py"
    python_version  = local.python_version
  }

  default_arguments = merge(local.common_default_args, {
    "--S3_BUCKET"              = var.bucket_name
    "--VACUUM_RETENTION_HOURS" = tostring(var.vacuum_retention_hours)
    "--GENERATE_MANIFESTS"     = tostring(var.generate_delta_manifests)
  })

  execution_property {
    max_concurrent_runs = 1
  }

  depends_on = [aws_s3_object.glue_scripts, aws_s3_object.glue_common]
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
    # Native Delta tables — Athena engine v3 reads these without manifests.
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

# ─── CloudWatch Log Group for Glue jobs ──────────────────────────────────────
resource "aws_cloudwatch_log_group" "glue" {
  name              = "/aws-glue/jobs/${var.project_name}-${var.environment}"
  retention_in_days = 14
}

# ─── Alarm on the rejection-rate metric the jobs publish ─────────────────────
resource "aws_cloudwatch_metric_alarm" "rejection_rate" {
  for_each = toset(["products", "orders", "order_items"])

  alarm_name          = "${var.project_name}-${var.environment}-${each.value}-rejection-rate"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "RejectionRate"
  namespace           = "Lakehouse/ETL"
  period              = 300
  statistic           = "Maximum"
  threshold           = var.max_rejection_rate * 100
  alarm_description   = "Share of rejected rows for ${each.value} exceeded the acceptable ceiling"
  treat_missing_data  = "notBreaching"

  dimensions = {
    Dataset = each.value
  }

  alarm_actions = var.alarm_sns_topic_arn == null ? [] : [var.alarm_sns_topic_arn]
}
