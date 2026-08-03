terraform {
  # 1.10 is the floor for S3 native state locking (use_lockfile).
  required_version = ">= 1.10.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
    }
  }

  backend "s3" {
    bucket = "ecommerce-lakehouse-tfstate-352505432441"
    key    = "lakehouse/terraform.tfstate"
    region = "eu-west-1"

    # Without a lock, two applies against the same state (two pushes to main in
    # quick succession, or a local apply racing CI) can interleave writes and
    # corrupt it. Recovery means hand-editing state or re-importing everything.
    use_lockfile = true
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "ecommerce-lakehouse"
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}

# ─── Data sources ────────────────────────────────────────────────────────────
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

locals {
  account_id  = data.aws_caller_identity.current.account_id
  region      = data.aws_region.current.name
  bucket_name = "${var.project_name}-${var.environment}-${local.account_id}"
}

# ─── Modules ─────────────────────────────────────────────────────────────────
module "s3" {
  source      = "./modules/s3"
  bucket_name = local.bucket_name
  environment = var.environment
}

module "iam" {
  source       = "./modules/iam"
  bucket_arn   = module.s3.bucket_arn
  project_name = var.project_name
  environment  = var.environment
  account_id   = local.account_id
  region       = local.region
}

module "sns" {
  source       = "./modules/sns"
  project_name = var.project_name
  environment  = var.environment
  alert_email  = var.alert_email
}

module "glue" {
  source                 = "./modules/glue"
  bucket_name            = local.bucket_name
  glue_role_arn          = module.iam.glue_role_arn
  project_name           = var.project_name
  environment            = var.environment
  glue_worker_type       = var.glue_worker_type
  glue_num_workers       = var.glue_num_workers
  max_rejection_rate     = var.max_rejection_rate
  vacuum_retention_hours = var.vacuum_retention_hours
  alarm_sns_topic_arn    = module.sns.topic_arn

  depends_on = [module.s3, module.iam]
}

module "athena" {
  source             = "./modules/athena"
  bucket_name        = local.bucket_name
  project_name       = var.project_name
  environment        = var.environment
  glue_database_name = module.glue.database_name

  depends_on = [module.glue]
}

module "step_functions" {
  source                  = "./modules/step_functions"
  project_name            = var.project_name
  environment             = var.environment
  step_functions_role_arn = module.iam.step_functions_role_arn
  sns_topic_arn           = module.sns.topic_arn

  glue_products_job_name    = module.glue.products_job_name
  glue_orders_job_name      = module.glue.orders_job_name
  glue_order_items_job_name = module.glue.order_items_job_name
  glue_maintenance_job_name = module.glue.maintenance_job_name
  glue_crawler_name         = module.glue.crawler_name
  glue_database_name        = module.glue.database_name
  athena_workgroup_name     = module.athena.workgroup_name

  depends_on = [module.glue, module.iam, module.sns, module.athena]
}

module "eventbridge" {
  source            = "./modules/eventbridge"
  project_name      = var.project_name
  environment       = var.environment
  bucket_name       = local.bucket_name
  state_machine_arn = module.step_functions.state_machine_arn

  depends_on = [module.s3, module.step_functions]
}
