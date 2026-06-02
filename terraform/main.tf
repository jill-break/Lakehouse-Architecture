terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Uncomment to store state remotely in S3 (recommended for teams)
  # backend "s3" {
  #   bucket = "your-tfstate-bucket"
  #   key    = "lakehouse/terraform.tfstate"
  #   region = var.aws_region
  # }
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
  bucket_name  = local.bucket_name
  bucket_arn   = module.s3.bucket_arn
  project_name = var.project_name
  environment  = var.environment
  account_id   = local.account_id
  region       = local.region
  github_repo  = var.github_repo
}

module "glue" {
  source            = "./modules/glue"
  bucket_name       = local.bucket_name
  glue_role_arn     = module.iam.glue_role_arn
  project_name      = var.project_name
  environment       = var.environment
  glue_worker_type  = var.glue_worker_type
  glue_num_workers  = var.glue_num_workers
  depends_on        = [module.s3, module.iam]
}

module "sns" {
  source       = "./modules/sns"
  project_name = var.project_name
  environment  = var.environment
  alert_email  = var.alert_email
}

module "step_functions" {
  source                  = "./modules/step_functions"
  project_name            = var.project_name
  environment             = var.environment
  step_functions_role_arn = module.iam.step_functions_role_arn
  bucket_name             = local.bucket_name
  sns_topic_arn           = module.sns.topic_arn
  glue_products_job_name  = module.glue.products_job_name
  glue_orders_job_name    = module.glue.orders_job_name
  glue_order_items_job_name = module.glue.order_items_job_name
  glue_crawler_name       = module.glue.crawler_name
  depends_on              = [module.glue, module.iam, module.sns]
}

module "athena" {
  source             = "./modules/athena"
  bucket_name        = local.bucket_name
  project_name       = var.project_name
  environment        = var.environment
  glue_database_name = module.glue.database_name
  depends_on         = [module.glue]
}
