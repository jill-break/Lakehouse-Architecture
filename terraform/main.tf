terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  backend "s3" {
    bucket = "ecommerce-lakehouse-tfstate-352505432441"
    key    = "lakehouse/terraform.tfstate"
    region = "us-east-1"
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
  bucket_name  = local.bucket_name
  bucket_arn   = module.s3.bucket_arn
  project_name = var.project_name
  environment  = var.environment
  account_id   = local.account_id
  region       = local.region
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
  glue_crawler_name                = module.glue.crawler_name
  glue_generate_manifests_job_name = module.glue.generate_manifests_job_name
  depends_on              = [module.glue, module.iam, module.sns]
}

module "eventbridge" {
  source            = "./modules/eventbridge"
  project_name      = var.project_name
  environment       = var.environment
  bucket_name       = local.bucket_name
  state_machine_arn = module.step_functions.state_machine_arn
  sns_topic_arn     = module.sns.topic_arn
  depends_on        = [module.s3, module.step_functions]
}

module "athena" {
  source             = "./modules/athena"
  bucket_name        = local.bucket_name
  project_name       = var.project_name
  environment        = var.environment
  glue_database_name = module.glue.database_name
  depends_on         = [module.glue]
}
