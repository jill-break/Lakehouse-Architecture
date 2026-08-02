variable "aws_region" {
  description = "AWS region to deploy resources into"
  type        = string
  default     = "eu-west-1"
}

variable "project_name" {
  description = "Short name used as a prefix for all resource names"
  type        = string
  default     = "ecommerce-lakehouse"
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be one of: dev, staging, prod"
  }
}

variable "alert_email" {
  description = "Email address to receive SNS pipeline failure alerts"
  type        = string
}

variable "glue_worker_type" {
  description = "Glue worker type (G.1X, G.2X, G.025X)"
  type        = string
  default     = "G.1X"
}

variable "glue_num_workers" {
  description = "Number of Glue workers per job"
  type        = number
  default     = 2
}

variable "max_rejection_rate" {
  description = <<-DESC
    Circuit breaker: an ETL job fails if more than this share of its input rows
    is rejected, rather than merging nothing and reporting success.
  DESC
  type        = number
  default     = 0.05
}

variable "vacuum_retention_hours" {
  description = "Delta VACUUM retention window in hours (168 = Delta's 7-day safety floor)"
  type        = number
  default     = 168
}
