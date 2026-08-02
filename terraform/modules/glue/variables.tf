variable "project_name" { type = string }
variable "environment" { type = string }
variable "bucket_name" { type = string }
variable "glue_role_arn" { type = string }
variable "glue_worker_type" { type = string }
variable "glue_num_workers" { type = number }

variable "max_rejection_rate" {
  description = "Fail an ETL job when more than this share of its rows are rejected"
  type        = number
  default     = 0.05

  validation {
    condition     = var.max_rejection_rate > 0 && var.max_rejection_rate <= 1
    error_message = "max_rejection_rate must be between 0 and 1."
  }
}

variable "vacuum_retention_hours" {
  description = "Delta VACUUM retention window in hours (168 = Delta's 7-day safety floor)"
  type        = number
  default     = 168
}

variable "generate_delta_manifests" {
  description = <<-DESC
    Generate symlink_format_manifest files during maintenance. Athena engine v3
    reads the native Delta tables registered by the crawler, so this is off by
    default; enable it only if a catalog turns out to need manifests.
  DESC
  type        = bool
  default     = false
}

variable "alarm_sns_topic_arn" {
  description = "SNS topic notified when the rejection-rate alarm fires"
  type        = string
  default     = null
}
