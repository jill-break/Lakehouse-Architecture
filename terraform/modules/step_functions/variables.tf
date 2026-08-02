variable "project_name" { type = string }
variable "environment" { type = string }
variable "step_functions_role_arn" { type = string }
variable "sns_topic_arn" { type = string }

variable "glue_products_job_name" { type = string }
variable "glue_orders_job_name" { type = string }
variable "glue_order_items_job_name" { type = string }
variable "glue_maintenance_job_name" { type = string }
variable "glue_crawler_name" { type = string }

# Templated rather than hardcoded so a non-dev environment does not end up
# validating against the dev database and workgroup.
variable "glue_database_name" { type = string }
variable "athena_workgroup_name" { type = string }
