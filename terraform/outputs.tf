output "s3_bucket_name" {
  description = "Name of the lakehouse S3 bucket"
  value       = module.s3.bucket_name
}

output "s3_bucket_arn" {
  description = "ARN of the lakehouse S3 bucket"
  value       = module.s3.bucket_arn
}

output "glue_role_arn" {
  description = "IAM role ARN used by Glue jobs"
  value       = module.iam.glue_role_arn
}

output "step_functions_role_arn" {
  description = "IAM role ARN used by Step Functions"
  value       = module.iam.step_functions_role_arn
}

output "state_machine_arn" {
  description = "ARN of the Step Functions state machine"
  value       = module.step_functions.state_machine_arn
}

output "glue_products_job_name" {
  value = module.glue.products_job_name
}

output "glue_orders_job_name" {
  value = module.glue.orders_job_name
}

output "glue_order_items_job_name" {
  value = module.glue.order_items_job_name
}

output "eventbridge_rule_name" {
  description = "EventBridge rule that auto-triggers the pipeline on S3 file upload"
  value       = module.eventbridge.rule_name
}

output "sns_topic_arn" {
  description = "ARN of the SNS alert topic"
  value       = module.sns.topic_arn
}

output "athena_workgroup" {
  description = "Name of the Athena workgroup"
  value       = module.athena.workgroup_name
}

output "glue_database_name" {
  description = "Glue Data Catalog database name"
  value       = module.glue.database_name
}
