variable "project_name" { type = string }
variable "environment"  { type = string }
variable "bucket_name"  { type = string }
variable "bucket_arn"   { type = string }
variable "account_id"   { type = string }
variable "region"       { type = string }
variable "github_repo"  {
  type        = string
  description = "GitHub repo in owner/repo format, e.g. CourageDei/lakehouse-project"
}
