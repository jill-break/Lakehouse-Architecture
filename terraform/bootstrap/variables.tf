variable "aws_region" {
  description = "AWS region for the bootstrap resources"
  type        = string
  default     = "us-east-1"
}

variable "github_owner" {
  description = "GitHub account or organisation that owns the repository"
  type        = string
}

variable "github_repo" {
  description = "Repository name (without the owner prefix)"
  type        = string
}

variable "deploy_branch" {
  description = <<-DESC
    The only branch whose workflow runs may assume the deploy role. Anything
    else — another branch, a tag, a fork PR — gets the read-only plan role.
  DESC
  type        = string
  default     = "main"
}

variable "project_name" {
  description = "Resource-name prefix the CI role is allowed to manage"
  type        = string
  default     = "ecommerce-lakehouse"
}

variable "state_bucket" {
  description = "S3 bucket holding the Terraform state files"
  type        = string
  default     = "ecommerce-lakehouse-tfstate-352505432441"
}
