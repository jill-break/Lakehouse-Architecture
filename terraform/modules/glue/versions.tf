# Reusable modules declare their own requirements rather than inheriting the
# root module's by luck — a module consumed from anywhere else would otherwise
# have no stated contract. This one also builds common.zip with archive_file.
terraform {
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
}
