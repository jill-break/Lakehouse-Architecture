# Reusable modules declare their own requirements rather than inheriting the
# root module's by luck — a module consumed from anywhere else would otherwise
# have no stated contract.
terraform {
  required_version = ">= 1.10.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}
