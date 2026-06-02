resource "aws_s3_bucket" "lakehouse" {
  bucket        = var.bucket_name
  force_destroy = var.environment != "prod"
}

resource "aws_s3_bucket_versioning" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "lakehouse" {
  bucket                  = aws_s3_bucket.lakehouse.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Lifecycle rule: transition archived files to cheaper storage after 30 days
resource "aws_s3_bucket_lifecycle_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  rule {
    id     = "archive-transition"
    status = "Enabled"

    filter {
      prefix = "archived/"
    }

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 90
      storage_class = "GLACIER"
    }
  }

  rule {
    id     = "rejected-expiry"
    status = "Enabled"

    filter {
      prefix = "rejected/"
    }

    expiration {
      days = 90
    }
  }

  rule {
    id     = "athena-results-expiry"
    status = "Enabled"

    filter {
      prefix = "athena-results/"
    }

    expiration {
      days = 30
    }
  }
}

