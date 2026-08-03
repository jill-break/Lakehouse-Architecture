# Accepted risk: server access logging needs a second bucket whose only
# consumer would be itself. CloudTrail data events are the better answer if
# this ever leaves the lab.
# tfsec:ignore:AVD-AWS-0089
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

# Accepted risk: SSE-S3 rather than a customer-managed KMS key. A CMK would add
# a monthly key charge plus key policies for Glue, Athena and the crawler, and
# the threat it defends against — AWS-side key custody — is not in scope for a
# lab. Revisit before this holds anything regulated.
# tfsec:ignore:AVD-AWS-0132
resource "aws_s3_bucket_server_side_encryption_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Reject any request that did not arrive over TLS.
resource "aws_s3_bucket_policy" "require_tls" {
  bucket = aws_s3_bucket.lakehouse.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DenyInsecureTransport"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:*"
      Resource = [
        aws_s3_bucket.lakehouse.arn,
        "${aws_s3_bucket.lakehouse.arn}/*",
      ]
      Condition = {
        Bool = { "aws:SecureTransport" = "false" }
      }
    }]
  })

  depends_on = [aws_s3_bucket_public_access_block.lakehouse]
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

