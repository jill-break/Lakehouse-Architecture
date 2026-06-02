# ─── Enable EventBridge notifications on the S3 bucket ───────────────────────
resource "aws_s3_bucket_notification" "raw_zone" {
  bucket      = var.bucket_name
  eventbridge = true
}

# ─── IAM role for EventBridge to invoke Step Functions ───────────────────────
resource "aws_iam_role" "eventbridge" {
  name = "${var.project_name}-${var.environment}-eventbridge-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "events.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "eventbridge" {
  name = "${var.project_name}-${var.environment}-eventbridge-policy"
  role = aws_iam_role.eventbridge.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "states:StartExecution"
      Resource = var.state_machine_arn
    }]
  })
}

# ─── EventBridge rule — fires on any PutObject into raw/ ─────────────────────
resource "aws_cloudwatch_event_rule" "raw_file_uploaded" {
  name        = "${var.project_name}-${var.environment}-raw-file-uploaded"
  description = "Triggers the lakehouse ETL pipeline when a file lands in the S3 raw zone"

  event_pattern = jsonencode({
    source      = ["aws.s3"]
    detail-type = ["Object Created"]
    detail = {
      bucket = { name = [var.bucket_name] }
      object = {
        key = [
          { prefix = "raw/products/" },
          { prefix = "raw/orders/" },
          { prefix = "raw/order_items/" },
        ]
      }
    }
  })
}

# ─── EventBridge target — Step Functions state machine ───────────────────────
resource "aws_cloudwatch_event_target" "step_functions" {
  rule     = aws_cloudwatch_event_rule.raw_file_uploaded.name
  arn      = var.state_machine_arn
  role_arn = aws_iam_role.eventbridge.arn

  # Pass bucket name and SNS topic into the state machine execution input
  input_transformer {
    input_paths = {
      bucket = "$.detail.bucket.name"
      key    = "$.detail.object.key"
    }
    input_template = <<-JSON
      {
        "S3_BUCKET": "<bucket>",
        "S3_KEY": "<key>",
        "SNS_TOPIC_ARN": "${var.sns_topic_arn}"
      }
    JSON
  }
}
