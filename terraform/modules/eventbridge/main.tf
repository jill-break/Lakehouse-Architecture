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

# ─── EventBridge rule — fires ONLY when raw/_READY is uploaded ───────────────
# This means the pipeline starts once, after all data files are in place.
resource "aws_cloudwatch_event_rule" "ready_signal" {
  name        = "${var.project_name}-${var.environment}-pipeline-ready"
  description = "Triggers the ETL pipeline when raw/_READY marker file is uploaded"

  event_pattern = jsonencode({
    source      = ["aws.s3"]
    detail-type = ["Object Created"]
    detail = {
      bucket = { name = [var.bucket_name] }
      object = {
        key = [{ suffix = "_READY" }]
      }
    }
  })
}

# ─── EventBridge → Step Functions (single execution) ─────────────────────────
resource "aws_cloudwatch_event_target" "step_functions" {
  rule     = aws_cloudwatch_event_rule.ready_signal.name
  arn      = var.state_machine_arn
  role_arn = aws_iam_role.eventbridge.arn

  # The state machine reads neither of the values this used to inject — the
  # bucket, topic and job names are all templated into the definition at apply
  # time. Passing the triggering key through is genuinely useful for tracing.
  input_transformer {
    input_paths = {
      bucket = "$.detail.bucket.name"
      key    = "$.detail.object.key"
    }
    input_template = "{\"trigger\": {\"bucket\": \"<bucket>\", \"key\": \"<key>\"}}"
  }
}
