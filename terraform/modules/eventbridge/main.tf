# ─── Enable EventBridge notifications on the S3 bucket ───────────────────────
resource "aws_s3_bucket_notification" "raw_zone" {
  bucket      = var.bucket_name
  eventbridge = true
}

# ─── SQS queue — batches multiple file uploads into one pipeline trigger ──────
resource "aws_sqs_queue" "trigger" {
  name                       = "${var.project_name}-${var.environment}-pipeline-trigger"
  visibility_timeout_seconds = 300
  # Deduplicate: hold messages for 60s so rapid uploads collapse into one run
  receive_wait_time_seconds  = 20
}

resource "aws_sqs_queue_policy" "trigger" {
  queue_url = aws_sqs_queue.trigger.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "events.amazonaws.com" }
      Action    = "sqs:SendMessage"
      Resource  = aws_sqs_queue.trigger.arn
    }]
  })
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
    Statement = [
      {
        Effect   = "Allow"
        Action   = "states:StartExecution"
        Resource = var.state_machine_arn
      },
      {
        Effect   = "Allow"
        Action   = "sqs:SendMessage"
        Resource = aws_sqs_queue.trigger.arn
      }
    ]
  })
}

# ─── EventBridge rule — fires on any PutObject into raw/ ─────────────────────
resource "aws_cloudwatch_event_rule" "raw_file_uploaded" {
  name        = "${var.project_name}-${var.environment}-raw-file-uploaded"
  description = "Fires when a file lands in the S3 raw zone"

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

# ─── EventBridge → SQS (buffer) ──────────────────────────────────────────────
resource "aws_cloudwatch_event_target" "sqs" {
  rule = aws_cloudwatch_event_rule.raw_file_uploaded.name
  arn  = aws_sqs_queue.trigger.arn

  input_transformer {
    input_paths = {
      bucket = "$.detail.bucket.name"
      key    = "$.detail.object.key"
    }
    input_template = "{\"S3_BUCKET\": \"<bucket>\", \"S3_KEY\": \"<key>\", \"SNS_TOPIC_ARN\": \"${var.sns_topic_arn}\"}"
  }
}

# ─── EventBridge → Step Functions directly (one execution per file prefix) ───
resource "aws_cloudwatch_event_target" "step_functions" {
  rule     = aws_cloudwatch_event_rule.raw_file_uploaded.name
  arn      = var.state_machine_arn
  role_arn = aws_iam_role.eventbridge.arn

  # Use the bucket name as execution name prefix so duplicate fires are obvious
  input_transformer {
    input_paths = {
      bucket = "$.detail.bucket.name"
    }
    input_template = "{\"S3_BUCKET\": \"<bucket>\", \"SNS_TOPIC_ARN\": \"${var.sns_topic_arn}\"}"
  }
}
