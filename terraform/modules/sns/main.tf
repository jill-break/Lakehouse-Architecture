# Accepted risk: encrypted with the AWS-managed SNS key rather than a CMK.
# tfsec wants customer-managed; the alias below costs nothing and needs no key
# policy, which is the right trade for failure notifications.
# tfsec:ignore:AVD-AWS-0136
resource "aws_sns_topic" "alerts" {
  name = "${var.project_name}-${var.environment}-pipeline-alerts"

  # Alert bodies carry Step Functions error causes, which can quote job
  # parameters. Encrypt at rest with the AWS-managed SNS key (no cost, no key
  # policy to maintain).
  kms_master_key_id = "alias/aws/sns"
}

resource "aws_sns_topic_subscription" "email" {
  count     = var.alert_email != "" ? 1 : 0
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}
