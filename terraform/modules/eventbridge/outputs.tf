output "rule_name" {
  value = aws_cloudwatch_event_rule.raw_file_uploaded.name
}

output "rule_arn" {
  value = aws_cloudwatch_event_rule.raw_file_uploaded.arn
}
