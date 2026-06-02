output "rule_name" {
  value = aws_cloudwatch_event_rule.ready_signal.name
}

output "rule_arn" {
  value = aws_cloudwatch_event_rule.ready_signal.arn
}
