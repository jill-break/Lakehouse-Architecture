output "github_actions_role_arn" {
  description = "Paste this as the AWS_ROLE_ARN GitHub secret"
  value       = aws_iam_role.github_actions.arn
}

output "oidc_provider_arn" {
  value = aws_iam_openid_connect_provider.github.arn
}
