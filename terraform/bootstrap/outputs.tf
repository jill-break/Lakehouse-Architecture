output "deploy_role_arn" {
  description = "Paste this as the AWS_DEPLOY_ROLE_ARN GitHub secret (apply, main branch only)"
  value       = aws_iam_role.github_actions_deploy.arn
}

output "plan_role_arn" {
  description = "Paste this as the AWS_PLAN_ROLE_ARN GitHub secret (read-only, pull requests)"
  value       = aws_iam_role.github_actions_plan.arn
}

output "permissions_boundary_arn" {
  value = aws_iam_policy.ci_boundary.arn
}

output "oidc_provider_arn" {
  value = aws_iam_openid_connect_provider.github.arn
}
