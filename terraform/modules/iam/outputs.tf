output "glue_role_arn" {
  value = aws_iam_role.glue.arn
}

output "step_functions_role_arn" {
  value = aws_iam_role.step_functions.arn
}

output "github_actions_role_arn" {
  value = aws_iam_role.github_actions.arn
}
