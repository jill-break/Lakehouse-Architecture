terraform {
  required_version = ">= 1.10.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  backend "s3" {
    bucket = "ecommerce-lakehouse-tfstate-352505432441"
    key    = "bootstrap/terraform.tfstate"
    region = "eu-west-1"

    use_lockfile = true
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project   = "ecommerce-lakehouse"
      ManagedBy = "terraform"
      Scope     = "bootstrap"
    }
  }
}

data "aws_caller_identity" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
  region     = var.aws_region
  repo       = "${var.github_owner}/${var.github_repo}"

  # Resource-name prefixes this account's CI is allowed to touch.
  name_prefix   = "${var.project_name}-*"
  db_prefix     = "${replace(var.project_name, "-", "_")}_*"
  data_bucket   = "arn:aws:s3:::${var.project_name}-*"
  state_bucket  = "arn:aws:s3:::${var.state_bucket}"
  glue_arn_base = "arn:aws:glue:${local.region}:${local.account_id}"
  logs_arn_base = "arn:aws:logs:${local.region}:${local.account_id}"
  iam_arn_base  = "arn:aws:iam::${local.account_id}"
}

# ─── GitHub Actions OIDC Provider ────────────────────────────────────────────
# No thumbprint pin: AWS validates token.actions.githubusercontent.com against
# its trusted CA store, and the old hardcoded thumbprint silently rots when
# GitHub rotates its certificate.
resource "aws_iam_openid_connect_provider" "github" {
  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]
}

# ─── Permissions boundary ────────────────────────────────────────────────────
# A ceiling on both CI roles: even if an inline policy is later widened by
# mistake, nothing outside these services — and nothing that touches users,
# account settings or the organisation — can ever be authorised.
# tfsec:ignore:aws-iam-no-policy-wildcards A permissions boundary grants
# nothing on its own — it is a ceiling. Wildcards here describe the widest
# surface CI could ever reach, and narrowing them narrows nothing.
resource "aws_iam_policy" "ci_boundary" {
  name        = "${var.project_name}-ci-boundary"
  description = "Maximum permissions any GitHub Actions role in this account may hold"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ServicesInScope"
        Effect = "Allow"
        Action = [
          "s3:*",
          "glue:*",
          "states:*",
          "events:*",
          "sns:*",
          "athena:*",
          "logs:*",
          "cloudwatch:*",
          "iam:*",
          "sts:GetCallerIdentity",
          "sts:AssumeRole",
          "tag:GetResources",
        ]
        Resource = "*"
      },
      {
        Sid    = "NeverTouchIdentitiesOrTheAccount"
        Effect = "Deny"
        Action = [
          "iam:*User*",
          "iam:*AccessKey*",
          "iam:*LoginProfile*",
          "iam:*MFADevice*",
          "iam:*SAMLProvider*",
          "iam:CreateAccountAlias",
          "iam:DeleteAccountPasswordPolicy",
          "iam:UpdateAccountPasswordPolicy",
          "organizations:*",
          "account:*",
          "aws-portal:*",
          "billing:*",
          "ce:*",
        ]
        Resource = "*"
      },
      {
        # Referenced by its predictable ARN rather than by resource attribute:
        # a policy document cannot reference the policy it defines.
        Sid      = "NeverEscapeTheBoundary"
        Effect   = "Deny"
        Action   = ["iam:DeletePolicy", "iam:CreatePolicyVersion", "iam:DeletePolicyVersion"]
        Resource = "${local.iam_arn_base}:policy/${var.project_name}-ci-boundary"
      },
      {
        Sid      = "NeverDetachTheBoundary"
        Effect   = "Deny"
        Action   = ["iam:DeleteRolePermissionsBoundary"]
        Resource = "*"
      },
    ]
  })
}

# ─── Deploy role — main branch only ──────────────────────────────────────────
resource "aws_iam_role" "github_actions_deploy" {
  name                 = "${var.project_name}-github-actions-deploy"
  description          = "Assumed by CI on pushes to ${var.deploy_branch} to apply infrastructure"
  permissions_boundary = aws_iam_policy.ci_boundary.arn
  max_session_duration = 3600

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Federated = aws_iam_openid_connect_provider.github.arn }
      Action    = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = {
          "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com"
          # Exact match, not a wildcard: `repo:owner/repo:*` matches every
          # branch, tag and environment, so anyone who can push a branch could
          # assume this role.
          "token.actions.githubusercontent.com:sub" = "repo:${local.repo}:ref:refs/heads/${var.deploy_branch}"
        }
      }
    }]
  })
}

# ─── Plan role — pull requests, read-only ────────────────────────────────────
resource "aws_iam_role" "github_actions_plan" {
  name                 = "${var.project_name}-github-actions-plan"
  description          = "Assumed by CI on pull requests to run terraform plan"
  permissions_boundary = aws_iam_policy.ci_boundary.arn
  max_session_duration = 3600

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Federated = aws_iam_openid_connect_provider.github.arn }
      Action    = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = {
          "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com"
        }
        StringLike = {
          "token.actions.githubusercontent.com:sub" = "repo:${local.repo}:pull_request"
        }
      }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "plan_readonly" {
  role       = aws_iam_role.github_actions_plan.name
  policy_arn = "arn:aws:iam::aws:policy/ReadOnlyAccess"
}

# The plan role still needs to write the state lock file it acquires.
resource "aws_iam_role_policy" "plan_state_lock" {
  name = "${var.project_name}-plan-state-lock"
  role = aws_iam_role.github_actions_plan.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:PutObject", "s3:DeleteObject", "s3:GetObject", "s3:ListBucket"]
      Resource = [local.state_bucket, "${local.state_bucket}/*"]
    }]
  })
}

# ─── Deploy permissions ──────────────────────────────────────────────────────
# Scoped to the resources this project actually creates. Generated by walking
# the resource graph rather than by hand-guessing; widen it from CloudTrail
# (`aws iam generate-service-last-accessed-details`) if a new resource type is
# added, rather than reaching for AdministratorAccess again.
# tfsec:ignore:aws-iam-no-policy-wildcards Service wildcards are constrained to
# project-prefixed resource ARNs; the one Resource = "*" statement is limited to
# list/describe actions that AWS does not support resource-level permissions for.
resource "aws_iam_role_policy" "deploy" {
  name = "${var.project_name}-deploy"
  role = aws_iam_role.github_actions_deploy.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "TerraformState"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation",
        ]
        Resource = [local.state_bucket, "${local.state_bucket}/*"]
      },
      {
        Sid      = "LakehouseBucket"
        Effect   = "Allow"
        Action   = ["s3:*"]
        Resource = [local.data_bucket, "${local.data_bucket}/*"]
      },
      {
        Sid    = "ReadOnlyDiscovery"
        Effect = "Allow"
        Action = [
          "s3:ListAllMyBuckets",
          "glue:GetDatabases",
          "glue:GetJobs",
          "glue:ListCrawlers",
          "glue:GetTags",
          "states:ListStateMachines",
          "sns:ListTopics",
          "athena:ListWorkGroups",
          "athena:ListNamedQueries",
          "events:ListRules",
          "logs:DescribeLogGroups",
          "cloudwatch:DescribeAlarms",
          "sts:GetCallerIdentity",
          "tag:GetResources",
        ]
        Resource = "*"
      },
      {
        Sid    = "GlueResources"
        Effect = "Allow"
        Action = ["glue:*"]
        Resource = [
          "${local.glue_arn_base}:catalog",
          "${local.glue_arn_base}:database/${local.db_prefix}",
          "${local.glue_arn_base}:table/${local.db_prefix}/*",
          "${local.glue_arn_base}:job/${local.name_prefix}",
          "${local.glue_arn_base}:crawler/${local.name_prefix}",
        ]
      },
      {
        Sid    = "StepFunctions"
        Effect = "Allow"
        Action = ["states:*"]
        Resource = [
          "arn:aws:states:${local.region}:${local.account_id}:stateMachine:${local.name_prefix}",
          "arn:aws:states:${local.region}:${local.account_id}:execution:${local.name_prefix}:*",
        ]
      },
      {
        Sid      = "EventBridge"
        Effect   = "Allow"
        Action   = ["events:*"]
        Resource = "arn:aws:events:${local.region}:${local.account_id}:rule/${local.name_prefix}"
      },
      {
        Sid      = "Sns"
        Effect   = "Allow"
        Action   = ["sns:*"]
        Resource = "arn:aws:sns:${local.region}:${local.account_id}:${local.name_prefix}"
      },
      {
        Sid      = "Athena"
        Effect   = "Allow"
        Action   = ["athena:*"]
        Resource = "arn:aws:athena:${local.region}:${local.account_id}:workgroup/${local.name_prefix}"
      },
      {
        Sid    = "Logs"
        Effect = "Allow"
        Action = ["logs:*"]
        Resource = [
          "${local.logs_arn_base}:log-group:/aws-glue/*",
          "${local.logs_arn_base}:log-group:/aws/states/*",
          "${local.logs_arn_base}:log-group::log-stream:*",
        ]
      },
      {
        Sid      = "Alarms"
        Effect   = "Allow"
        Action   = ["cloudwatch:PutMetricAlarm", "cloudwatch:DeleteAlarms", "cloudwatch:TagResource"]
        Resource = "arn:aws:cloudwatch:${local.region}:${local.account_id}:alarm:${local.name_prefix}"
      },
      {
        Sid    = "WorkloadRoles"
        Effect = "Allow"
        Action = [
          "iam:CreateRole",
          "iam:DeleteRole",
          "iam:GetRole",
          "iam:ListRolePolicies",
          "iam:ListAttachedRolePolicies",
          "iam:ListInstanceProfilesForRole",
          "iam:PutRolePolicy",
          "iam:GetRolePolicy",
          "iam:DeleteRolePolicy",
          "iam:AttachRolePolicy",
          "iam:DetachRolePolicy",
          "iam:TagRole",
          "iam:UntagRole",
          "iam:UpdateAssumeRolePolicy",
          "iam:PassRole",
        ]
        Resource = "${local.iam_arn_base}:role/${local.name_prefix}"
      },
      {
        # The workload-role prefix above also matches the CI roles themselves.
        # Without this, the deploy role could widen its own trust policy or
        # inline policy — the boundary would still cap it, but self-modification
        # is not something CI ever needs to do. Bootstrap is applied by a human.
        Sid    = "NeverModifyTheCiRoles"
        Effect = "Deny"
        Action = ["iam:*"]
        Resource = [
          aws_iam_role.github_actions_deploy.arn,
          aws_iam_role.github_actions_plan.arn,
        ]
      },
    ]
  })
}
