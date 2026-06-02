# ─── GitHub Actions OIDC Provider ────────────────────────────────────────────
resource "aws_iam_openid_connect_provider" "github" {
  url             = "https://token.actions.githubusercontent.com"
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = ["6938fd4d98bab03faadb97b34396831e3780aea1"]
}

# ─── GitHub Actions IAM Role (OIDC trust) ────────────────────────────────────
resource "aws_iam_role" "github_actions" {
  name = "${var.project_name}-${var.environment}-github-actions-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Federated = aws_iam_openid_connect_provider.github.arn
      }
      Action = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = {
          "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com"
        }
        StringLike = {
          # Scoped to your repo and main branch only
          "token.actions.githubusercontent.com:sub" = "repo:${var.github_repo}:*"
        }
      }
    }]
  })
}

resource "aws_iam_role_policy" "github_actions" {
  name = "${var.project_name}-${var.environment}-github-actions-policy"
  role = aws_iam_role.github_actions.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "TerraformS3State"
        Effect = "Allow"
        Action = [
          "s3:GetObject", "s3:PutObject", "s3:ListBucket", "s3:DeleteObject"
        ]
        Resource = [
          "arn:aws:s3:::${var.bucket_name}",
          "arn:aws:s3:::${var.bucket_name}/*",
        ]
      },
      {
        Sid    = "GlueScriptUpload"
        Effect = "Allow"
        Action = ["s3:PutObject", "s3:GetObject", "s3:DeleteObject"]
        Resource = "arn:aws:s3:::${var.bucket_name}/glue-scripts/*"
      },
      {
        Sid    = "TerraformIAM"
        Effect = "Allow"
        Action = [
          "iam:CreateRole", "iam:DeleteRole", "iam:GetRole",
          "iam:AttachRolePolicy", "iam:DetachRolePolicy",
          "iam:PutRolePolicy", "iam:DeleteRolePolicy", "iam:GetRolePolicy",
          "iam:CreateOpenIDConnectProvider", "iam:GetOpenIDConnectProvider",
          "iam:DeleteOpenIDConnectProvider", "iam:TagOpenIDConnectProvider",
          "iam:PassRole", "iam:ListAttachedRolePolicies", "iam:ListRolePolicies",
          "iam:TagRole", "iam:UntagRole",
        ]
        Resource = "*"
      },
      {
        Sid    = "TerraformGlue"
        Effect = "Allow"
        Action = [
          "glue:*"
        ]
        Resource = "*"
      },
      {
        Sid    = "TerraformStepFunctions"
        Effect = "Allow"
        Action = [
          "states:CreateStateMachine", "states:UpdateStateMachine",
          "states:DeleteStateMachine", "states:DescribeStateMachine",
          "states:TagResource", "states:UntagResource",
          "states:CreateActivity", "states:DeleteActivity",
        ]
        Resource = "*"
      },
      {
        Sid    = "TerraformSNS"
        Effect = "Allow"
        Action = [
          "sns:CreateTopic", "sns:DeleteTopic", "sns:GetTopicAttributes",
          "sns:SetTopicAttributes", "sns:Subscribe", "sns:Unsubscribe",
          "sns:TagResource", "sns:UntagResource",
        ]
        Resource = "*"
      },
      {
        Sid    = "TerraformAthena"
        Effect = "Allow"
        Action = [
          "athena:CreateWorkGroup", "athena:DeleteWorkGroup",
          "athena:GetWorkGroup", "athena:UpdateWorkGroup",
          "athena:CreateNamedQuery", "athena:DeleteNamedQuery",
          "athena:GetNamedQuery", "athena:TagResource", "athena:UntagResource",
          "athena:StartQueryExecution", "athena:GetQueryExecution",
          "athena:GetQueryResults",
        ]
        Resource = "*"
      },
      {
        Sid    = "TerraformCloudWatch"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup", "logs:DeleteLogGroup",
          "logs:DescribeLogGroups", "logs:PutRetentionPolicy",
          "logs:TagLogGroup", "logs:UntagLogGroup",
          "logs:CreateLogDelivery", "logs:PutResourcePolicy",
          "logs:DescribeResourcePolicies",
        ]
        Resource = "*"
      },
    ]
  })
}

# ─── Glue IAM Role ───────────────────────────────────────────────────────────
resource "aws_iam_role" "glue" {
  name = "${var.project_name}-${var.environment}-glue-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_s3" {
  name = "${var.project_name}-${var.environment}-glue-s3-policy"
  role = aws_iam_role.glue.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3BucketAccess"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:GetObjectAcl",
          "s3:PutObject",
          "s3:PutObjectAcl",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation",
          "s3:CopyObject",
        ]
        Resource = [
          var.bucket_arn,
          "${var.bucket_arn}/*",
        ]
      },
      {
        Sid    = "GlueDataCatalog"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetTable",
          "glue:GetTables",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:GetPartitions",
          "glue:BatchCreatePartition",
        ]
        Resource = "*"
      },
      {
        Sid    = "CloudWatchLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
        ]
        Resource = "arn:aws:logs:${var.region}:${var.account_id}:log-group:/aws-glue/*"
      },
    ]
  })
}

# ─── Step Functions IAM Role ─────────────────────────────────────────────────
resource "aws_iam_role" "step_functions" {
  name = "${var.project_name}-${var.environment}-sfn-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "states.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "step_functions" {
  name = "${var.project_name}-${var.environment}-sfn-policy"
  role = aws_iam_role.step_functions.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "StartGlueJobs"
        Effect = "Allow"
        Action = [
          "glue:StartJobRun",
          "glue:GetJobRun",
          "glue:GetJobRuns",
          "glue:BatchStopJobRun",
          "glue:StartCrawler",
          "glue:GetCrawler",
        ]
        Resource = "*"
      },
      {
        Sid    = "PublishSNS"
        Effect = "Allow"
        Action = ["sns:Publish"]
        Resource = "arn:aws:sns:${var.region}:${var.account_id}:*"
      },
      {
        Sid    = "AthenaAccess"
        Effect = "Allow"
        Action = [
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults",
        ]
        Resource = "*"
      },
      {
        Sid    = "AthenaS3Results"
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket",
        ]
        Resource = [
          var.bucket_arn,
          "${var.bucket_arn}/athena-results/*",
        ]
      },
      {
        Sid    = "CloudWatchLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogDelivery",
          "logs:GetLogDelivery",
          "logs:UpdateLogDelivery",
          "logs:DeleteLogDelivery",
          "logs:ListLogDeliveries",
          "logs:PutLogEvents",
          "logs:PutResourcePolicy",
          "logs:DescribeResourcePolicies",
          "logs:DescribeLogGroups",
        ]
        Resource = "*"
      },
      {
        Sid    = "XRayAccess"
        Effect = "Allow"
        Action = [
          "xray:PutTraceSegments",
          "xray:PutTelemetryRecords",
          "xray:GetSamplingRules",
          "xray:GetSamplingTargets",
        ]
        Resource = "*"
      },
    ]
  })
}
