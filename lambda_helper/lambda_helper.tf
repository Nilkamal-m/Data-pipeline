# ==============================================================================
# HELPER LAMBDA FOR AWS GLUE BRONZE & SILVER JOBS (lambda_helper/lambda_helper.tf)
# ==============================================================================
# Helper Lambda infrastructure to trigger and monitor AWS Glue Bronze & Silver jobs
# during development when Glue Console access is restricted.
# Re-uses or attaches to the Glue Execution IAM Role.
# ==============================================================================

variable "aws_region" {
  type        = string
  default     = "us-east-1"
  description = "AWS deployment region."
}

variable "environment" {
  type        = string
  default     = "dev"
  description = "Deployment environment stage (dev, prod)."
}

variable "app_name" {
  type        = string
  default     = "uax-datalake"
  description = "Application name prefix for resources."
}

variable "use_existing_glue_role" {
  type        = bool
  default     = true
  description = "If true, reuses the existing AWS Glue execution role ARN."
}

# ------------------------------------------------------------------------------
# 1. IAM Execution Role for Helper Lambda (Or Reuse Glue Role)
# ------------------------------------------------------------------------------
# Option A: Standalone Role with Glue & CloudWatch Logs permissions
resource "aws_iam_role" "lambda_helper_role" {
  count = var.use_existing_glue_role ? 0 : 1
  name  = "${var.app_name}-glue-trigger-lambda-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = ["lambda.amazonaws.com", "glue.amazonaws.com"]
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = {
    Environment = var.environment
    Application = var.app_name
    Layer       = "DevelopmentHelper"
    ManagedBy   = "Terraform"
  }
}

# IAM Policy allowing Lambda to Start & Monitor Glue Jobs
resource "aws_iam_policy" "lambda_glue_trigger_policy" {
  count       = var.use_existing_glue_role ? 0 : 1
  name        = "${var.app_name}-glue-trigger-policy-${var.environment}"
  description = "Allows Helper Lambda function to start, monitor, and inspect AWS Glue jobs."

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:StartJobRun",
          "glue:GetJobRun",
          "glue:GetJobRuns",
          "glue:BatchStopJobRun"
        ]
        Resource = [
          "arn:aws:glue:${var.aws_region}:*:job/${var.app_name}*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_glue_trigger_attach" {
  count      = var.use_existing_glue_role ? 0 : 1
  role       = aws_iam_role.lambda_helper_role[0].name
  policy_arn = aws_iam_policy.lambda_glue_trigger_policy[0].arn
}

locals {
  # Reuses existing Glue Execution Role or standalone helper role
  glue_execution_role_arn = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${var.app_name}-glue-execution-role-${var.environment}"
  lambda_role_arn         = var.use_existing_glue_role ? local.glue_execution_role_arn : aws_iam_role.lambda_helper_role[0].arn
}

data "aws_caller_identity" "current" {}

# ------------------------------------------------------------------------------
# 2. Zip Lambda Source Code
# ------------------------------------------------------------------------------
data "archive_file" "lambda_helper_zip" {
  type        = "zip"
  source_file = "${path.module}/lambda_function.py"
  output_path = "${path.module}/lambda_function.zip"
}

# ------------------------------------------------------------------------------
# 3. AWS Lambda Function Definition
# ------------------------------------------------------------------------------
resource "aws_lambda_function" "glue_trigger_lambda" {
  filename         = data.archive_file.lambda_helper_zip.output_path
  source_code_hash = data.archive_file.lambda_helper_zip.output_base64sha256
  function_name    = "${var.app_name}-glue-job-trigger-${var.environment}"
  role             = local.lambda_role_arn
  handler          = "lambda_function.lambda_handler"
  runtime          = "python3.9"
  timeout          = 900  # 15 minutes max timeout for synchronous polling
  memory_size      = 256

  environment {
    variables = {
      DEFAULT_BRONZE_JOB = "${var.app_name}-bronze-ingestion-${var.environment}"
      DEFAULT_SILVER_JOB = "${var.app_name}-silver-iceberg-etl-${var.environment}"
      ENVIRONMENT        = var.environment
    }
  }

  tags = {
    Environment = var.environment
    Application = var.app_name
    Layer       = "DevelopmentHelper"
    ManagedBy   = "Terraform"
  }
}

# ------------------------------------------------------------------------------
# Outputs
# ------------------------------------------------------------------------------
output "helper_lambda_function_name" {
  value       = aws_lambda_function.glue_trigger_lambda.function_name
  description = "Helper Lambda Function name for triggering Bronze & Silver Glue jobs."
}

output "helper_lambda_arn" {
  value       = aws_lambda_function.glue_trigger_lambda.arn
  description = "Helper Lambda ARN."
}
