# ==============================================================================
# FILE 1: BRONZE LAYER TERRAFORM INFRASTRUCTURE (1_bronze.tf)
# ==============================================================================
# Includes:
# - S3 Data Lake Bucket & Core S3 Folder Prefixes
# - Secrets Manager Secrets for REST API OAuth 2.0 Credentials
# - AWS Glue Execution IAM Role & Policy
# - AWS Glue Python Shell Ingestion Job (incremental_load_handler.py)
# - Pre-existence skip logic (use_existing_s3_bucket, use_existing_iam_role, use_existing_secrets)
# ==============================================================================

terraform {
  required_version = ">= 1.3.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

data "aws_caller_identity" "current" {}

locals {
  bucket_name   = var.use_existing_s3_bucket ? "${var.data_lake_bucket_name}-${var.environment}" : (length(aws_s3_bucket.data_lake) > 0 ? aws_s3_bucket.data_lake[0].bucket : "${var.data_lake_bucket_name}-${var.environment}")
  bucket_arn    = var.use_existing_s3_bucket ? "arn:aws:s3:::${var.data_lake_bucket_name}-${var.environment}" : (length(aws_s3_bucket.data_lake) > 0 ? aws_s3_bucket.data_lake[0].arn : "arn:aws:s3:::${var.data_lake_bucket_name}-${var.environment}")
  glue_role_arn = var.use_existing_iam_role ? "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${var.app_name}-glue-execution-role-${var.environment}" : (length(aws_iam_role.glue_execution_role) > 0 ? aws_iam_role.glue_execution_role[0].arn : "")
}

# ------------------------------------------------------------------------------
# 1. Single S3 Data Lake Bucket (Skips creation if use_existing_s3_bucket = true)
# ------------------------------------------------------------------------------
resource "aws_s3_bucket" "data_lake" {
  count         = var.use_existing_s3_bucket ? 0 : 1
  bucket        = "${var.data_lake_bucket_name}-${var.environment}"
  force_destroy = false

  tags = {
    Environment = var.environment
    Application = var.app_name
    Layer       = "Bronze"
    ManagedBy   = "Terraform"
  }
}

resource "aws_s3_bucket_versioning" "data_lake_versioning" {
  count  = var.use_existing_s3_bucket ? 0 : 1
  bucket = aws_s3_bucket.data_lake[0].id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake_encryption" {
  count  = var.use_existing_s3_bucket ? 0 : 1
  bucket = aws_s3_bucket.data_lake[0].id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "data_lake_public_block" {
  count                   = var.use_existing_s3_bucket ? 0 : 1
  bucket                  = aws_s3_bucket.data_lake[0].id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Bronze S3 Folder Objects
resource "aws_s3_object" "folder_bronze" {
  bucket = local.bucket_name
  key    = "bronze/"
}

resource "aws_s3_object" "folder_metadata" {
  bucket = local.bucket_name
  key    = "metadata/"
}

resource "aws_s3_object" "folder_bronze_script" {
  bucket = local.bucket_name
  key    = "bronze/script/"
}


# ------------------------------------------------------------------------------
# 2. Secrets Manager Secrets (Skips creation if use_existing_secrets = true)
# ------------------------------------------------------------------------------
resource "aws_secretsmanager_secret" "servicenow_secret" {
  count       = var.use_existing_secrets ? 0 : 1
  name        = "data-lake/servicenow-credentials-${var.environment}"
  description = "ServiceNow OAuth 2.0 credentials (Password/Client Credentials flow)."

  tags = {
    Environment = var.environment
    Application = var.app_name
    Layer       = "Bronze"
    ManagedBy   = "Terraform"
  }
}

resource "aws_secretsmanager_secret_version" "servicenow_secret_val" {
  count     = var.use_existing_secrets ? 0 : 1
  secret_id = aws_secretsmanager_secret.servicenow_secret[0].id
  secret_string = jsonencode({
    grant_type    = "password"
    client_id     = "CHANGE_ME_SERVICENOW_CLIENT_ID"
    client_secret = "CHANGE_ME_SERVICENOW_CLIENT_SECRET"
    username      = "servicenow_api_user"
    password      = "CHANGE_ME_SERVICENOW_PASSWORD"
    token_url     = "https://your-instance.service-now.com/oauth_token.do"
  })
}

resource "aws_secretsmanager_secret" "moveworks_secret" {
  count       = var.use_existing_secrets ? 0 : 1
  name        = "data-lake/moveworks-credentials-${var.environment}"
  description = "Moveworks OAuth 2.0 credentials (Client Credentials flow)."

  tags = {
    Environment = var.environment
    Application = var.app_name
    Layer       = "Bronze"
    ManagedBy   = "Terraform"
  }
}

resource "aws_secretsmanager_secret_version" "moveworks_secret_val" {
  count     = var.use_existing_secrets ? 0 : 1
  secret_id = aws_secretsmanager_secret.moveworks_secret[0].id
  secret_string = jsonencode({
    grant_type    = "client_credentials"
    client_id     = "CHANGE_ME_MOVEWORKS_CLIENT_ID"
    client_secret = "CHANGE_ME_MOVEWORKS_CLIENT_SECRET"
    token_url     = "https://api.moveworks.ai/rest/v1/oauth/token"
    scope         = "export:read"
  })
}

resource "aws_secretsmanager_secret" "genesys_secret" {
  count       = var.use_existing_secrets ? 0 : 1
  name        = "data-lake/genesys-credentials-${var.environment}"
  description = "Genesys Cloud OAuth 2.0 credentials (Client Credentials flow)."

  tags = {
    Environment = var.environment
    Application = var.app_name
    Layer       = "Bronze"
    ManagedBy   = "Terraform"
  }
}

resource "aws_secretsmanager_secret_version" "genesys_secret_val" {
  count     = var.use_existing_secrets ? 0 : 1
  secret_id = aws_secretsmanager_secret.genesys_secret[0].id
  secret_string = jsonencode({
    grant_type    = "client_credentials"
    client_id     = "CHANGE_ME_GENESYS_CLIENT_ID"
    client_secret = "CHANGE_ME_GENESYS_CLIENT_SECRET"
    token_url     = "https://login.mypurecloud.com/oauth/token"
  })
}


# ------------------------------------------------------------------------------
# 3. AWS Glue Execution IAM Role & Policies (Skips creation if use_existing_iam_role = true)
# ------------------------------------------------------------------------------
resource "aws_iam_role" "glue_execution_role" {
  count = var.use_existing_iam_role ? 0 : 1
  name  = "${var.app_name}-glue-execution-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })

  tags = {
    Environment = var.environment
    Application = var.app_name
    ManagedBy   = "Terraform"
  }
}

resource "aws_iam_policy" "glue_execution_policy" {
  count       = var.use_existing_iam_role ? 0 : 1
  name        = "${var.app_name}-glue-policy-${var.environment}"
  description = "Execution policy for AWS Glue Data Pipeline ingestion and PySpark Iceberg ETL jobs."

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          local.bucket_arn,
          "${local.bucket_arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue",
          "secretsmanager:DescribeSecret"
        ]
        Resource = [
          "arn:aws:secretsmanager:${var.aws_region}:*:secret:data-lake/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "cloudwatch:PutMetricData",
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:CreateTable",
          "glue:GetTable",
          "glue:GetTables",
          "glue:UpdateTable",
          "glue:DeleteTable",
          "glue:BatchCreatePartition",
          "glue:BatchGetPartition",
          "glue:GetPartition",
          "glue:GetPartitions"
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_policy_attach" {
  count      = var.use_existing_iam_role ? 0 : 1
  role       = aws_iam_role.glue_execution_role[0].name
  policy_arn = aws_iam_policy.glue_execution_policy[0].arn
}

resource "aws_iam_role_policy_attachment" "glue_service_role_attach" {
  count      = var.use_existing_iam_role ? 0 : 1
  role       = aws_iam_role.glue_execution_role[0].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}


# ------------------------------------------------------------------------------
# 4. AWS Glue Python Shell Ingestion Job (Bronze)
# ------------------------------------------------------------------------------
resource "aws_glue_job" "bronze_ingestion_job" {
  name         = "${var.app_name}-bronze-ingestion-${var.environment}"
  description  = "AWS Glue Python Shell Job executing Bronze REST API & DB ingestion."
  role_arn     = local.glue_role_arn
  glue_version = "3.0"

  command {
    name            = "pythonshell"
    python_version  = "3.9"
    script_location = "s3://${local.bucket_name}/bronze/script/incremental_load_handler.py"
  }

  default_arguments = {
    "--extra-py-files"        = "s3://${local.bucket_name}/bronze/script/config_loader.py,s3://${local.bucket_name}/bronze/script/connectors.zip"
    "--CONFIG_S3_PATH"        = "s3://${local.bucket_name}/bronze/script/config/bronze_config.json"
    "--BRONZE_BUCKET"         = local.bucket_name
    "--OUTPUT_FORMAT"         = var.output_format
    "--CLOUDWATCH_NAMESPACE"  = "UAX/DataPipeline/Ingestion"
    "--ERROR_HANDLING_MODE"   = "CONTINUE_ON_ERROR"
    "--job-language"          = "python"
  }

  tags = {
    Environment = var.environment
    Application = var.app_name
    Layer       = "Bronze"
    ManagedBy   = "Terraform"
  }
}
