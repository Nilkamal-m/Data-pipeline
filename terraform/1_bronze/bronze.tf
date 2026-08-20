# ==============================================================================
# BRONZE LAYER TERRAFORM INFRASTRUCTURE (terraform/1_bronze/bronze.tf)
# ==============================================================================
# Self-contained Terraform script for Bronze Ingestion.
# Contains all variables, S3 Bucket, Secrets Manager, IAM Roles, and Bronze Glue Job.
# Includes pre-existence check logic to skip creating resources if already present.
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

# ------------------------------------------------------------------------------
# Bronze Layer Variables (All necessary variables defined inside this file)
# ------------------------------------------------------------------------------
variable "aws_region" {
  type        = string
  default     = "us-east-1"
  description = "AWS deployment region."
}

variable "environment" {
  type        = string
  default     = "dev"
  description = "Deployment environment stage (dev, staging, prod)."
}

variable "app_name" {
  type        = string
  default     = "hr-datalake"
  description = "Application name prefix for resources."
}

variable "data_lake_bucket_name" {
  type        = string
  default     = "uax-data-lake-bucket"
  description = "Base S3 data lake bucket name (environment suffix will be appended)."
}

variable "output_format" {
  type        = string
  default     = "parquet"
  description = "Raw Bronze data serialization format (parquet or json)."
}

# Pre-existence safety toggles
variable "use_existing_s3_bucket" {
  type        = bool
  default     = false
  description = "If true, skips creating S3 bucket and reuses existing bucket."
}

variable "use_existing_iam_role" {
  type        = bool
  default     = false
  description = "If true, skips creating Glue IAM role and reuses existing role."
}

variable "use_existing_secrets" {
  type        = bool
  default     = false
  description = "If true, skips creating Secrets Manager secrets and reuses existing secrets."
}

locals {
  bucket_name   = "${var.data_lake_bucket_name}-${var.environment}"
  bucket_arn    = "arn:aws:s3:::${local.bucket_name}"
  glue_role_arn = var.use_existing_iam_role ? "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${var.app_name}-glue-execution-role-${var.environment}" : (length(aws_iam_role.glue_execution_role) > 0 ? aws_iam_role.glue_execution_role[0].arn : "")
}


# ------------------------------------------------------------------------------
# 1. Single S3 Bucket using AWS Community Module
# ------------------------------------------------------------------------------
module "s3_bucket" {
  source  = "terraform-aws-modules/s3-bucket/aws"
  version = "~> 4.0"

  create_bucket = !var.use_existing_s3_bucket
  bucket        = local.bucket_name
  force_destroy = false

  # Block public access
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true

  # Server side encryption
  server_side_encryption_configuration = {
    rule = {
      apply_server_side_encryption_by_default = {
        sse_algorithm = "AES256"
      }
    }
  }

  tags = {
    Environment = var.environment
    Application = var.app_name
    Layer       = "Bronze"
    ManagedBy   = "Terraform"
  }
}

# Bronze S3 Folder Structure Placeholders
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

# ------------------------------------------------------------------------------
# Bronze Layer Outputs
# ------------------------------------------------------------------------------
output "data_lake_s3_bucket_name" {
  value       = local.bucket_name
  description = "Single S3 Data Lake bucket name."
}

output "glue_bronze_ingestion_job_name" {
  value       = aws_glue_job.bronze_ingestion_job.name
  description = "AWS Glue Python Shell Bronze Ingestion Job Name."
}
