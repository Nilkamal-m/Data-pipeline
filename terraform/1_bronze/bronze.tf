# ==============================================================================
# BRONZE LAYER TERRAFORM INFRASTRUCTURE (terraform/1_bronze/bronze.tf)
# ==============================================================================
# Self-contained Terraform script for Bronze Ingestion.
# Contains all variables, S3 Bucket, Secrets Manager, IAM Roles, and Bronze Glue Job.
# Includes pre-existence check logic to skip creating resources if already present.
# ==============================================================================

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
  description = "Deployment environment stage (dev, prod)."
}

variable "app_name" {
  type        = string
  default     = "uax-datalake"
  description = "Application name prefix for resources."
}



variable "output_format" {
  type        = string
  default     = "parquet"
  description = "Raw Bronze data serialization format."
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

locals {
  bucket_name   = "${var.app_name}-${var.environment}-bucket"
  bucket_arn    = "arn:aws:s3:::${local.bucket_name}"
  glue_role_arn = module.glue_iam_role.iam_role_arn
}


# ------------------------------------------------------------------------------
# 0. AWS KMS Customer Managed Key using Enterprise Private Registry Module
# ------------------------------------------------------------------------------
module "kms_key" {
  source = "kms/aws"

  description    = "KMS Key for UAX Data Lake S3 Bucket, Secrets Manager, Glue, Athena, CloudWatch Logs, SNS, and Step Functions"
  kms_alias_name = "alias/${var.app_name}-${var.environment}-s3-key"
  service_name   = ["s3", "secretsmanager", "glue", "athena", "logs", "sns", "states"]

  tags = {
    Environment = var.environment
    Application = var.app_name
    Layer       = "Security"
    ManagedBy   = "Terraform"
  }
}


# ------------------------------------------------------------------------------
# 1. Single S3 Bucket using Enterprise Private Registry Module
# ------------------------------------------------------------------------------
module "s3_bucket" {
  source = "s3-bucket/aws"

  create_bucket = !var.use_existing_s3_bucket
  bucket        = local.bucket_name
  force_destroy = false

  # Block public access
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true

  # Server side encryption with KMS CMK
  server_side_encryption_configuration = {
    rule = {
      apply_server_side_encryption_by_default = {
        kms_master_key_id = module.kms_key.key_arn
        sse_algorithm     = "aws:kms"
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


# ------------------------------------------------------------------------------
# 2. Secrets Manager Secrets using Enterprise Private Registry Module
# ------------------------------------------------------------------------------
module "servicenow_secret" {
  source = "secrets-manager/aws"

  create      = !var.use_existing_secrets
  name        = "${var.app_name}/servicenow-credentials-${var.environment}"
  description = "ServiceNow REST API credentials (Basic Auth)."
  kms_key_id  = module.kms_key.key_arn

  secret_string = jsonencode({
    auth_type     = "basic"
    grant_type    = ""
    client_id     = ""
    client_secret = ""
    username      = "servicenow_api_user"
    password      = "CHANGE_ME_SERVICENOW_PASSWORD"
    token_url     = ""
    scope         = ""
  })

  tags = {
    Environment = var.environment
    Application = var.app_name
    Layer       = "Bronze"
    ManagedBy   = "Terraform"
  }
}

module "moveworks_secret" {
  source = "secrets-manager/aws"

  create      = !var.use_existing_secrets
  name        = "${var.app_name}/moveworks-credentials-${var.environment}"
  description = "Moveworks REST API credentials (OAuth 2.0)."
  kms_key_id  = module.kms_key.key_arn

  secret_string = jsonencode({
    auth_type     = "oauth2"
    grant_type    = "client_credentials"
    client_id     = "CHANGE_ME_MOVEWORKS_CLIENT_ID"
    client_secret = "CHANGE_ME_MOVEWORKS_CLIENT_SECRET"
    username      = ""
    password      = ""
    token_url     = "https://api.moveworks.ai/rest/v1/oauth/token"
    scope         = "export:read"
  })

  tags = {
    Environment = var.environment
    Application = var.app_name
    Layer       = "Bronze"
    ManagedBy   = "Terraform"
  }
}

module "genesys_secret" {
  source = "secrets-manager/aws"

  create      = !var.use_existing_secrets
  name        = "${var.app_name}/genesys-credentials-${var.environment}"
  description = "Genesys Cloud REST API credentials (OAuth 2.0)."
  kms_key_id  = module.kms_key.key_arn

  secret_string = jsonencode({
    auth_type     = "oauth2"
    grant_type    = "client_credentials"
    client_id     = "CHANGE_ME_GENESYS_CLIENT_ID"
    client_secret = "CHANGE_ME_GENESYS_CLIENT_SECRET"
    username      = ""
    password      = ""
    token_url     = "https://login.mypurecloud.com/oauth/token"
    scope         = ""
  })

  tags = {
    Environment = var.environment
    Application = var.app_name
    Layer       = "Bronze"
    ManagedBy   = "Terraform"
  }
}


# ------------------------------------------------------------------------------
# 3. AWS IAM Execution Role & Policies for Glue and Lambda Helper
# ------------------------------------------------------------------------------
module "glue_iam_policy" {
  source = "iam/aws//modules/iam-policy"

  create_policy = !var.use_existing_iam_role
  name          = "${var.app_name}-glue-policy-${var.environment}"
  description   = "Execution policy for AWS Glue ingestion, PySpark Iceberg ETL jobs, and Helper Lambda trigger."

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
          "kms:Decrypt",
          "kms:Encrypt",
          "kms:GenerateDataKey",
          "kms:GenerateDataKeyWithoutPlaintext",
          "kms:DescribeKey",
          "kms:ReEncrypt*"
        ]
        Resource = [
          module.kms_key.key_arn
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue",
          "secretsmanager:DescribeSecret"
        ]
        Resource = [
          "arn:aws:secretsmanager:${var.aws_region}:*:secret:${var.app_name}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "cloudwatch:PutMetricData"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = [
          "arn:aws:logs:${var.aws_region}:*:log-group:/aws-glue/jobs/*",
          "arn:aws:logs:${var.aws_region}:*:log-group:/aws/lambda/${var.app_name}*",
          "arn:aws:logs:${var.aws_region}:*:log-group:${var.app_name}*"
        ]
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
          "glue:GetPartitions",
          "glue:StartJobRun",
          "glue:GetJobRun",
          "glue:GetJobRuns",
          "glue:BatchStopJobRun"
        ]
        Resource = "*"
      }
    ]
  })

  tags = {
    Environment = var.environment
    Application = var.app_name
    ManagedBy   = "Terraform"
  }
}

module "glue_iam_role" {
  source = "iam/aws//modules/iam-assumable-role"

  create_role       = !var.use_existing_iam_role
  role_name         = "${var.app_name}-glue-execution-role-${var.environment}"
  role_requires_mfa = false

  trusted_role_services = [
    "glue.amazonaws.com",
    "lambda.amazonaws.com"
  ]

  custom_role_policy_arns = [
    "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole",
    module.glue_iam_policy.arn
  ]

  tags = {
    Environment = var.environment
    Application = var.app_name
    ManagedBy   = "Terraform"
  }
}


# ------------------------------------------------------------------------------
# 3.1 AWS Glue Security Configuration for Encryption at Rest & CloudWatch Logs
# ------------------------------------------------------------------------------
resource "aws_glue_security_configuration" "glue_security_config" {
  name = "${var.app_name}-glue-secconfig-${var.environment}"

  encryption_configuration {
    cloudwatch_encryption {
      cloudwatch_encryption_mode = "SSE-KMS"
      kms_key_arn                = module.kms_key.key_arn
    }

    job_bookmarks_encryption {
      job_bookmarks_encryption_mode = "CSE-KMS"
      kms_key_arn                   = module.kms_key.key_arn
    }

    s3_encryption {
      s3_encryption_mode = "SSE-KMS"
      kms_key_arn        = module.kms_key.key_arn
    }
  }
}


# ------------------------------------------------------------------------------
# 4. AWS Glue Python Shell Ingestion Job using Enterprise Private Registry Module
# ------------------------------------------------------------------------------
module "bronze_glue_job" {
  source = "glue/aws//modules/job"

  name                        = "${var.app_name}-bronze-ingestion-${var.environment}"
  description                 = "AWS Glue Python Shell Job executing Bronze REST API & DB ingestion."
  role_arn                    = local.glue_role_arn
  glue_version                = "3.0"
  security_configuration_name = aws_glue_security_configuration.glue_security_config.name
  cloudwatch_kms_key_arn      = module.kms_key.key_arn
  gluejob_kms_key_arn         = module.kms_key.key_arn
  s3_kms_key_arn              = module.kms_key.key_arn

  command = {
    name            = "pythonshell"
    python_version  = "3.9"
    script_location = "s3://${local.bucket_name}/bronze/script/uax_bronze_load.py"
  }

  default_arguments = {
    "--security-configuration" = aws_glue_security_configuration.glue_security_config.name
    "--extra-py-files"         = "s3://${local.bucket_name}/bronze/script/config_loader.py,s3://${local.bucket_name}/bronze/script/connectors.zip"
    "--CONFIG_S3_PATH"         = "s3://${local.bucket_name}/bronze/script/config/bronze_config.json"
    "--BRONZE_BUCKET"          = local.bucket_name
    "--OUTPUT_FORMAT"          = var.output_format
    "--CLOUDWATCH_NAMESPACE"   = "UAX/DataPipeline/Ingestion"
    "--ERROR_HANDLING_MODE"    = "CONTINUE_ON_ERROR"
    "--job-language"           = "python"
  }

  tags = {
    Environment = var.environment
    Application = var.app_name
    Layer       = "Bronze"
    ManagedBy   = "Terraform"
  }
}

# ------------------------------------------------------------------------------
# Bronze Layer Outputs (Prints details for all created services)
# ------------------------------------------------------------------------------
output "data_lake_s3_bucket_name" {
  value       = local.bucket_name
  description = "S3 Data Lake bucket name."
}

output "servicenow_secret_name" {
  value       = "${var.app_name}/servicenow-credentials-${var.environment}"
  description = "ServiceNow Secrets Manager Secret Name."
}

output "moveworks_secret_name" {
  value       = "${var.app_name}/moveworks-credentials-${var.environment}"
  description = "Moveworks Secrets Manager Secret Name."
}

output "genesys_secret_name" {
  value       = "${var.app_name}/genesys-credentials-${var.environment}"
  description = "Genesys Secrets Manager Secret Name."
}

output "glue_iam_policy_name" {
  value       = "${var.app_name}-glue-policy-${var.environment}"
  description = "AWS Glue IAM Execution Policy Name."
}

output "glue_iam_role_name" {
  value       = "${var.app_name}-glue-execution-role-${var.environment}"
  description = "AWS Glue IAM Execution Role Name."
}

output "glue_bronze_ingestion_job_name" {
  value       = "${var.app_name}-bronze-ingestion-${var.environment}"
  description = "AWS Glue Python Shell Bronze Ingestion Job Name."
}

output "kms_key_arn" {
  value       = module.kms_key.key_arn
  description = "AWS KMS Customer Managed Key ARN for Data Lake PII Protection."
}
