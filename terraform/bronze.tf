# ==============================================================================
# PART 1: BRONZE LAYER TERRAFORM INFRASTRUCTURE
# ==============================================================================
# Includes:
# - Single S3 Data Lake Bucket & Core S3 Folder Prefixes
# - Secrets Manager Secrets for REST API OAuth 2.0 Credentials
# - AWS Glue Execution IAM Role & Least-Privilege Policies
# - AWS Glue Python Shell Ingestion Job (incremental_load_handler.py)
# ==============================================================================

# ------------------------------------------------------------------------------
# 1. Single S3 Data Lake Bucket
# ------------------------------------------------------------------------------
resource "aws_s3_bucket" "data_lake" {
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
  bucket = aws_s3_bucket.data_lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake_encryption" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "data_lake_public_block" {
  bucket                  = aws_s3_bucket.data_lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Bronze S3 Folder Structure Placeholders
resource "aws_s3_object" "folder_bronze" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "bronze/"
}

resource "aws_s3_object" "folder_metadata" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "metadata/"
}

resource "aws_s3_object" "folder_bronze_script" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "bronze/script/"
}


# ------------------------------------------------------------------------------
# 2. AWS Secrets Manager Secrets (OAuth 2.0 Credentials)
# ------------------------------------------------------------------------------
resource "aws_secretsmanager_secret" "servicenow_secret" {
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
  secret_id = aws_secretsmanager_secret.servicenow_secret.id
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
  secret_id = aws_secretsmanager_secret.moveworks_secret.id
  secret_string = jsonencode({
    grant_type    = "client_credentials"
    client_id     = "CHANGE_ME_MOVEWORKS_CLIENT_ID"
    client_secret = "CHANGE_ME_MOVEWORKS_CLIENT_SECRET"
    token_url     = "https://api.moveworks.ai/rest/v1/oauth/token"
    scope         = "export:read"
  })
}

resource "aws_secretsmanager_secret" "genesys_secret" {
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
  secret_id = aws_secretsmanager_secret.genesys_secret.id
  secret_string = jsonencode({
    grant_type    = "client_credentials"
    client_id     = "CHANGE_ME_GENESYS_CLIENT_ID"
    client_secret = "CHANGE_ME_GENESYS_CLIENT_SECRET"
    token_url     = "https://login.mypurecloud.com/oauth/token"
  })
}


# ------------------------------------------------------------------------------
# 3. AWS Glue Execution IAM Role & Least-Privilege Policies
# ------------------------------------------------------------------------------
resource "aws_iam_role" "glue_execution_role" {
  name = "${var.app_name}-glue-execution-role-${var.environment}"

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
          aws_s3_bucket.data_lake.arn,
          "${aws_s3_bucket.data_lake.arn}/*"
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
  role       = aws_iam_role.glue_execution_role.name
  policy_arn = aws_iam_policy.glue_execution_policy.arn
}

resource "aws_iam_role_policy_attachment" "glue_service_role_attach" {
  role       = aws_iam_role.glue_execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}


# ------------------------------------------------------------------------------
# 4. AWS Glue Python Shell Ingestion Job (Bronze)
# ------------------------------------------------------------------------------
resource "aws_glue_job" "bronze_ingestion_job" {
  name         = "${var.app_name}-bronze-ingestion-${var.environment}"
  description  = "AWS Glue Python Shell Job executing Bronze REST API & DB ingestion."
  role_arn     = aws_iam_role.glue_execution_role.arn
  glue_version = "3.0"

  command {
    name            = "pythonshell"
    python_version  = "3.9"
    script_location = "s3://${aws_s3_bucket.data_lake.bucket}/bronze/script/incremental_load_handler.py"
  }

  default_arguments = {
    "--extra-py-files"        = "s3://${aws_s3_bucket.data_lake.bucket}/bronze/script/config_loader.py,s3://${aws_s3_bucket.data_lake.bucket}/bronze/script/connectors.zip"
    "--CONFIG_S3_PATH"        = "s3://${aws_s3_bucket.data_lake.bucket}/bronze/script/config/bronze_config.json"
    "--BRONZE_BUCKET"         = aws_s3_bucket.data_lake.bucket
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
