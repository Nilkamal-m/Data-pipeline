# ==============================================================================
# AWS DATA PIPELINE - SINGLE STANDALONE TERRAFORM EXECUTABLE (main.tf)
# ==============================================================================
# Single self-contained file containing all Bronze, Silver, Athena, SNS,
# Step Functions State Machines, and EventBridge resources.
# Includes pre-existence checks to safely skip creating resources if already present.
# ==============================================================================

# ------------------------------------------------------------------------------
# 1. Terraform Provider Setup & Data Sources
# ------------------------------------------------------------------------------
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
# 2. Embedded Variables (All necessary variables defined inside this single file)
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
  default     = "uax-data-pipeline"
  description = "Application name prefix for resources."
}

variable "data_lake_bucket_name" {
  type        = string
  default     = "uax-data-lake-bucket"
  description = "Base S3 data lake bucket name (environment suffix will be appended)."
}

variable "alert_email_address" {
  type        = string
  default     = "data-eng-alerts@company.com"
  description = "Email address to receive pipeline execution success/failure SNS alerts."
}

variable "schedule_expression" {
  type        = string
  default     = "cron(0 6 * * ? *)"
  description = "EventBridge cron expression for automated pipeline execution."
}

variable "output_format" {
  type        = string
  default     = "parquet"
  description = "Raw Bronze data serialization format (parquet or json)."
}

# Pre-existence Safety Toggles (Set to true if service already exists to skip creation)
variable "use_existing_s3_bucket" {
  type        = bool
  default     = false
  description = "If true, skips creating S3 bucket and uses existing bucket name."
}

variable "use_existing_iam_role" {
  type        = bool
  default     = false
  description = "If true, skips creating Glue IAM role and uses existing role."
}

variable "use_existing_glue_database" {
  type        = bool
  default     = false
  description = "If true, skips creating Glue database and uses existing database."
}

variable "use_existing_athena_workgroup" {
  type        = bool
  default     = false
  description = "If true, skips creating Athena workgroup and uses existing workgroup."
}


# ------------------------------------------------------------------------------
# 3. Dynamic Local Variables (Resolves ARNs and Names cleanly)
# ------------------------------------------------------------------------------
locals {
  bucket_name       = var.use_existing_s3_bucket ? "${var.data_lake_bucket_name}-${var.environment}" : (length(aws_s3_bucket.data_lake) > 0 ? aws_s3_bucket.data_lake[0].bucket : "${var.data_lake_bucket_name}-${var.environment}")
  bucket_arn        = var.use_existing_s3_bucket ? "arn:aws:s3:::${var.data_lake_bucket_name}-${var.environment}" : (length(aws_s3_bucket.data_lake) > 0 ? aws_s3_bucket.data_lake[0].arn : "arn:aws:s3:::${var.data_lake_bucket_name}-${var.environment}")
  glue_role_arn     = var.use_existing_iam_role ? "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${var.app_name}-glue-execution-role-${var.environment}" : (length(aws_iam_role.glue_execution_role) > 0 ? aws_iam_role.glue_execution_role[0].arn : "")
  glue_db_name      = var.use_existing_glue_database ? "uax_data_lake_db_${var.environment}" : (length(aws_glue_catalog_database.silver_db) > 0 ? aws_glue_catalog_database.silver_db[0].name : "uax_data_lake_db_${var.environment}")
  athena_wg_name    = var.use_existing_athena_workgroup ? "uax_data_lake_workgroup_${var.environment}" : (length(aws_athena_workgroup.data_pipeline) > 0 ? aws_athena_workgroup.data_pipeline[0].name : "uax_data_lake_workgroup_${var.environment}")
}


# ------------------------------------------------------------------------------
# 4. PART 1: S3 Data Lake Bucket & Folder Prefixes (Bronze & Silver)
# ------------------------------------------------------------------------------
resource "aws_s3_bucket" "data_lake" {
  count         = var.use_existing_s3_bucket ? 0 : 1
  bucket        = "${var.data_lake_bucket_name}-${var.environment}"
  force_destroy = false

  tags = {
    Environment = var.environment
    Application = var.app_name
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

# S3 Folder Objects
resource "aws_s3_object" "folder_bronze" {
  bucket = local.bucket_name
  key    = "bronze/"
}

resource "aws_s3_object" "folder_silver" {
  bucket = local.bucket_name
  key    = "silver/"
}

resource "aws_s3_object" "folder_metadata" {
  bucket = local.bucket_name
  key    = "metadata/"
}

resource "aws_s3_object" "folder_athena_results" {
  bucket = local.bucket_name
  key    = "athena-results/"
}

resource "aws_s3_object" "folder_bronze_script" {
  bucket = local.bucket_name
  key    = "bronze/script/"
}

resource "aws_s3_object" "folder_silver_script" {
  bucket = local.bucket_name
  key    = "silver/script/"
}


# ------------------------------------------------------------------------------
# 5. Secrets Manager (REST API Credentials)
# ------------------------------------------------------------------------------
resource "aws_secretsmanager_secret" "servicenow_secret" {
  name        = "data-lake/servicenow-credentials-${var.environment}"
  description = "ServiceNow OAuth 2.0 credentials (Password/Client Credentials flow)."

  tags = {
    Environment = var.environment
    Application = var.app_name
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
# 6. IAM Roles & Policies (Glue, Step Functions, EventBridge)
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
# 7. BRONZE LAYER: AWS Glue Python Shell Ingestion Job
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
# 8. SILVER LAYER: Glue Database, Iceberg Crawler, & PySpark Iceberg ETL Job
# ------------------------------------------------------------------------------
resource "aws_glue_catalog_database" "silver_db" {
  count       = var.use_existing_glue_database ? 0 : 1
  name        = "uax_data_lake_db_${var.environment}"
  description = "AWS Glue Data Catalog Database for Silver Layer Apache Iceberg Tables."
}

resource "aws_glue_crawler" "silver_iceberg_crawler" {
  name          = "${var.app_name}-silver-iceberg-crawler-${var.environment}"
  database_name = local.glue_db_name
  role          = local.glue_role_arn
  description   = "Crawls Silver Layer Apache Iceberg tables into AWS Glue Data Catalog."

  s3_target {
    path = "s3://${local.bucket_name}/silver/"
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }

  tags = {
    Environment = var.environment
    Application = var.app_name
    Layer       = "Silver"
    ManagedBy   = "Terraform"
  }
}

resource "aws_glue_job" "silver_iceberg_job" {
  name              = "${var.app_name}-silver-iceberg-etl-${var.environment}"
  description       = "AWS Glue PySpark ETL Job transforming Bronze raw data into Silver Apache Iceberg tables."
  role_arn          = local.glue_role_arn
  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"

  command {
    name            = "glueetl"
    script_location = "s3://${local.bucket_name}/silver/script/silver_iceberg_etl.py"
  }

  default_arguments = {
    "--extra-py-files"          = "s3://${local.bucket_name}/silver/script/silver_config_loader.py,s3://${local.bucket_name}/silver/script/transformer.py"
    "--SILVER_CONFIG_S3_PATH"  = "s3://${local.bucket_name}/silver/script/config/silver_config.json"
    "--datalake-formats"        = "iceberg"
    "--conf"                    = "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    "--DATA_LAKE_BUCKET"        = local.bucket_name
    "--GLUE_DATABASE"           = local.glue_db_name
    "--job-language"            = "python"
  }

  tags = {
    Environment = var.environment
    Application = var.app_name
    Layer       = "Silver"
    ManagedBy   = "Terraform"
  }

  depends_on = [
    aws_glue_job.bronze_ingestion_job
  ]
}


# ------------------------------------------------------------------------------
# 9. ATHENA WORKGROUP: Dedicated Query WorkGroup
# ------------------------------------------------------------------------------
resource "aws_athena_workgroup" "data_pipeline" {
  count       = var.use_existing_athena_workgroup ? 0 : 1
  name        = "uax_data_lake_workgroup_${var.environment}"
  description = "Dedicated Athena WorkGroup for querying Silver Apache Iceberg tables."
  state       = "ENABLED"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${local.bucket_name}/athena-results/"
    }
  }

  tags = {
    Environment = var.environment
    Application = var.app_name
    Layer       = "Athena"
    ManagedBy   = "Terraform"
  }
}


# ------------------------------------------------------------------------------
# 10. SNS ALERT TOPIC & SUBSCRIPTION
# ------------------------------------------------------------------------------
resource "aws_sns_topic" "pipeline_alerts" {
  name        = "${var.app_name}-alerts-topic-${var.environment}"
  description = "SNS Alert Topic for Data Pipeline success/failure notifications."

  tags = {
    Environment = var.environment
    Application = var.app_name
    ManagedBy   = "Terraform"
  }
}

resource "aws_sns_topic_subscription" "email_alert" {
  topic_arn = aws_sns_topic.pipeline_alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email_address
}


# ------------------------------------------------------------------------------
# 11. STEP FUNCTIONS & EVENTBRIDGE: Orchestration State Machines & Cron Schedules
# ------------------------------------------------------------------------------
resource "aws_iam_role" "step_functions_role" {
  name = "${var.app_name}-stepfunctions-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "states.amazonaws.com"
        }
      }
    ]
  })

  tags = {
    Environment = var.environment
    Application = var.app_name
    Layer       = "Orchestration"
    ManagedBy   = "Terraform"
  }
}

resource "aws_iam_policy" "step_functions_policy" {
  name        = "${var.app_name}-stepfunctions-policy-${var.environment}"
  description = "Execution policy for AWS Step Functions orchestrating Glue jobs and Crawlers."

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:StartJobRun",
          "glue:GetJobRun",
          "glue:GetJobRuns",
          "glue:BatchStopJobRun",
          "glue:StartCrawler",
          "glue:GetCrawler"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "sns:Publish"
        ]
        Resource = [
          aws_sns_topic.pipeline_alerts.arn
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogDelivery",
          "logs:GetLogDelivery",
          "logs:UpdateLogDelivery",
          "logs:DeleteLogDelivery",
          "logs:ListLogDeliveries",
          "logs:PutResourcePolicy",
          "logs:DescribeResourcePolicies",
          "logs:DescribeLogGroups"
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "step_functions_policy_attach" {
  role       = aws_iam_role.step_functions_role.name
  policy_arn = aws_iam_policy.step_functions_policy.arn
}

resource "aws_iam_role" "eventbridge_role" {
  name = "${var.app_name}-eventbridge-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "events.amazonaws.com"
        }
      }
    ]
  })

  tags = {
    Environment = var.environment
    Application = var.app_name
    Layer       = "Orchestration"
    ManagedBy   = "Terraform"
  }
}

resource "aws_iam_policy" "eventbridge_policy" {
  name        = "${var.app_name}-eventbridge-policy-${var.environment}"
  description = "Execution policy for EventBridge triggering Step Functions state machines."

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "states:StartExecution"
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "eventbridge_policy_attach" {
  role       = aws_iam_role.eventbridge_role.name
  policy_arn = aws_iam_policy.eventbridge_policy.arn
}

# ServiceNow State Machine
resource "aws_sfn_state_machine" "servicenow_orchestrator" {
  name     = "${var.app_name}-servicenow-orchestrator-${var.environment}"
  role_arn = aws_iam_role.step_functions_role.arn

  definition = jsonencode({
    Comment = "Orchestrates ServiceNow Ingestion -> Silver Iceberg ETL -> Glue Crawler Sync -> SNS Alert"
    StartAt = "TriggerBronzeIngestion"
    States = {
      TriggerBronzeIngestion = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.bronze_ingestion_job.name
          Arguments = {
            "--SOURCE_SYSTEM"   = "servicenow"
            "--TABLE_NAME"      = "incident,change_request,problem,sys_user"
            "--SECRET_NAME"     = aws_secretsmanager_secret.servicenow_secret.name
            "--CONFIG_S3_PATH"  = "s3://${local.bucket_name}/bronze/script/config/bronze_config.json"
          }
        }
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            Next        = "SendFailureAlert"
          }
        ]
        Next = "TriggerSilverIcebergETL"
      }

      TriggerSilverIcebergETL = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.silver_iceberg_job.name
          Arguments = {
            "--SOURCE_SYSTEM"          = "servicenow"
            "--TABLE_NAME"             = "incident,change_request,problem,sys_user"
            "--SILVER_CONFIG_S3_PATH" = "s3://${local.bucket_name}/silver/script/config/silver_config.json"
          }
        }
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            Next        = "SendFailureAlert"
          }
        ]
        Next = "TriggerSilverCrawler"
      }

      TriggerSilverCrawler = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startCrawler"
        Parameters = {
          Name = aws_glue_crawler.silver_iceberg_crawler.name
        }
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            Next        = "SendFailureAlert"
          }
        ]
        Next = "WaitForCrawler"
      }

      WaitForCrawler = {
        Type    = "Wait"
        Seconds = 15
        Next    = "GetCrawlerStatus"
      }

      GetCrawlerStatus = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:glue:getCrawler"
        Parameters = {
          Name = aws_glue_crawler.silver_iceberg_crawler.name
        }
        Next = "CheckCrawlerRunning"
      }

      CheckCrawlerRunning = {
        Type = "Choice"
        Choices = [
          {
            Variable     = "$.Crawler.State"
            StringEquals = "RUNNING"
            Next         = "WaitForCrawler"
          }
        ]
        Default = "SendSuccessAlert"
      }

      SendSuccessAlert = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:sns:publish"
        Parameters = {
          TopicArn = aws_sns_topic.pipeline_alerts.arn
          Subject  = "SUCCESS: ServiceNow Pipeline Completed (${var.environment})"
          Message  = "ServiceNow Bronze Ingestion, Silver Iceberg ETL, and Glue Crawler Sync completed successfully."
        }
        End = true
      }

      SendFailureAlert = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:sns:publish"
        Parameters = {
          TopicArn = aws_sns_topic.pipeline_alerts.arn
          Subject  = "FAILURE: ServiceNow Pipeline Failed (${var.environment})"
          Message  = "ServiceNow Pipeline failed during execution. Inspect AWS Glue Logs or Step Functions execution history."
        }
        Fail = true
      }
    }
  })

  tags = {
    Environment = var.environment
    Application = var.app_name
    Layer       = "Orchestration"
    ManagedBy   = "Terraform"
  }

  depends_on = [
    aws_glue_job.bronze_ingestion_job,
    aws_glue_job.silver_iceberg_job,
    aws_glue_crawler.silver_iceberg_crawler
  ]
}

# Moveworks State Machine
resource "aws_sfn_state_machine" "moveworks_orchestrator" {
  name     = "${var.app_name}-moveworks-orchestrator-${var.environment}"
  role_arn = aws_iam_role.step_functions_role.arn

  definition = jsonencode({
    Comment = "Orchestrates Moveworks Ingestion -> Silver Iceberg ETL -> Glue Crawler Sync -> SNS Alert"
    StartAt = "TriggerBronzeIngestion"
    States = {
      TriggerBronzeIngestion = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.bronze_ingestion_job.name
          Arguments = {
            "--SOURCE_SYSTEM"   = "moveworks"
            "--TABLE_NAME"      = "interactions,users,tickets"
            "--SECRET_NAME"     = aws_secretsmanager_secret.moveworks_secret.name
            "--CONFIG_S3_PATH"  = "s3://${local.bucket_name}/bronze/script/config/bronze_config.json"
          }
        }
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            Next        = "SendFailureAlert"
          }
        ]
        Next = "TriggerSilverIcebergETL"
      }

      TriggerSilverIcebergETL = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.silver_iceberg_job.name
          Arguments = {
            "--SOURCE_SYSTEM"          = "moveworks"
            "--TABLE_NAME"             = "interactions,users,tickets"
            "--SILVER_CONFIG_S3_PATH" = "s3://${local.bucket_name}/silver/script/config/silver_config.json"
          }
        }
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            Next        = "SendFailureAlert"
          }
        ]
        Next = "TriggerSilverCrawler"
      }

      TriggerSilverCrawler = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startCrawler"
        Parameters = {
          Name = aws_glue_crawler.silver_iceberg_crawler.name
        }
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            Next        = "SendFailureAlert"
          }
        ]
        Next = "WaitForCrawler"
      }

      WaitForCrawler = {
        Type    = "Wait"
        Seconds = 15
        Next    = "GetCrawlerStatus"
      }

      GetCrawlerStatus = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:glue:getCrawler"
        Parameters = {
          Name = aws_glue_crawler.silver_iceberg_crawler.name
        }
        Next = "CheckCrawlerRunning"
      }

      CheckCrawlerRunning = {
        Type = "Choice"
        Choices = [
          {
            Variable     = "$.Crawler.State"
            StringEquals = "RUNNING"
            Next         = "WaitForCrawler"
          }
        ]
        Default = "SendSuccessAlert"
      }

      SendSuccessAlert = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:sns:publish"
        Parameters = {
          TopicArn = aws_sns_topic.pipeline_alerts.arn
          Subject  = "SUCCESS: Moveworks Pipeline Completed (${var.environment})"
          Message  = "Moveworks Bronze Ingestion, Silver Iceberg ETL, and Glue Crawler Sync completed successfully."
        }
        End = true
      }

      SendFailureAlert = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:sns:publish"
        Parameters = {
          TopicArn = aws_sns_topic.pipeline_alerts.arn
          Subject  = "FAILURE: Moveworks Pipeline Failed (${var.environment})"
          Message  = "Moveworks Pipeline failed during execution. Inspect AWS Glue Logs or Step Functions execution history."
        }
        Fail = true
      }
    }
  })

  tags = {
    Environment = var.environment
    Application = var.app_name
    Layer       = "Orchestration"
    ManagedBy   = "Terraform"
  }

  depends_on = [
    aws_glue_job.bronze_ingestion_job,
    aws_glue_job.silver_iceberg_job,
    aws_glue_crawler.silver_iceberg_crawler
  ]
}

# Genesys State Machine
resource "aws_sfn_state_machine" "genesys_orchestrator" {
  name     = "${var.app_name}-genesys-orchestrator-${var.environment}"
  role_arn = aws_iam_role.step_functions_role.arn

  definition = jsonencode({
    Comment = "Orchestrates Genesys Ingestion -> Silver Iceberg ETL -> Glue Crawler Sync -> SNS Alert"
    StartAt = "TriggerBronzeIngestion"
    States = {
      TriggerBronzeIngestion = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.bronze_ingestion_job.name
          Arguments = {
            "--SOURCE_SYSTEM"   = "genesys"
            "--TABLE_NAME"      = "conversations,users,queues"
            "--SECRET_NAME"     = aws_secretsmanager_secret.genesys_secret.name
            "--CONFIG_S3_PATH"  = "s3://${local.bucket_name}/bronze/script/config/bronze_config.json"
          }
        }
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            Next        = "SendFailureAlert"
          }
        ]
        Next = "TriggerSilverIcebergETL"
      }

      TriggerSilverIcebergETL = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.silver_iceberg_job.name
          Arguments = {
            "--SOURCE_SYSTEM"          = "genesys"
            "--TABLE_NAME"             = "conversations,users,queues"
            "--SILVER_CONFIG_S3_PATH" = "s3://${local.bucket_name}/silver/script/config/silver_config.json"
          }
        }
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            Next        = "SendFailureAlert"
          }
        ]
        Next = "TriggerSilverCrawler"
      }

      TriggerSilverCrawler = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startCrawler"
        Parameters = {
          Name = aws_glue_crawler.silver_iceberg_crawler.name
        }
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            Next        = "SendFailureAlert"
          }
        ]
        Next = "WaitForCrawler"
      }

      WaitForCrawler = {
        Type    = "Wait"
        Seconds = 15
        Next    = "GetCrawlerStatus"
      }

      GetCrawlerStatus = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:glue:getCrawler"
        Parameters = {
          Name = aws_glue_crawler.silver_iceberg_crawler.name
        }
        Next = "CheckCrawlerRunning"
      }

      CheckCrawlerRunning = {
        Type = "Choice"
        Choices = [
          {
            Variable     = "$.Crawler.State"
            StringEquals = "RUNNING"
            Next         = "WaitForCrawler"
          }
        ]
        Default = "SendSuccessAlert"
      }

      SendSuccessAlert = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:sns:publish"
        Parameters = {
          TopicArn = aws_sns_topic.pipeline_alerts.arn
          Subject  = "SUCCESS: Genesys Pipeline Completed (${var.environment})"
          Message  = "Genesys Bronze Ingestion, Silver Iceberg ETL, and Glue Crawler Sync completed successfully."
        }
        End = true
      }

      SendFailureAlert = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:sns:publish"
        Parameters = {
          TopicArn = aws_sns_topic.pipeline_alerts.arn
          Subject  = "FAILURE: Genesys Pipeline Failed (${var.environment})"
          Message  = "Genesys Pipeline failed during execution. Inspect AWS Glue Logs or Step Functions execution history."
        }
        Fail = true
      }
    }
  })

  tags = {
    Environment = var.environment
    Application = var.app_name
    Layer       = "Orchestration"
    ManagedBy   = "Terraform"
  }

  depends_on = [
    aws_glue_job.bronze_ingestion_job,
    aws_glue_job.silver_iceberg_job,
    aws_glue_crawler.silver_iceberg_crawler
  ]
}

# EventBridge Cron Rules & Targets
resource "aws_cloudwatch_event_rule" "servicenow_cron" {
  name                = "${var.app_name}-servicenow-schedule-${var.environment}"
  description         = "Triggers ServiceNow pipeline state machine on schedule."
  schedule_expression = var.schedule_expression
  state               = "ENABLED"
}

resource "aws_cloudwatch_event_target" "servicenow_target" {
  rule      = aws_cloudwatch_event_rule.servicenow_cron.name
  target_id = "TriggerServiceNowStateMachine"
  arn       = aws_sfn_state_machine.servicenow_orchestrator.arn
  role_arn  = aws_iam_role.eventbridge_role.arn
}

resource "aws_cloudwatch_event_rule" "moveworks_cron" {
  name                = "${var.app_name}-moveworks-schedule-${var.environment}"
  description         = "Triggers Moveworks pipeline state machine on schedule."
  schedule_expression = var.schedule_expression
  state               = "ENABLED"
}

resource "aws_cloudwatch_event_target" "moveworks_target" {
  rule      = aws_cloudwatch_event_rule.moveworks_cron.name
  target_id = "TriggerMoveworksStateMachine"
  arn       = aws_sfn_state_machine.moveworks_orchestrator.arn
  role_arn  = aws_iam_role.eventbridge_role.arn
}

resource "aws_cloudwatch_event_rule" "genesys_cron" {
  name                = "${var.app_name}-genesys-schedule-${var.environment}"
  description         = "Triggers Genesys pipeline state machine on schedule."
  schedule_expression = var.schedule_expression
  state               = "ENABLED"
}

resource "aws_cloudwatch_event_target" "genesys_target" {
  rule      = aws_cloudwatch_event_rule.genesys_cron.name
  target_id = "TriggerGenesysStateMachine"
  arn       = aws_sfn_state_machine.genesys_orchestrator.arn
  role_arn  = aws_iam_role.eventbridge_role.arn
}


# ------------------------------------------------------------------------------
# 12. Outputs
# ------------------------------------------------------------------------------
output "data_lake_s3_bucket_name" {
  value       = local.bucket_name
  description = "Single S3 Data Lake bucket name."
}

output "glue_bronze_ingestion_job_name" {
  value       = aws_glue_job.bronze_ingestion_job.name
  description = "AWS Glue Python Shell Bronze Ingestion Job Name."
}

output "glue_silver_iceberg_job_name" {
  value       = aws_glue_job.silver_iceberg_job.name
  description = "AWS Glue PySpark Silver Iceberg ETL Job Name."
}

output "glue_catalog_database_name" {
  value       = local.glue_db_name
  description = "AWS Glue Data Catalog Database Name for Silver Iceberg Tables."
}

output "athena_workgroup_name" {
  value       = local.athena_wg_name
  description = "Amazon Athena WorkGroup Name."
}

output "sns_alert_topic_arn" {
  value       = aws_sns_topic.pipeline_alerts.arn
  description = "Amazon SNS Alert Topic ARN."
}

output "servicenow_state_machine_arn" {
  value       = aws_sfn_state_machine.servicenow_orchestrator.arn
  description = "ServiceNow Step Functions State Machine ARN."
}

output "moveworks_state_machine_arn" {
  value       = aws_sfn_state_machine.moveworks_orchestrator.arn
  description = "Moveworks Step Functions State Machine ARN."
}

output "genesys_state_machine_arn" {
  value       = aws_sfn_state_machine.genesys_orchestrator.arn
  description = "Genesys Step Functions State Machine ARN."
}
