# ==============================================================================
# Terraform Main Infrastructure Resources
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

# ------------------------------------------------------------------------------
# 1. Single S3 Data Lake Bucket & Folder Prefixes
# ------------------------------------------------------------------------------
resource "aws_s3_bucket" "data_lake" {
  bucket        = "${var.data_lake_bucket_name}-${var.environment}"
  force_destroy = false

  tags = {
    Environment = var.environment
    Application = var.app_name
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

# S3 Partition Folder Placeholders
resource "aws_s3_object" "folder_bronze" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "bronze/"
}

resource "aws_s3_object" "folder_silver" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "silver/"
}

resource "aws_s3_object" "folder_metadata" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "metadata/"
}

resource "aws_s3_object" "folder_athena_results" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "athena-results/"
}

resource "aws_s3_object" "folder_bronze_script" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "bronze/script/"
}

resource "aws_s3_object" "folder_silver_script" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "silver/script/"
}


# ------------------------------------------------------------------------------
# 2. AWS Secrets Manager (OAuth 2.0 API Credentials Secrets)
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
# 3. AWS Glue Data Catalog & Crawler
# ------------------------------------------------------------------------------
resource "aws_glue_catalog_database" "silver_db" {
  name        = "uax_data_lake_db_${var.environment}"
  description = "AWS Glue Data Catalog Database for Silver Layer Apache Iceberg Tables."
}

resource "aws_glue_crawler" "silver_iceberg_crawler" {
  name          = "${var.app_name}-silver-iceberg-crawler-${var.environment}"
  database_name = aws_glue_catalog_database.silver_db.name
  role          = aws_iam_role.glue_execution_role.arn
  description   = "Crawls Silver Layer Apache Iceberg tables into AWS Glue Data Catalog."

  s3_target {
    path = "s3://${aws_s3_bucket.data_lake.bucket}/silver/"
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }

  tags = {
    Environment = var.environment
    Application = var.app_name
    ManagedBy   = "Terraform"
  }
}


# ------------------------------------------------------------------------------
# 4. AWS Glue Jobs
# ------------------------------------------------------------------------------

# Bronze REST API / RDBMS Python Shell Ingestion Job
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
    ManagedBy   = "Terraform"
  }
}

# Silver PySpark Apache Iceberg ETL Job
resource "aws_glue_job" "silver_iceberg_job" {
  name              = "${var.app_name}-silver-iceberg-etl-${var.environment}"
  description       = "AWS Glue PySpark ETL Job transforming Bronze raw data into Silver Apache Iceberg tables."
  role_arn          = aws_iam_role.glue_execution_role.arn
  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.data_lake.bucket}/silver/script/silver_iceberg_etl.py"
  }

  default_arguments = {
    "--extra-py-files"          = "s3://${aws_s3_bucket.data_lake.bucket}/silver/script/silver_config_loader.py,s3://${aws_s3_bucket.data_lake.bucket}/silver/script/transformer.py"
    "--SILVER_CONFIG_S3_PATH"  = "s3://${aws_s3_bucket.data_lake.bucket}/silver/script/config/silver_config.json"
    "--datalake-formats"        = "iceberg"
    "--conf"                    = "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    "--DATA_LAKE_BUCKET"        = aws_s3_bucket.data_lake.bucket
    "--GLUE_DATABASE"           = aws_glue_catalog_database.silver_db.name
    "--job-language"            = "python"
  }

  tags = {
    Environment = var.environment
    Application = var.app_name
    ManagedBy   = "Terraform"
  }
}


# ------------------------------------------------------------------------------
# 5. Amazon Athena WorkGroup
# ------------------------------------------------------------------------------
resource "aws_athena_workgroup" "data_pipeline" {
  name        = "uax_data_lake_workgroup_${var.environment}"
  description = "Dedicated Athena WorkGroup for querying Silver Apache Iceberg tables."
  state       = "ENABLED"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.data_lake.bucket}/athena-results/"
    }
  }

  tags = {
    Environment = var.environment
    Application = var.app_name
    ManagedBy   = "Terraform"
  }
}


# ------------------------------------------------------------------------------
# 6. Amazon SNS Alert Topic & Email Subscription
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
# 7. Amazon EventBridge Cron Rules & Targets
# ------------------------------------------------------------------------------

# ServiceNow Cron Rule
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

# Moveworks Cron Rule
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

# Genesys Cron Rule
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
