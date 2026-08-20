# ==============================================================================
# SILVER LAYER TERRAFORM INFRASTRUCTURE (terraform/2_silver/silver.tf)
# ==============================================================================
# Self-contained Terraform script for Silver Iceberg ETL Layer.
# Contains all variables, Glue Catalog DB, Iceberg Crawler, and PySpark ETL Job.
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
# Silver Layer Variables (All necessary variables defined inside this file)
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

# Pre-existence safety toggles
variable "use_existing_glue_database" {
  type        = bool
  default     = false
  description = "If true, skips creating Glue database and reuses existing database."
}

locals {
  bucket_name = "${var.data_lake_bucket_name}-${var.environment}"
  # Reuses the single shared IAM Glue Execution Role created in 1_bronze/bronze.tf
  glue_role_arn = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${var.app_name}-glue-execution-role-${var.environment}"
  glue_db_name  = var.use_existing_glue_database ? "uax_data_lake_db_${var.environment}" : (length(aws_glue_catalog_database.silver_db) > 0 ? aws_glue_catalog_database.silver_db[0].name : "uax_data_lake_db_${var.environment}")
}


# ------------------------------------------------------------------------------
# 1. Silver S3 Folder Structure Placeholders
# ------------------------------------------------------------------------------
resource "aws_s3_object" "folder_silver" {
  bucket = local.bucket_name
  key    = "silver/"
}

resource "aws_s3_object" "folder_silver_script" {
  bucket = local.bucket_name
  key    = "silver/script/"
}


# ------------------------------------------------------------------------------
# 2. AWS Glue Data Catalog Database & Iceberg Crawler (Skips creation if use_existing_glue_database = true)
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


# ------------------------------------------------------------------------------
# 3. AWS Glue PySpark Apache Iceberg ETL Job (Silver)
# ------------------------------------------------------------------------------
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
}

# ------------------------------------------------------------------------------
# Silver Layer Outputs
# ------------------------------------------------------------------------------
output "glue_silver_iceberg_job_name" {
  value       = aws_glue_job.silver_iceberg_job.name
  description = "AWS Glue PySpark Silver Iceberg ETL Job Name."
}

output "glue_catalog_database_name" {
  value       = local.glue_db_name
  description = "AWS Glue Data Catalog Database Name for Silver Iceberg Tables."
}
