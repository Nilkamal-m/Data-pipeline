# ==============================================================================
# SILVER LAYER TERRAFORM INFRASTRUCTURE (terraform/2_silver/silver.tf)
# ==============================================================================
# Self-contained Terraform script for Silver Iceberg ETL Layer.
# Uses Enterprise Private Registry: cps-terraform.anthem.com/organization/*
# Contains all variables, Glue Catalog DB, Iceberg Crawler, and PySpark ETL Job.
# Includes pre-existence check logic to skip creating resources if already present.
# ==============================================================================

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
  description = "Deployment environment stage (dev, prod)."
}

variable "app_name" {
  type        = string
  default     = "uax-datalake"
  description = "Application name prefix for resources."
}

# Pre-existence safety toggles
variable "use_existing_glue_database" {
  type        = bool
  default     = false
  description = "If true, skips creating Glue database and reuses existing database."
}

locals {
  bucket_name   = "${var.app_name}-${var.environment}-bucket"
  glue_role_arn = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${var.app_name}-glue-execution-role-${var.environment}"
  glue_db_name  = "${var.app_name}-db-${var.environment}"
}

# ------------------------------------------------------------------------------
# 1. AWS Glue Data Catalog Database using Enterprise Private Registry Module
# ------------------------------------------------------------------------------
module "silver_glue_catalog_database" {
  source = "cps-terraform.anthem.com/organization/glue/aws//modules/catalog-database"

  create      = !var.use_existing_glue_database
  name        = local.glue_db_name
  description = "AWS Glue Data Catalog Database for Silver Layer Apache Iceberg Tables."
}

# ------------------------------------------------------------------------------
# 2. AWS Glue Iceberg Crawler using Enterprise Private Registry Module
# ------------------------------------------------------------------------------
module "silver_iceberg_crawler" {
  source = "cps-terraform.anthem.com/organization/glue/aws//modules/crawler"

  name          = "${var.app_name}-silver-iceberg-crawler-${var.environment}"
  database_name = local.glue_db_name
  role          = local.glue_role_arn
  description   = "Crawls Silver Layer Apache Iceberg tables into AWS Glue Data Catalog."

  s3_target = [
    {
      path = "s3://${local.bucket_name}/silver/"
    }
  ]

  schema_change_policy = {
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
# 3. AWS Glue PySpark Apache Iceberg ETL Job using Enterprise Private Registry Module
# ------------------------------------------------------------------------------
module "silver_iceberg_job" {
  source = "cps-terraform.anthem.com/organization/glue/aws//modules/job"

  name              = "${var.app_name}-silver-iceberg-etl-${var.environment}"
  description       = "AWS Glue PySpark ETL Job transforming Bronze raw data into Silver Apache Iceberg tables."
  role_arn          = local.glue_role_arn
  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"

  command = {
    name            = "glueetl"
    script_location = "s3://${local.bucket_name}/silver/script/silver_iceberg_etl.py"
  }

  default_arguments = {
    "--extra-py-files"         = "s3://${local.bucket_name}/silver/script/silver_config_loader.py,s3://${local.bucket_name}/silver/script/transformer.py"
    "--SILVER_CONFIG_S3_PATH" = "s3://${local.bucket_name}/silver/script/config/silver_config.json"
    "--datalake-formats"       = "iceberg"
    "--conf"                   = "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    "--DATA_LAKE_BUCKET"       = local.bucket_name
    "--GLUE_DATABASE"          = local.glue_db_name
    "--job-language"           = "python"
  }

  tags = {
    Environment = var.environment
    Application = var.app_name
    Layer       = "Silver"
    ManagedBy   = "Terraform"
  }
}

# ------------------------------------------------------------------------------
# Silver Layer Outputs (Prints details for all created services)
# ------------------------------------------------------------------------------
output "glue_catalog_database_name" {
  value       = local.glue_db_name
  description = "AWS Glue Data Catalog Database Name for Silver Iceberg Tables."
}

output "glue_silver_iceberg_crawler_name" {
  value       = "${var.app_name}-silver-iceberg-crawler-${var.environment}"
  description = "AWS Glue Silver Iceberg Crawler Name."
}

output "glue_silver_iceberg_job_name" {
  value       = "${var.app_name}-silver-iceberg-etl-${var.environment}"
  description = "AWS Glue PySpark Silver Iceberg ETL Job Name."
}
