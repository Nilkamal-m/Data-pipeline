# ==============================================================================
# SILVER LAYER TERRAFORM INFRASTRUCTURE (terraform/2_silver/silver.tf)
# ==============================================================================
# Self-contained Terraform script for Silver Iceberg ETL Layer.
# Contains all variables, Glue Catalog DB, and PySpark Iceberg ETL Job.
# Includes pre-existence check logic to skip creating resources if already present.
# Note: AWS Glue Crawlers are eliminated — Apache Iceberg natively manages table
# metadata, snapshots, and schema evolution via SparkCatalog in AWS Glue Data Catalog.
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
  glue_db_name  = "${replace(var.app_name, "-", "_")}_db_${var.environment}"
}


# ------------------------------------------------------------------------------
# 1. AWS Glue Data Catalog Database using Enterprise Private Registry Module
# ------------------------------------------------------------------------------
module "silver_glue_catalog_database" {
  source = "glue/aws//modules/catalog-database"

  create      = !var.use_existing_glue_database
  name        = local.glue_db_name
  description = "AWS Glue Data Catalog Database for Silver Layer Apache Iceberg Tables."
}


# ------------------------------------------------------------------------------
# 3. AWS Glue PySpark Apache Iceberg ETL Job
# ------------------------------------------------------------------------------
module "silver_iceberg_job" {
  source = "glue/aws//modules/job"

  name              = "${var.app_name}-silver-etl-${var.environment}"
  description       = "AWS Glue PySpark ETL Job transforming Bronze raw data into Silver Apache Iceberg tables."
  role_arn          = local.glue_role_arn
  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"

  command = {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${local.bucket_name}/silver/script/uax_silver_etl.py"
  }

  default_arguments = {
    "--extra-py-files"         = "s3://${local.bucket_name}/silver/script/silver_config_loader.py,s3://${local.bucket_name}/silver/script/transformer.py"
    "--SILVER_CONFIG_S3_PATH" = "s3://${local.bucket_name}/silver/script/config/silver_config.json"
    "--datalake-formats"       = "iceberg"
    "--conf"                   = "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    "--DATA_LAKE_BUCKET"       = local.bucket_name
    "--GLUE_DATABASE"          = local.glue_db_name
    "--TABLE_PREFIX"           = "tbl_"
    "--WATERMARK_TABLE_NAME"   = "tbl_watermarks"
    "--FULL_REFRESH"           = "false"
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


output "glue_silver_iceberg_job_name" {
  value       = "${var.app_name}-silver-etl-${var.environment}"
  description = "AWS Glue PySpark Silver Iceberg ETL Job Name."
}

output "glue_silver_job_name" {
  value       = "${var.app_name}-silver-etl-${var.environment}"
  description = "AWS Glue PySpark Silver ETL Job Name."
}

