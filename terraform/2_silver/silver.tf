# ==============================================================================
# SILVER LAYER TERRAFORM INFRASTRUCTURE (terraform/2_silver/silver.tf)
# ==============================================================================
# Self-contained Terraform script for Silver Iceberg ETL Layer.
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

# ------------------------------------------------------------------------------
# RDS MySQL Network Variables (Injected via tfvars or pipeline parameters)
# ------------------------------------------------------------------------------
variable "rds_security_group_id" {
  type        = string
  default     = ""
  description = "Security Group ID of the target RDS MySQL instance."
}

variable "rds_subnet_id" {
  type        = string
  default     = ""
  description = "Private Subnet ID where Glue ENI will be deployed."
}

variable "rds_availability_zone" {
  type        = string
  default     = ""
  description = "Availability Zone of the target RDS MySQL instance."
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
# 2. AWS Glue Iceberg Crawler using Enterprise Private Registry Module
# ------------------------------------------------------------------------------
module "silver_iceberg_crawler" {
  source = "glue/aws//modules/crawler"

  name          = "${var.app_name}-silver-iceberg-crawler-${var.environment}"
  database_name = local.glue_db_name
  role          = local.glue_role_arn
  description   = "Crawls Silver Layer Apache Iceberg tables into AWS Glue Data Catalog."

  s3_target = [
    {
      path = "s3://${local.bucket_name}/silver/data/"
    }
  ]

  table_prefix = "tbl_"

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
# 3. AWS Glue Connection for Private RDS MySQL (Anthem Private Registry Module)
# ------------------------------------------------------------------------------
module "glue_connection" {
  source = "cps-terraform.anthem.com/DIG/terraform-aws-glue-connection/aws"

  create_new_connection = true
  name                  = "${var.app_name}-rds-connection-${var.environment}"
  description           = "AWS Glue connection for RDS MySQL serving layer"

  physical_connection_requirements = [
    {
      availability_zone      = var.rds_availability_zone
      subnet_id              = var.rds_subnet_id
      security_group_id_list = [var.rds_security_group_id]
    }
  ]
}

# ------------------------------------------------------------------------------
# 4. AWS Glue PySpark Apache Iceberg & Gold ETL Job
# ------------------------------------------------------------------------------
module "silver_iceberg_job" {
  source = "glue/aws//modules/job"

  name              = "${var.app_name}-silver-etl-${var.environment}"
  description       = "AWS Glue PySpark ETL Job transforming Bronze raw data into Silver Apache Iceberg tables and Gold marts."
  role_arn          = local.glue_role_arn
  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"

  # Attach Glue VPC Connection for RDS MySQL access
  connections = [module.glue_connection.name]

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

output "glue_silver_iceberg_crawler_name" {
  value       = "${var.app_name}-silver-iceberg-crawler-${var.environment}"
  description = "AWS Glue Silver Iceberg Crawler Name."
}

output "glue_connection_name" {
  value       = module.glue_connection.name
  description = "AWS Glue Connection Name for RDS MySQL."
}

output "glue_silver_iceberg_job_name" {
  value       = "${var.app_name}-silver-etl-${var.environment}"
  description = "AWS Glue PySpark Silver Iceberg ETL Job Name."
}

output "glue_silver_job_name" {
  value       = "${var.app_name}-silver-etl-${var.environment}"
  description = "AWS Glue PySpark Silver ETL Job Name."
}

