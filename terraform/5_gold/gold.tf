# ==============================================================================
# GOLD LAYER TERRAFORM INFRASTRUCTURE (terraform/5_gold/gold.tf)
# ==============================================================================
# Self-contained Terraform script for Gold Serving Layer and Analytical Marts.
# Contains all variables, RDS VPC Glue Connection, and PySpark Gold Job.
# Reads Silver Apache Iceberg tables and publishes to RDS MySQL, Athena, etc.
# ==============================================================================

provider "aws" {
  region = var.aws_region
}

data "aws_caller_identity" "current" {}

# ------------------------------------------------------------------------------
# Gold Layer Variables
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
# 1. AWS Glue Connection for Private RDS MySQL (Anthem Private Registry Module)
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
# 2. AWS Glue PySpark Gold Analytical Marts Job
# ------------------------------------------------------------------------------
module "gold_marts_job" {
  source = "glue/aws//modules/job"

  name              = "${var.app_name}-gold-marts-${var.environment}"
  description       = "AWS Glue PySpark Job building Gold analytical marts from Silver Iceberg tables."
  role_arn          = local.glue_role_arn
  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"

  # Attach Glue VPC Connection for RDS MySQL access
  connections = [module.glue_connection.name]

  command = {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${local.bucket_name}/gold/script/gold_layer_manager.py"
  }

  default_arguments = {
    "--extra-py-files"        = "s3://${local.bucket_name}/gold/script/gold_config_loader.py,s3://${local.bucket_name}/gold/script/gold_initial_load.py,s3://${local.bucket_name}/gold/script/protegrity_encryption.py"
    "--GOLD_CONFIG_S3_PATH"  = "s3://${local.bucket_name}/gold/script/config/gold_config.json"
    "--datalake-formats"      = "iceberg"
    "--conf"                  = "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    "--DATA_LAKE_BUCKET"      = local.bucket_name
    "--GLUE_DATABASE"         = local.glue_db_name
    "--TABLE_PREFIX"          = "tbl_"
    "--RDS_GOLD_SCHEMA"       = "gold_reporting"
    "--CONNECTION_NAME"       = "${var.app_name}-rds-connection-${var.environment}"
    "--ATHENA_WORKGROUP"      = "primary"
    "--FULL_REFRESH"          = "false"
    "--job-language"          = "python"
  }

  tags = {
    Environment = var.environment
    Application = var.app_name
    Layer       = "Gold"
    ManagedBy   = "Terraform"
  }
}

# ------------------------------------------------------------------------------
# Gold Layer Outputs
# ------------------------------------------------------------------------------
output "glue_connection_name" {
  value       = module.glue_connection.name
  description = "AWS Glue Connection Name for RDS MySQL."
}

output "glue_gold_job_name" {
  value       = "${var.app_name}-gold-marts-${var.environment}"
  description = "AWS Glue PySpark Gold Analytical Marts Job Name."
}
