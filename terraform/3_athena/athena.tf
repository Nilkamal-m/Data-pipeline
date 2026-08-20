# ==============================================================================
# ATHENA TERRAFORM INFRASTRUCTURE (terraform/3_athena/athena.tf)
# ==============================================================================
# Self-contained Terraform script for Athena Query Engine Workgroup.
# Uses Enterprise Private Registry: cps-terraform.anthem.com/organization/*
# Contains all variables and Athena WorkGroup.
# Includes pre-existence check logic to skip creating resources if already present.
# ==============================================================================

provider "aws" {
  region = var.aws_region
}

# ------------------------------------------------------------------------------
# Athena Layer Variables (All necessary variables defined inside this file)
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
variable "use_existing_athena_workgroup" {
  type        = bool
  default     = false
  description = "If true, skips creating Athena workgroup and reuses existing workgroup."
}

locals {
  bucket_name    = "${var.data_lake_bucket_name}-${var.environment}"
  athena_wg_name = "uax_data_lake_workgroup_${var.environment}"
}

# ------------------------------------------------------------------------------
# 1. Amazon Athena WorkGroup using Enterprise Private Registry Module
# ------------------------------------------------------------------------------
module "athena_workgroup" {
  source = "cps-terraform.anthem.com/organization/athena/aws//modules/workgroup"

  create      = !var.use_existing_athena_workgroup
  name        = local.athena_wg_name
  description = "Dedicated Athena WorkGroup for querying Silver Apache Iceberg tables."
  state       = "ENABLED"

  configuration = {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration = {
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
# Athena Layer Outputs
# ------------------------------------------------------------------------------
output "athena_workgroup_name" {
  value       = local.athena_wg_name
  description = "Amazon Athena WorkGroup Name."
}
