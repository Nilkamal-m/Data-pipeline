# ==============================================================================
# ATHENA TERRAFORM INFRASTRUCTURE (terraform/3_athena/athena.tf)
# ==============================================================================
# Self-contained Terraform script for Athena Query Engine Workgroup.
# Contains all variables, S3 results prefix, and Athena WorkGroup.
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
  athena_wg_name = var.use_existing_athena_workgroup ? "uax_data_lake_workgroup_${var.environment}" : (length(aws_athena_workgroup.data_pipeline) > 0 ? aws_athena_workgroup.data_pipeline[0].name : "uax_data_lake_workgroup_${var.environment}")
}

# ------------------------------------------------------------------------------
# 1. Athena Results S3 Folder Structure Placeholder
# ------------------------------------------------------------------------------
resource "aws_s3_object" "folder_athena_results" {
  bucket = local.bucket_name
  key    = "athena-results/"
}


# ------------------------------------------------------------------------------
# 2. Amazon Athena WorkGroup (Skips creation if use_existing_athena_workgroup = true)
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
# Athena Layer Outputs
# ------------------------------------------------------------------------------
output "athena_workgroup_name" {
  value       = local.athena_wg_name
  description = "Amazon Athena WorkGroup Name."
}
