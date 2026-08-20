# ==============================================================================
# FILE 3: ATHENA TERRAFORM INFRASTRUCTURE (3_athena.tf)
# ==============================================================================
# Includes:
# - Athena Results S3 Folder Structure Placeholder
# - Amazon Athena Dedicated WorkGroup
# - Pre-existence skip logic (use_existing_athena_workgroup)
# ==============================================================================

locals {
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
