# ==============================================================================
# Global Terraform Variables
# ==============================================================================

variable "aws_region" {
  type        = string
  default     = "us-east-1"
  description = "AWS deployment region."
}

variable "environment" {
  type        = string
  default     = "dev"
  description = "Deployment environment stage (dev, staging, prod)."

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be one of: dev, staging, prod."
  }
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

variable "initial_load_date" {
  type        = string
  default     = "2024-01-01T00:00:00Z"
  description = "Fallback High-Water Mark ISO timestamp for initial pipeline execution."
}

variable "output_format" {
  type        = string
  default     = "parquet"
  description = "Raw Bronze data serialization format (parquet or json)."
}
