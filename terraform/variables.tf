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

variable "output_format" {
  type        = string
  default     = "parquet"
  description = "Raw Bronze data serialization format."
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

# Pre-existence safety toggles
variable "use_existing_s3_bucket" {
  type        = bool
  default     = false
  description = "If true, skips creating S3 bucket and reuses existing bucket."
}

variable "use_existing_iam_role" {
  type        = bool
  default     = false
  description = "If true, skips creating Glue IAM role and reuses existing role."
}

variable "use_existing_secrets" {
  type        = bool
  default     = false
  description = "If true, skips creating Secrets Manager secrets and reuses existing secrets."
}

variable "use_existing_glue_database" {
  type        = bool
  default     = false
  description = "If true, skips creating Glue database and reuses existing database."
}

variable "use_existing_athena_workgroup" {
  type        = bool
  default     = false
  description = "If true, skips creating Athena workgroup and reuses existing workgroup."
}

variable "use_existing_sns_topic" {
  type        = bool
  default     = false
  description = "If true, skips creating SNS topic and reuses existing SNS topic."
}

variable "use_existing_step_functions_role" {
  type        = bool
  default     = false
  description = "If true, skips creating Step Functions IAM role and reuses existing role."
}
