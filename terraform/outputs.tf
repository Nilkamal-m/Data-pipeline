# ==============================================================================
# Terraform Outputs
# ==============================================================================

output "data_lake_s3_bucket_name" {
  value       = local.bucket_name
  description = "Single S3 Data Lake bucket name."
}

output "glue_bronze_ingestion_job_name" {
  value       = aws_glue_job.bronze_ingestion_job.name
  description = "AWS Glue Python Shell Bronze Ingestion Job Name."
}

output "glue_silver_iceberg_job_name" {
  value       = aws_glue_job.silver_iceberg_job.name
  description = "AWS Glue PySpark Silver Iceberg ETL Job Name."
}

output "glue_catalog_database_name" {
  value       = local.glue_db_name
  description = "AWS Glue Data Catalog Database Name for Silver Iceberg Tables."
}

output "athena_workgroup_name" {
  value       = local.athena_wg_name
  description = "Amazon Athena WorkGroup Name."
}

output "sns_alert_topic_arn" {
  value       = local.sns_topic_arn
  description = "Amazon SNS Alert Topic ARN."
}

output "servicenow_state_machine_arn" {
  value       = aws_sfn_state_machine.servicenow_orchestrator.arn
  description = "ServiceNow Step Functions State Machine ARN."
}

output "moveworks_state_machine_arn" {
  value       = aws_sfn_state_machine.moveworks_orchestrator.arn
  description = "Moveworks Step Functions State Machine ARN."
}

output "genesys_state_machine_arn" {
  value       = aws_sfn_state_machine.genesys_orchestrator.arn
  description = "Genesys Step Functions State Machine ARN."
}
