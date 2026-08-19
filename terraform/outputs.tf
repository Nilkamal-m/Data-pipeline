# ==============================================================================
# Terraform Outputs
# ==============================================================================

output "data_lake_s3_bucket_name" {
  value       = aws_s3_bucket.data_lake.bucket
  description = "Single S3 Data Lake bucket name."
}

output "data_lake_s3_bucket_arn" {
  value       = aws_s3_bucket.data_lake.arn
  description = "Single S3 Data Lake bucket ARN."
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
  value       = aws_glue_catalog_database.silver_db.name
  description = "AWS Glue Data Catalog Database Name for Silver Iceberg Tables."
}

output "athena_workgroup_name" {
  value       = aws_athena_workgroup.data_pipeline.name
  description = "Amazon Athena WorkGroup Name."
}

output "sns_alert_topic_arn" {
  value       = aws_sns_topic.pipeline_alerts.arn
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
