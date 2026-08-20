# ==============================================================================
# PART 2: SILVER LAYER TERRAFORM INFRASTRUCTURE
# ==============================================================================
# Includes:
# - Silver & Athena Results S3 Folder Prefixes
# - AWS Glue Data Catalog Database for Silver Iceberg Tables
# - AWS Glue Crawler for Iceberg Tables
# - AWS Glue PySpark Apache Iceberg ETL Job (silver_iceberg_etl.py)
# - Amazon Athena Dedicated WorkGroup
# ==============================================================================

# ------------------------------------------------------------------------------
# 1. Silver & Athena S3 Folder Structure Placeholders
# ------------------------------------------------------------------------------
resource "aws_s3_object" "folder_silver" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "silver/"
}

resource "aws_s3_object" "folder_athena_results" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "athena-results/"
}

resource "aws_s3_object" "folder_silver_script" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "silver/script/"
}


# ------------------------------------------------------------------------------
# 2. AWS Glue Data Catalog Database & Iceberg Crawler
# ------------------------------------------------------------------------------
resource "aws_glue_catalog_database" "silver_db" {
  name        = "uax_data_lake_db_${var.environment}"
  description = "AWS Glue Data Catalog Database for Silver Layer Apache Iceberg Tables."
}

resource "aws_glue_crawler" "silver_iceberg_crawler" {
  name          = "${var.app_name}-silver-iceberg-crawler-${var.environment}"
  database_name = aws_glue_catalog_database.silver_db.name
  role          = aws_iam_role.glue_execution_role.arn
  description   = "Crawls Silver Layer Apache Iceberg tables into AWS Glue Data Catalog."

  s3_target {
    path = "s3://${aws_s3_bucket.data_lake.bucket}/silver/"
  }

  schema_change_policy {
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
# 3. AWS Glue PySpark Apache Iceberg ETL Job (Silver)
# ------------------------------------------------------------------------------
resource "aws_glue_job" "silver_iceberg_job" {
  name              = "${var.app_name}-silver-iceberg-etl-${var.environment}"
  description       = "AWS Glue PySpark ETL Job transforming Bronze raw data into Silver Apache Iceberg tables."
  role_arn          = aws_iam_role.glue_execution_role.arn
  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.data_lake.bucket}/silver/script/silver_iceberg_etl.py"
  }

  default_arguments = {
    "--extra-py-files"          = "s3://${aws_s3_bucket.data_lake.bucket}/silver/script/silver_config_loader.py,s3://${aws_s3_bucket.data_lake.bucket}/silver/script/transformer.py"
    "--SILVER_CONFIG_S3_PATH"  = "s3://${aws_s3_bucket.data_lake.bucket}/silver/script/config/silver_config.json"
    "--datalake-formats"        = "iceberg"
    "--conf"                    = "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    "--DATA_LAKE_BUCKET"        = aws_s3_bucket.data_lake.bucket
    "--GLUE_DATABASE"           = aws_glue_catalog_database.silver_db.name
    "--job-language"            = "python"
  }

  tags = {
    Environment = var.environment
    Application = var.app_name
    Layer       = "Silver"
    ManagedBy   = "Terraform"
  }

  depends_on = [
    aws_glue_job.bronze_ingestion_job
  ]
}


# ------------------------------------------------------------------------------
# 4. Amazon Athena WorkGroup
# ------------------------------------------------------------------------------
resource "aws_athena_workgroup" "data_pipeline" {
  name        = "uax_data_lake_workgroup_${var.environment}"
  description = "Dedicated Athena WorkGroup for querying Silver Apache Iceberg tables."
  state       = "ENABLED"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.data_lake.bucket}/athena-results/"
    }
  }

  tags = {
    Environment = var.environment
    Application = var.app_name
    Layer       = "Silver"
    ManagedBy   = "Terraform"
  }
}
