# ==============================================================================
# STEP FUNCTIONS & SNS ORCHESTRATION INFRASTRUCTURE (terraform/4_step_functions/step_functions.tf)
# ==============================================================================
# Self-contained Terraform script for Step Functions Orchestration & SNS Alerts.
# Contains all variables, SNS Topics, State Machines, and EventBridge Cron Rules.
# Includes pre-existence check logic to skip creating resources if already present.
# ==============================================================================

terraform {
  required_version = ">= 1.3.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

data "aws_caller_identity" "current" {}

# ------------------------------------------------------------------------------
# Step Functions Layer Variables (All necessary variables defined inside this file)
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

# Pre-existence safety toggles
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

locals {
  bucket_name         = "${var.data_lake_bucket_name}-${var.environment}"
  sns_topic_arn       = var.use_existing_sns_topic ? "arn:aws:sns:${var.aws_region}:${data.aws_caller_identity.current.account_id}:${var.app_name}-alerts-topic-${var.environment}" : (length(aws_sns_topic.pipeline_alerts) > 0 ? aws_sns_topic.pipeline_alerts[0].arn : "")
  sfn_role_arn        = var.use_existing_step_functions_role ? "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${var.app_name}-stepfunctions-role-${var.environment}" : (length(aws_iam_role.step_functions_role) > 0 ? aws_iam_role.step_functions_role[0].arn : "")
  bronze_job_name     = "${var.app_name}-bronze-ingestion-${var.environment}"
  silver_job_name     = "${var.app_name}-silver-iceberg-etl-${var.environment}"
  silver_crawler_name = "${var.app_name}-silver-iceberg-crawler-${var.environment}"
}


# ------------------------------------------------------------------------------
# 1. Amazon SNS Alert Topic & Email Subscription (Skips creation if use_existing_sns_topic = true)
# ------------------------------------------------------------------------------
resource "aws_sns_topic" "pipeline_alerts" {
  count       = var.use_existing_sns_topic ? 0 : 1
  name        = "${var.app_name}-alerts-topic-${var.environment}"
  description = "SNS Alert Topic for Data Pipeline success/failure notifications."

  tags = {
    Environment = var.environment
    Application = var.app_name
    Layer       = "Orchestration"
    ManagedBy   = "Terraform"
  }
}

resource "aws_sns_topic_subscription" "email_alert" {
  count     = var.use_existing_sns_topic ? 0 : 1
  topic_arn = aws_sns_topic.pipeline_alerts[0].arn
  protocol  = "email"
  endpoint  = var.alert_email_address
}


# ------------------------------------------------------------------------------
# 2. Step Functions & EventBridge IAM Roles & Policies (Skips creation if use_existing_step_functions_role = true)
# ------------------------------------------------------------------------------
resource "aws_iam_role" "step_functions_role" {
  count = var.use_existing_step_functions_role ? 0 : 1
  name  = "${var.app_name}-stepfunctions-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "states.amazonaws.com"
        }
      }
    ]
  })

  tags = {
    Environment = var.environment
    Application = var.app_name
    Layer       = "Orchestration"
    ManagedBy   = "Terraform"
  }
}

resource "aws_iam_policy" "step_functions_policy" {
  count       = var.use_existing_step_functions_role ? 0 : 1
  name        = "${var.app_name}-stepfunctions-policy-${var.environment}"
  description = "Execution policy for AWS Step Functions orchestrating Glue jobs and Crawlers."

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:StartJobRun",
          "glue:GetJobRun",
          "glue:GetJobRuns",
          "glue:BatchStopJobRun",
          "glue:StartCrawler",
          "glue:GetCrawler"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "sns:Publish"
        ]
        Resource = [
          local.sns_topic_arn
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogDelivery",
          "logs:GetLogDelivery",
          "logs:UpdateLogDelivery",
          "logs:DeleteLogDelivery",
          "logs:ListLogDeliveries",
          "logs:PutResourcePolicy",
          "logs:DescribeResourcePolicies",
          "logs:DescribeLogGroups"
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "step_functions_policy_attach" {
  count      = var.use_existing_step_functions_role ? 0 : 1
  role       = aws_iam_role.step_functions_role[0].name
  policy_arn = aws_iam_policy.step_functions_policy[0].arn
}

resource "aws_iam_role" "eventbridge_role" {
  name = "${var.app_name}-eventbridge-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "events.amazonaws.com"
        }
      }
    ]
  })

  tags = {
    Environment = var.environment
    Application = var.app_name
    Layer       = "Orchestration"
    ManagedBy   = "Terraform"
  }
}

resource "aws_iam_policy" "eventbridge_policy" {
  name        = "${var.app_name}-eventbridge-policy-${var.environment}"
  description = "Execution policy for EventBridge triggering Step Functions state machines."

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "states:StartExecution"
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "eventbridge_policy_attach" {
  role       = aws_iam_role.eventbridge_role.name
  policy_arn = aws_iam_policy.eventbridge_policy.arn
}


# ------------------------------------------------------------------------------
# 3. AWS Step Functions State Machines
# ------------------------------------------------------------------------------

# ServiceNow State Machine
resource "aws_sfn_state_machine" "servicenow_orchestrator" {
  name     = "${var.app_name}-servicenow-orchestrator-${var.environment}"
  role_arn = local.sfn_role_arn

  definition = jsonencode({
    Comment = "Orchestrates ServiceNow Ingestion -> Silver Iceberg ETL -> Glue Crawler Sync -> SNS Alert"
    StartAt = "TriggerBronzeIngestion"
    States = {
      TriggerBronzeIngestion = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = local.bronze_job_name
          Arguments = {
            "--SOURCE_SYSTEM"   = "servicenow"
            "--TABLE_NAME"      = "incident,change_request,problem,sys_user"
            "--SECRET_NAME"     = "data-lake/servicenow-credentials-${var.environment}"
            "--CONFIG_S3_PATH"  = "s3://${local.bucket_name}/bronze/script/config/bronze_config.json"
          }
        }
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            Next        = "SendFailureAlert"
          }
        ]
        Next = "TriggerSilverIcebergETL"
      }

      TriggerSilverIcebergETL = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = local.silver_job_name
          Arguments = {
            "--SOURCE_SYSTEM"          = "servicenow"
            "--TABLE_NAME"             = "incident,change_request,problem,sys_user"
            "--SILVER_CONFIG_S3_PATH" = "s3://${local.bucket_name}/silver/script/config/silver_config.json"
          }
        }
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            Next        = "SendFailureAlert"
          }
        ]
        Next = "TriggerSilverCrawler"
      }

      TriggerSilverCrawler = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startCrawler"
        Parameters = {
          Name = local.silver_crawler_name
        }
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            Next        = "SendFailureAlert"
          }
        ]
        Next = "WaitForCrawler"
      }

      WaitForCrawler = {
        Type    = "Wait"
        Seconds = 15
        Next    = "GetCrawlerStatus"
      }

      GetCrawlerStatus = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:glue:getCrawler"
        Parameters = {
          Name = local.silver_crawler_name
        }
        Next = "CheckCrawlerRunning"
      }

      CheckCrawlerRunning = {
        Type = "Choice"
        Choices = [
          {
            Variable     = "$.Crawler.State"
            StringEquals = "RUNNING"
            Next         = "WaitForCrawler"
          }
        ]
        Default = "SendSuccessAlert"
      }

      SendSuccessAlert = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:sns:publish"
        Parameters = {
          TopicArn = local.sns_topic_arn
          Subject  = "SUCCESS: ServiceNow Pipeline Completed (${var.environment})"
          Message  = "ServiceNow Bronze Ingestion, Silver Iceberg ETL, and Glue Crawler Sync completed successfully."
        }
        End = true
      }

      SendFailureAlert = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:sns:publish"
        Parameters = {
          TopicArn = local.sns_topic_arn
          Subject  = "FAILURE: ServiceNow Pipeline Failed (${var.environment})"
          Message  = "ServiceNow Pipeline failed during execution. Inspect AWS Glue Logs or Step Functions execution history."
        }
        Fail = true
      }
    }
  })

  tags = {
    Environment = var.environment
    Application = var.app_name
    Layer       = "Orchestration"
    ManagedBy   = "Terraform"
  }
}

# Moveworks State Machine
resource "aws_sfn_state_machine" "moveworks_orchestrator" {
  name     = "${var.app_name}-moveworks-orchestrator-${var.environment}"
  role_arn = local.sfn_role_arn

  definition = jsonencode({
    Comment = "Orchestrates Moveworks Ingestion -> Silver Iceberg ETL -> Glue Crawler Sync -> SNS Alert"
    StartAt = "TriggerBronzeIngestion"
    States = {
      TriggerBronzeIngestion = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = local.bronze_job_name
          Arguments = {
            "--SOURCE_SYSTEM"   = "moveworks"
            "--TABLE_NAME"      = "interactions,users,tickets"
            "--SECRET_NAME"     = "data-lake/moveworks-credentials-${var.environment}"
            "--CONFIG_S3_PATH"  = "s3://${local.bucket_name}/bronze/script/config/bronze_config.json"
          }
        }
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            Next        = "SendFailureAlert"
          }
        ]
        Next = "TriggerSilverIcebergETL"
      }

      TriggerSilverIcebergETL = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = local.silver_job_name
          Arguments = {
            "--SOURCE_SYSTEM"          = "moveworks"
            "--TABLE_NAME"             = "interactions,users,tickets"
            "--SILVER_CONFIG_S3_PATH" = "s3://${local.bucket_name}/silver/script/config/silver_config.json"
          }
        }
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            Next        = "SendFailureAlert"
          }
        ]
        Next = "TriggerSilverCrawler"
      }

      TriggerSilverCrawler = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startCrawler"
        Parameters = {
          Name = local.silver_crawler_name
        }
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            Next        = "SendFailureAlert"
          }
        ]
        Next = "WaitForCrawler"
      }

      WaitForCrawler = {
        Type    = "Wait"
        Seconds = 15
        Next    = "GetCrawlerStatus"
      }

      GetCrawlerStatus = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:glue:getCrawler"
        Parameters = {
          Name = local.silver_crawler_name
        }
        Next = "CheckCrawlerRunning"
      }

      CheckCrawlerRunning = {
        Type = "Choice"
        Choices = [
          {
            Variable     = "$.Crawler.State"
            StringEquals = "RUNNING"
            Next         = "WaitForCrawler"
          }
        ]
        Default = "SendSuccessAlert"
      }

      SendSuccessAlert = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:sns:publish"
        Parameters = {
          TopicArn = local.sns_topic_arn
          Subject  = "SUCCESS: Moveworks Pipeline Completed (${var.environment})"
          Message  = "Moveworks Bronze Ingestion, Silver Iceberg ETL, and Glue Crawler Sync completed successfully."
        }
        End = true
      }

      SendFailureAlert = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:sns:publish"
        Parameters = {
          TopicArn = local.sns_topic_arn
          Subject  = "FAILURE: Moveworks Pipeline Failed (${var.environment})"
          Message  = "Moveworks Pipeline failed during execution. Inspect AWS Glue Logs or Step Functions execution history."
        }
        Fail = true
      }
    }
  })

  tags = {
    Environment = var.environment
    Application = var.app_name
    Layer       = "Orchestration"
    ManagedBy   = "Terraform"
  }
}

# Genesys State Machine
resource "aws_sfn_state_machine" "genesys_orchestrator" {
  name     = "${var.app_name}-genesys-orchestrator-${var.environment}"
  role_arn = local.sfn_role_arn

  definition = jsonencode({
    Comment = "Orchestrates Genesys Ingestion -> Silver Iceberg ETL -> Glue Crawler Sync -> SNS Alert"
    StartAt = "TriggerBronzeIngestion"
    States = {
      TriggerBronzeIngestion = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = local.bronze_job_name
          Arguments = {
            "--SOURCE_SYSTEM"   = "genesys"
            "--TABLE_NAME"      = "conversations,users,queues"
            "--SECRET_NAME"     = "data-lake/genesys-credentials-${var.environment}"
            "--CONFIG_S3_PATH"  = "s3://${local.bucket_name}/bronze/script/config/bronze_config.json"
          }
        }
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            Next        = "SendFailureAlert"
          }
        ]
        Next = "TriggerSilverIcebergETL"
      }

      TriggerSilverIcebergETL = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = local.silver_job_name
          Arguments = {
            "--SOURCE_SYSTEM"          = "genesys"
            "--TABLE_NAME"             = "conversations,users,queues"
            "--SILVER_CONFIG_S3_PATH" = "s3://${local.bucket_name}/silver/script/config/silver_config.json"
          }
        }
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            Next        = "SendFailureAlert"
          }
        ]
        Next = "TriggerSilverCrawler"
      }

      TriggerSilverCrawler = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startCrawler"
        Parameters = {
          Name = local.silver_crawler_name
        }
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            Next        = "SendFailureAlert"
          }
        ]
        Next = "WaitForCrawler"
      }

      WaitForCrawler = {
        Type    = "Wait"
        Seconds = 15
        Next    = "GetCrawlerStatus"
      }

      GetCrawlerStatus = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:glue:getCrawler"
        Parameters = {
          Name = local.silver_crawler_name
        }
        Next = "CheckCrawlerRunning"
      }

      CheckCrawlerRunning = {
        Type = "Choice"
        Choices = [
          {
            Variable     = "$.Crawler.State"
            StringEquals = "RUNNING"
            Next         = "WaitForCrawler"
          }
        ]
        Default = "SendSuccessAlert"
      }

      SendSuccessAlert = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:sns:publish"
        Parameters = {
          TopicArn = local.sns_topic_arn
          Subject  = "SUCCESS: Genesys Pipeline Completed (${var.environment})"
          Message  = "Genesys Bronze Ingestion, Silver Iceberg ETL, and Glue Crawler Sync completed successfully."
        }
        End = true
      }

      SendFailureAlert = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:sns:publish"
        Parameters = {
          TopicArn = local.sns_topic_arn
          Subject  = "FAILURE: Genesys Pipeline Failed (${var.environment})"
          Message  = "Genesys Pipeline failed during execution. Inspect AWS Glue Logs or Step Functions execution history."
        }
        Fail = true
      }
    }
  })

  tags = {
    Environment = var.environment
    Application = var.app_name
    Layer       = "Orchestration"
    ManagedBy   = "Terraform"
  }
}

# ------------------------------------------------------------------------------
# 4. Amazon EventBridge Cron Rules & Targets
# ------------------------------------------------------------------------------
resource "aws_cloudwatch_event_rule" "servicenow_cron" {
  name                = "${var.app_name}-servicenow-schedule-${var.environment}"
  description         = "Triggers ServiceNow pipeline state machine on schedule."
  schedule_expression = var.schedule_expression
  state               = "ENABLED"
}

resource "aws_cloudwatch_event_target" "servicenow_target" {
  rule      = aws_cloudwatch_event_rule.servicenow_cron.name
  target_id = "TriggerServiceNowStateMachine"
  arn       = aws_sfn_state_machine.servicenow_orchestrator.arn
  role_arn  = aws_iam_role.eventbridge_role.arn
}

resource "aws_cloudwatch_event_rule" "moveworks_cron" {
  name                = "${var.app_name}-moveworks-schedule-${var.environment}"
  description         = "Triggers Moveworks pipeline state machine on schedule."
  schedule_expression = var.schedule_expression
  state               = "ENABLED"
}

resource "aws_cloudwatch_event_target" "moveworks_target" {
  rule      = aws_cloudwatch_event_rule.moveworks_cron.name
  target_id = "TriggerMoveworksStateMachine"
  arn       = aws_sfn_state_machine.moveworks_orchestrator.arn
  role_arn  = aws_iam_role.eventbridge_role.arn
}

resource "aws_cloudwatch_event_rule" "genesys_cron" {
  name                = "${var.app_name}-genesys-schedule-${var.environment}"
  description         = "Triggers Genesys pipeline state machine on schedule."
  schedule_expression = var.schedule_expression
  state               = "ENABLED"
}

resource "aws_cloudwatch_event_target" "genesys_target" {
  rule      = aws_cloudwatch_event_rule.genesys_cron.name
  target_id = "TriggerGenesysStateMachine"
  arn       = aws_sfn_state_machine.genesys_orchestrator.arn
  role_arn  = aws_iam_role.eventbridge_role.arn
}

# ------------------------------------------------------------------------------
# Step Functions Layer Outputs
# ------------------------------------------------------------------------------
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
