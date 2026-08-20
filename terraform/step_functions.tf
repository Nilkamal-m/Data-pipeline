# ==============================================================================
# PART 3: STEP FUNCTIONS & ORCHESTRATION TERRAFORM INFRASTRUCTURE
# ==============================================================================
# Includes:
# - Step Functions & EventBridge IAM Roles & Policies
# - Amazon SNS Alert Topic & Email Subscription
# - 3 AWS Step Functions State Machines (ServiceNow, Moveworks, Genesys)
# - 3 Amazon EventBridge Cron Rules & Targets
# ==============================================================================

# ------------------------------------------------------------------------------
# 1. Step Functions & EventBridge IAM Roles
# ------------------------------------------------------------------------------
resource "aws_iam_role" "step_functions_role" {
  name = "${var.app_name}-stepfunctions-role-${var.environment}"

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
          aws_sns_topic.pipeline_alerts.arn
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
  role       = aws_iam_role.step_functions_role.name
  policy_arn = aws_iam_policy.step_functions_policy.arn
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
# 2. Amazon SNS Alert Topic & Email Subscription
# ------------------------------------------------------------------------------
resource "aws_sns_topic" "pipeline_alerts" {
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
  topic_arn = aws_sns_topic.pipeline_alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email_address
}


# ------------------------------------------------------------------------------
# 3. AWS Step Functions State Machines
# ------------------------------------------------------------------------------

# ServiceNow Orchestrator State Machine
resource "aws_sfn_state_machine" "servicenow_orchestrator" {
  name     = "${var.app_name}-servicenow-orchestrator-${var.environment}"
  role_arn = aws_iam_role.step_functions_role.arn

  definition = jsonencode({
    Comment = "Orchestrates ServiceNow Ingestion -> Silver Iceberg ETL -> Glue Crawler Sync -> SNS Alert"
    StartAt = "TriggerBronzeIngestion"
    States = {
      TriggerBronzeIngestion = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.bronze_ingestion_job.name
          Arguments = {
            "--SOURCE_SYSTEM"   = "servicenow"
            "--TABLE_NAME"      = "incident,change_request,problem,sys_user"
            "--SECRET_NAME"     = aws_secretsmanager_secret.servicenow_secret.name
            "--CONFIG_S3_PATH"  = "s3://${aws_s3_bucket.data_lake.bucket}/bronze/script/config/bronze_config.json"
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
          JobName = aws_glue_job.silver_iceberg_job.name
          Arguments = {
            "--SOURCE_SYSTEM"          = "servicenow"
            "--TABLE_NAME"             = "incident,change_request,problem,sys_user"
            "--SILVER_CONFIG_S3_PATH" = "s3://${aws_s3_bucket.data_lake.bucket}/silver/script/config/silver_config.json"
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
          Name = aws_glue_crawler.silver_iceberg_crawler.name
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
          Name = aws_glue_crawler.silver_iceberg_crawler.name
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
          TopicArn = aws_sns_topic.pipeline_alerts.arn
          Subject  = "SUCCESS: ServiceNow Pipeline Completed (${var.environment})"
          Message  = "ServiceNow Bronze Ingestion, Silver Iceberg ETL, and Glue Crawler Sync completed successfully."
        }
        End = true
      }

      SendFailureAlert = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:sns:publish"
        Parameters = {
          TopicArn = aws_sns_topic.pipeline_alerts.arn
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

# Moveworks Orchestrator State Machine
resource "aws_sfn_state_machine" "moveworks_orchestrator" {
  name     = "${var.app_name}-moveworks-orchestrator-${var.environment}"
  role_arn = aws_iam_role.step_functions_role.arn

  definition = jsonencode({
    Comment = "Orchestrates Moveworks Ingestion -> Silver Iceberg ETL -> Glue Crawler Sync -> SNS Alert"
    StartAt = "TriggerBronzeIngestion"
    States = {
      TriggerBronzeIngestion = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.bronze_ingestion_job.name
          Arguments = {
            "--SOURCE_SYSTEM"   = "moveworks"
            "--TABLE_NAME"      = "interactions,users,tickets"
            "--SECRET_NAME"     = aws_secretsmanager_secret.moveworks_secret.name
            "--CONFIG_S3_PATH"  = "s3://${aws_s3_bucket.data_lake.bucket}/bronze/script/config/bronze_config.json"
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
          JobName = aws_glue_job.silver_iceberg_job.name
          Arguments = {
            "--SOURCE_SYSTEM"          = "moveworks"
            "--TABLE_NAME"             = "interactions,users,tickets"
            "--SILVER_CONFIG_S3_PATH" = "s3://${aws_s3_bucket.data_lake.bucket}/silver/script/config/silver_config.json"
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
          Name = aws_glue_crawler.silver_iceberg_crawler.name
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
          Name = aws_glue_crawler.silver_iceberg_crawler.name
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
          TopicArn = aws_sns_topic.pipeline_alerts.arn
          Subject  = "SUCCESS: Moveworks Pipeline Completed (${var.environment})"
          Message  = "Moveworks Bronze Ingestion, Silver Iceberg ETL, and Glue Crawler Sync completed successfully."
        }
        End = true
      }

      SendFailureAlert = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:sns:publish"
        Parameters = {
          TopicArn = aws_sns_topic.pipeline_alerts.arn
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

# Genesys Orchestrator State Machine
resource "aws_sfn_state_machine" "genesys_orchestrator" {
  name     = "${var.app_name}-genesys-orchestrator-${var.environment}"
  role_arn = aws_iam_role.step_functions_role.arn

  definition = jsonencode({
    Comment = "Orchestrates Genesys Ingestion -> Silver Iceberg ETL -> Glue Crawler Sync -> SNS Alert"
    StartAt = "TriggerBronzeIngestion"
    States = {
      TriggerBronzeIngestion = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.bronze_ingestion_job.name
          Arguments = {
            "--SOURCE_SYSTEM"   = "genesys"
            "--TABLE_NAME"      = "conversations,users,queues"
            "--SECRET_NAME"     = aws_secretsmanager_secret.genesys_secret.name
            "--CONFIG_S3_PATH"  = "s3://${aws_s3_bucket.data_lake.bucket}/bronze/script/config/bronze_config.json"
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
          JobName = aws_glue_job.silver_iceberg_job.name
          Arguments = {
            "--SOURCE_SYSTEM"          = "genesys"
            "--TABLE_NAME"             = "conversations,users,queues"
            "--SILVER_CONFIG_S3_PATH" = "s3://${aws_s3_bucket.data_lake.bucket}/silver/script/config/silver_config.json"
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
          Name = aws_glue_crawler.silver_iceberg_crawler.name
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
          Name = aws_glue_crawler.silver_iceberg_crawler.name
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
          TopicArn = aws_sns_topic.pipeline_alerts.arn
          Subject  = "SUCCESS: Genesys Pipeline Completed (${var.environment})"
          Message  = "Genesys Bronze Ingestion, Silver Iceberg ETL, and Glue Crawler Sync completed successfully."
        }
        End = true
      }

      SendFailureAlert = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:sns:publish"
        Parameters = {
          TopicArn = aws_sns_topic.pipeline_alerts.arn
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
