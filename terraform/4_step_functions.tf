locals {
  sns_topic_arn       = var.use_existing_sns_topic ? "arn:aws:sns:${var.aws_region}:${data.aws_caller_identity.current.account_id}:${var.app_name}-alerts-topic-${var.environment}" : module.sns_topic.topic_arn
  sfn_role_arn        = var.use_existing_step_functions_role ? "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${var.app_name}-stepfunctions-role-${var.environment}" : module.step_functions_iam_role.iam_role_arn
  bronze_job_name     = "${var.app_name}-bronze-ingestion-${var.environment}"
  silver_job_name     = "${var.app_name}-silver-iceberg-etl-${var.environment}"
  silver_crawler_name = "${var.app_name}-silver-iceberg-crawler-${var.environment}"
}

# ------------------------------------------------------------------------------
# 1. Amazon SNS Alert Topic & Email Subscription using AWS Community Module
# ------------------------------------------------------------------------------
module "sns_topic" {
  source  = "terraform-aws-modules/sns/aws"
  version = "~> 6.0.0"

  create = !var.use_existing_sns_topic
  name   = "${var.app_name}-alerts-topic-${var.environment}"

  subscriptions = {
    email = {
      protocol = "email"
      endpoint = var.alert_email_address
    }
  }

  tags = {
    Environment = var.environment
    Application = var.app_name
    Layer       = "Orchestration"
    ManagedBy   = "Terraform"
  }
}

# ------------------------------------------------------------------------------
# 2. Step Functions & EventBridge IAM Roles & Policies using AWS Community Module
# ------------------------------------------------------------------------------
module "step_functions_iam_policy" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-policy"
  version = "~> 5.30.0"

  create_policy = !var.use_existing_step_functions_role
  name          = "${var.app_name}-stepfunctions-policy-${var.environment}"
  description   = "Execution policy for AWS Step Functions orchestrating Glue jobs and Crawlers."

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

  tags = {
    Environment = var.environment
    Application = var.app_name
    Layer       = "Orchestration"
    ManagedBy   = "Terraform"
  }
}

module "step_functions_iam_role" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-assumable-role"
  version = "~> 5.30.0"

  create_role       = !var.use_existing_step_functions_role
  role_name         = "${var.app_name}-stepfunctions-role-${var.environment}"
  role_requires_mfa = false

  trusted_role_services = [
    "states.amazonaws.com"
  ]

  custom_role_policy_arns = [
    module.step_functions_iam_policy.arn
  ]

  tags = {
    Environment = var.environment
    Application = var.app_name
    Layer       = "Orchestration"
    ManagedBy   = "Terraform"
  }
}

module "eventbridge_iam_policy" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-policy"
  version = "~> 5.30.0"

  create_policy = true
  name          = "${var.app_name}-eventbridge-policy-${var.environment}"
  description   = "Execution policy for EventBridge triggering Step Functions state machines."

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

  tags = {
    Environment = var.environment
    Application = var.app_name
    Layer       = "Orchestration"
    ManagedBy   = "Terraform"
  }
}

module "eventbridge_iam_role" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-assumable-role"
  version = "~> 5.30.0"

  create_role       = true
  role_name         = "${var.app_name}-eventbridge-role-${var.environment}"
  role_requires_mfa = false

  trusted_role_services = [
    "events.amazonaws.com"
  ]

  custom_role_policy_arns = [
    module.eventbridge_iam_policy.arn
  ]

  tags = {
    Environment = var.environment
    Application = var.app_name
    Layer       = "Orchestration"
    ManagedBy   = "Terraform"
  }
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
  role_arn  = module.eventbridge_iam_role.iam_role_arn
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
  role_arn  = module.eventbridge_iam_role.iam_role_arn
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
  role_arn  = module.eventbridge_iam_role.iam_role_arn
}
