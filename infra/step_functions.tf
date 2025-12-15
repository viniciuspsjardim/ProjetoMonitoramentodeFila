resource "aws_sfn_state_machine" "main_orchestrator" {
  name     = "${var.project_name}-orchestrator-${var.environment}"
  role_arn = aws_iam_role.step_functions_role.arn

  definition = templatefile("${path.module}/step_functions.asl.json", {
    aws_region = var.aws_region
    account_id = data.aws_caller_identity.current.account_id
    project_name = var.project_name
    bucket_name = aws_s3_bucket.datalake.id
  })
}

data "aws_caller_identity" "current" {}

# 1. Daily ETL Schedule
resource "aws_scheduler_schedule" "etl_daily" {
  name       = "${var.project_name}-daily-etl"
  group_name = "default"
  flexible_time_window { mode = "OFF" }
  schedule_expression = "cron(0 1 * * ? *)" # 1 AM UTC

  target {
    arn      = aws_sfn_state_machine.main_orchestrator.arn
    role_arn = aws_iam_role.scheduler_role.arn
    input = jsonencode({ "trigger": "scheduler", "type": "daily" })
  }
}

# 2. Inference Schedule (15 mins)
resource "aws_scheduler_schedule" "inference_15m" {
  name       = "${var.project_name}-inference-15m"
  group_name = "default"
  flexible_time_window { mode = "OFF" }
  schedule_expression = "rate(15 minutes)"

  target {
    arn      = aws_sfn_state_machine.main_orchestrator.arn
    role_arn = aws_iam_role.scheduler_role.arn
    input = jsonencode({ "trigger": "scheduler", "type": "inference_only" }) # Logic in SF needed to skip ETL if type=inference
  }
}

# 3. Weekly Training Schedule
resource "aws_scheduler_schedule" "training_weekly" {
  name       = "${var.project_name}-training-weekly"
  group_name = "default"
  flexible_time_window { mode = "OFF" }
  schedule_expression = "cron(0 2 ? * SUN *)" # Sunday 2 AM

  target {
    arn      = aws_sfn_state_machine.main_orchestrator.arn
    role_arn = aws_iam_role.scheduler_role.arn
    input = jsonencode({ "trigger": "scheduler", "type": "weekly_train" })
  }
}
