output "s3_bucket_name" {
  value = aws_s3_bucket.datalake.id
}

output "glue_role_arn" {
  value = aws_iam_role.glue_role.arn
}

output "state_machine_arn" {
  value = aws_sfn_state_machine.main_orchestrator.arn
}
