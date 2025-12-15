resource "aws_glue_catalog_database" "main_db" {
  name = var.glue_db_name
}

resource "aws_secretsmanager_secret" "openai_key" {
  name = "${var.project_name}-openai-key-${var.environment}-${random_id.bucket_suffix.hex}"
}

resource "aws_secretsmanager_secret_version" "openai_key_val" {
  secret_id     = aws_secretsmanager_secret.openai_key.id
  secret_string = jsonencode({
    OPENAI_API_KEY = var.openai_api_key
  })
}
