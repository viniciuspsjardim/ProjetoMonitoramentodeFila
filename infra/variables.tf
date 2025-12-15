variable "aws_region" {
  description = "AWS Region"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project Name"
  type        = string
  default     = "healthcare-ops"
}

variable "environment" {
  description = "Environment (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "glue_db_name" {
  description = "Glue Database Name"
  type        = string
  default     = "healthcare_ops_db"
}

variable "openai_api_key" {
  description = "OpenAI API Key (Placeholder, strictly passed via secrets)"
  type        = string
  sensitive   = true
  default     = "placeholder-key"
}
