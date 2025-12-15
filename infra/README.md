# Infra Deployment

## Prerequisites
1. AWS CLI configured
2. Terraform >= 1.0

## Deploy
```bash
terraform init
terraform plan
terraform apply
```

## Resources
- S3 Bucket (Bronze/Silver/Gold)
- IAM Roles (Glue, Lambda, Step Functions)
- Step Functions State Machine (Placeholder)
- EventBridge Scheduler
