import os
import json
import logging
import boto3
from openai import OpenAI
from pydantic import BaseModel, Field
from typing import Optional

# Setup Logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Secrets Manager Helper
def get_secret(secret_name):
    client = boto3.client('secretsmanager')
    try:
        response = client.get_secret_value(SecretId=secret_name)
        return json.loads(response['SecretString'])['OPENAI_API_KEY']
    except Exception as e:
        logger.error(f"Error retrieving secret: {e}")
        raise

# Pydantic Model for Structured Output
class IncidentClassification(BaseModel):
    category: str = Field(..., description="The category of the operational event (e.g., 'Delay', 'Equipment Failure', 'Staff Shortage').")
    severity: str = Field(..., description="Severity level: 'Low', 'Medium', 'High', 'Critical'.")
    root_cause: Optional[str] = Field(None, description="Potential root cause inferred from the text.")
    impacted_sector: Optional[str] = Field(None, description="The hospital sector impacted.")
    suggested_action: str = Field(..., description="Immediate action recommended.")
    confidence: float = Field(..., description="Confidence score between 0.0 and 1.0.")

def lambda_handler(event, context):
    """
    Expects event with 'text' field.
    Returns structured classification.
    """
    try:
        text = event.get('text', '')
        if not text:
            return {'statusCode': 400, 'body': 'No text provided'}

        # Get API Key
        api_key = get_secret(os.environ.get('SECRET_NAME', 'healthcare-ops-openai-key'))
        client = OpenAI(api_key=api_key)

        logger.info(f"Classifying text: {text[:50]}...")

        completion = client.beta.chat.completions.parse(
            model="gpt-4o-mini", # Cost effective
            messages=[
                {"role": "system", "content": "You are an AI assistant for hospital operations. Classify the following event log."},
                {"role": "user", "content": text},
            ],
            response_format=IncidentClassification,
        )

        result = completion.choices[0].message.parsed
        
        # Save to S3 (Gold) logic would go here
        # output_key = ... s3.put_object(...)
        
        return {
            'statusCode': 200,
            'body': json.dumps(result.dict())
        }

    except Exception as e:
        logger.error(f"Failed to classify: {e}")
        return {'statusCode': 500, 'body': str(e)}
