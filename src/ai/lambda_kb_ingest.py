import os
import json
import logging
import boto3
from openai import OpenAI

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_secret(secret_name):
    # (Same secret logic)
    client = boto3.client('secretsmanager')
    resp = client.get_secret_value(SecretId=secret_name)
    return json.loads(resp['SecretString'])['OPENAI_API_KEY']

def lambda_handler(event, context):
    """
    Triggered by S3 Object Create in /kb/ folder.
    Reads file, chunks it, embeds it, and indexes in OpenSearch (mocked here).
    """
    s3 = boto3.client('s3')
    
    # Get Bucket and Key
    record = event['Records'][0]
    bucket = record['s3']['bucket']['name']
    key = record['s3']['object']['key']
    
    logger.info(f"Processing KB file: {key}")
    
    try:
        # 1. Read File
        obj = s3.get_object(Bucket=bucket, Key=key)
        content = obj['Body'].read().decode('utf-8')
        
        # 2. Chunking (Simple logic for MVP)
        chunk_size = 1000
        chunks = [content[i:i+chunk_size] for i in range(0, len(content), chunk_size)]
        
        # 3. Embeddings
        api_key = get_secret(os.environ.get('SECRET_NAME'))
        client = OpenAI(api_key=api_key)
        
        embeddings = []
        for chunk in chunks:
            resp = client.embeddings.create(
                input=chunk,
                model="text-embedding-3-small"
            )
            vec = resp.data[0].embedding
            embeddings.append({"text": chunk, "vector": vec, "source": key})
            
        # 4. Index in OpenSearch (Placeholder)
        # opensearch_client.index(...)
        logger.info(f"Generated {len(embeddings)} embeddings. Indexing skipped (Mock).")
        
        return {'statusCode': 200, 'status': 'Indexed'}
        
    except Exception as e:
        logger.error(f"Ingestion failed: {e}")
        raise e
