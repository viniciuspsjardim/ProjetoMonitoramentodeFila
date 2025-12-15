import os
import json
import logging
import boto3
from openai import OpenAI

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_secret(secret_name):
    client = boto3.client('secretsmanager')
    resp = client.get_secret_value(SecretId=secret_name)
    return json.loads(resp['SecretString'])['OPENAI_API_KEY']

def lambda_handler(event, context):
    """
    Expects 'query' in event.
    Returns Answer based on KB + Citations.
    """
    query = event.get('query')
    if not query:
        return {'statusCode': 400, 'body': 'Missing query'}
        
    try:
        api_key = get_secret(os.environ.get('SECRET_NAME'))
        client = OpenAI(api_key=api_key)
        
        # 1. Embed Query
        resp = client.embeddings.create(
            input=query,
            model="text-embedding-3-small"
        )
        query_vec = resp.data[0].embedding
        
        # 2. Retrieve from OpenSearch (Mocked)
        # hits = opensearch.search(vector=query_vec, k=3)
        # Mock Context
        context_chunks = [
            "Protocolo de Triagem ID-10: Priorizar dor torácica em idosos.",
            "Janela de Manutenção S3: Todo domingo às 02:00 AM."
        ]
        context_text = "\n\n".join(context_chunks)
        
        # 3. Generate Answer
        system_prompt = f"""
        You are a helpful assistant for hospital Ops. Use the following context to answer the query.
        If the answer is not in the context, say you don't know.
        
        Context:
        {context_text}
        """
        
        completion = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": query}
            ]
        )
        
        answer = completion.choices[0].message.content
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                "query": query,
                "answer": answer,
                "citations": ["doc1", "doc2"]
            })
        }
        
    except Exception as e:
        logger.error(f"Query failed: {e}")
        return {'statusCode': 500, 'body': str(e)}
