import os
import json
import boto3
import logging

# Carrega as variáveis de ambiente do arquivo .env
from dotenv import load_dotenv
load_dotenv()

# Configuração da AWS
AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
AWS_REGION = os.environ['AWS_REGION']
QUEUE_URL = os.environ['QUEUE_URL']
DYNAMODB_TABLE_NAME = os.environ['DYNAMODB_TABLE_NAME']

# Configura o logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cria uma conexão com a fila SQS
sqs = boto3.client('sqs', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name=AWS_REGION)

# Cria uma conexão com o DynamoDB
dynamodb = boto3.resource('dynamodb', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name=AWS_REGION)
table = dynamodb.Table(DYNAMODB_TABLE_NAME)

# Define a função que processa as mensagens
def process_message(message):
    # Deserializa a mensagem JSON
    message_body = json.loads(message['Body'])
    # Processa a mensagem
    # ...
    # Retorna o resultado do processamento
    return {'message': 'Mensagem processada com sucesso'}

# Define a função Lambda
def lambda_handler(event, context):
    try:
        # Recebe as mensagens da fila SQS
        messages = sqs.receive_message(QueueUrl=QUEUE_URL, MaxNumberOfMessages=10)
        if 'Messages' in messages:
            for message in messages['Messages']:
                # Processa a mensagem
                result = process_message(message)
                # Salva o resultado no DynamoDB
                table.put_item(Item=result)
                # Remove a mensagem da fila
                sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=message['ReceiptHandle'])
        return {'statusCode': 200, 'body': 'Mensagem processada com sucesso'}
    except Exception as e:
        logger.error(f"Erro ao processar mensagem: {str(e)}")
        return {'statusCode': 500, 'body': 'Erro ao processar mensagem'}