import os
import boto3
import json
import gzip
import io
import logging

# Importa a função process_files do script import_files.py
from import_files import process_files
from import_files import process_message

# Carrega as variáveis de ambiente do arquivo .env
from dotenv import load_dotenv
load_dotenv()

# Configuração da AWS
AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
QUEUE_URL = os.environ['QUEUE_URL']
DYNAMODB_TABLE_NAME = os.environ['DYNAMODB_TABLE_NAME']
BUCKET_NAME = os.environ['BUCKET_NAME']

# Configura o logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cria uma conexão com a fila SQS
sqs = boto3.client('sqs', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name=AWS_REGION)

# Cria uma conexão com o DynamoDB
dynamodb = boto3.resource('dynamodb', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name=AWS_REGION)
table = dynamodb.Table(DYNAMODB_TABLE_NAME)

# Cria uma conexão com o S3
s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name=AWS_REGION)

# Define a função que processa as mensagens - incluir function no script import_files.py
def process_message(message):
    try:
        # Deserializa a mensagem JSON
        message_body = json.loads(message['Body'])
        # Faz download do arquivo do S3
        bucket_name = message_body['bucket_name']
        key = message_body['key']
        response = s3.get_object(Bucket=bucket_name, Key=key) #ajustar download arquivo
        file_content = response['Body'].read() #usar arquivo acmp exemplo
        # Carrega o arquivo em um DataFrame
        df = process_files(file_content)
        # Processa os registros no arquivo
        # ... ajustar lógica
        # Retorna o resultado do processamento
        result = [] #lista w/ dict
        logger.info(f"Processado mensagem {message_body['message_id']}")
        logger.info(f"Resultado: {result}")
        return result
    except Exception as e:
        logger.error(f"Erro ao processar mensagem {message_body['message_id']}: {str(e)}")
        raise

# Define a função Lambda
def lambda_handler(event, context):
    try:
        # Recebe as mensagens da fila SQS
        messages = sqs.receive_message(QueueUrl=QUEUE_URL, MaxNumberOfMessages=10)
        if 'Messages' in messages:
            for message in messages['Messages']:
                # Processa a mensagem
                results = process_message(message)
            # Salva os registros no DynamoDB
            for record in results:
                table.put_item(Item=record)
            # Remove a mensagem da fila
            sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=message['ReceiptHandle'])
        return {'statusCode': 200, 'body': 'Mensagem processada com sucesso'}
    except Exception as e:
        logger.error(f"Erro ao processar mensagem: {str(e)}")
        return {'statusCode': 500, 'body': 'Erro ao processar mensagem'}

#Este script faz o seguinte:
#Configura as credenciais da AWS.
#Cria uma conexão com a fila SQS.
#Cria uma conexão com o DynamoDB.
#Cria uma conexão com o S3.
#Define a função process_message que processa as mensagens recebidas.
#Define a função Lambda que recebe as mensagens da fila SQS.
#Processa as mensagens recebidas, 
#Faz download do arquivo do S3
#Processa os registros no arquivo 
#Salva o resultado no DynamoDB.
#Remove as mensagens processadas da fila.
