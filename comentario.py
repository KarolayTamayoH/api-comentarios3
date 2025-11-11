import boto3
import uuid
import os
import json

def lambda_handler(event, context):
    print(event)

    # Parsear el body (puede venir como string)
    body = event['body']
    if isinstance(body, str):
        body = json.loads(body)

    tenant_id = body['tenant_id']
    texto = body['texto']
    nombre_tabla = os.environ["TABLE_NAME"]
    bucket_name = os.environ["INGEST_BUCKET"]

    # Crear el comentario
    uuidv1 = str(uuid.uuid1())
    comentario = {
        'tenant_id': tenant_id,
        'uuid': uuidv1,
        'detalle': {
            'texto': texto
        }
    }

    # Guardar en DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(nombre_tabla)
    table.put_item(Item=comentario)

    # Guardar JSON en S3 (Ingesta Push)
    s3 = boto3.client('s3')
    file_name = f"{tenant_id}/{uuidv1}.json"
    s3.put_object(
        Bucket=bucket_name,
        Key=file_name,
        Body=json.dumps(comentario),
        ContentType='application/json'
    )

    print(f"Archivo subido a S3: s3://{bucket_name}/{file_name}")

    # Respuesta HTTP
    return {
        'statusCode': 200,
        'headers': {'Content-Type': 'application/json'},
        'body': json.dumps({
            'message': 'Comentario guardado correctamente',
            'comentario': comentario,
            's3_path': f"s3://{bucket_name}/{file_name}"
        })
    }
