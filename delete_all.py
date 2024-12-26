import boto3

# Configuração do cliente DynamoDB
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')  # Ajuste a região conforme necessário
table_name = 'tb_dev_geo_localizaco'
table = dynamodb.Table(table_name)

def delete_all_items():
    print(f"Iniciando a exclusão de todos os itens da tabela {table_name}...")

    # Realizar o scan para listar todos os itens
    response = table.scan()
    items = response.get('Items', [])

    # Deletar os itens em lotes
    with table.batch_writer() as batch:
        for item in items:
            # Extract the key name from the key schema dictionary
            key = {attr['AttributeName']: item[attr['AttributeName']] for attr in table.key_schema}
            batch.delete_item(Key=key)

    print(f"{len(items)} itens excluídos.")


    # Paginar e continuar a exclusão, se necessário
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        items = response.get('Items', [])
        
        with table.batch_writer() as batch:
            for item in items:
                # Apply the same fix here
                key = {attr['AttributeName']: item[attr['AttributeName']] for attr in table.key_schema}
                batch.delete_item(Key=key)

        print(f"{len(items)} itens excluídos na próxima página.")

    print(f"Exclusão concluída para a tabela {table_name}.")

if __name__ == "__main__":
    delete_all_items()
