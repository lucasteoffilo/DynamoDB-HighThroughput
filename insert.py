import boto3
import uuid
import time
from datetime import datetime, UTC
from decimal import Decimal
import random

# Configuração do cliente DynamoDB
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table_name = 'tb_dev_geo_localizaco'
table = dynamodb.Table(table_name)

# Lista de IDs de usuário
primary_user_id = 1  # Cliente principal (70%)
secondary_user_ids = list(range(2, 32))  # Outros 30 clientes (30%)

# Contador de id_rastreador
id_rastreador_counter = 1

# Função para gerar dados simulados
def generate_item(id_usuario):
    global id_rastreador_counter
    current_time = datetime.now(UTC)
    ttl_time = int(current_time.timestamp() + 30)  # convertido para int timestamp Unix

    item = {
        'id_localizacao': str(uuid.uuid4()),
        'dt_localizacao': current_time.isoformat(),
        'acelerometro': Decimal(str(3.58271565)),
        'altitude': Decimal(str(929.13878713)),
        'bateria_bkp': Decimal(str(10.69216659)),
        'bateria_carro': Decimal(str(14.58555477)),
        'curso': Decimal(str(42.50366274)),
        'dt_insert': current_time.isoformat(),
        'dt_insert_ttl': ttl_time,  # agora é um timestamp Unix
        'endereco': str(uuid.uuid4()),
        'flg_bloqueio': True,
        'flg_clonado': False,
        'flg_gps_valido': True,
        'flg_jamming': False,
        'flg_painel': True,
        'flg_panico': False,
        'horimetro': str(uuid.uuid4()),
        'id_usuario_adm': id_usuario,
        'id_rastreador': id_rastreador_counter,
        'ignicao': True,
        'input3': str(uuid.uuid4()),
        'latitude': Decimal(str(65.00336379)),
        'longitude': Decimal(str(-175.0928208)),
        'odometro': str(uuid.uuid4()),
        'servidor_in': str(uuid.uuid4()),
        'sinal_celular': Decimal(str(94.86547456)),
        'sinal_gps': Decimal(str(1.56562221)),
        'tp_relogio': 0,
        'velocidade': Decimal(str(103.81141676))
    }
    # print(f"id_rastreador gerado: {id_rastreador_counter}")
    id_rastreador_counter += 1
    return item

# Função para inserir registros no DynamoDB
def insert_records_per_second(records_per_second=300, duration_seconds=10):
    print("Iniciando inserção de registros no DynamoDB...")
    start_time = time.time()
    total_records = 0
    last_generated_id = None  # Variável para armazenar o último id_rastreador gerado

    while time.time() - start_time < duration_seconds:
        with table.batch_writer() as batch:
            for _ in range(records_per_second):
                # Decidir a qual cliente atribuir o registro
                if random.random() < 0.7:
                    id_usuario = primary_user_id  # 70% para o cliente principal
                else:
                    id_usuario = random.choice(secondary_user_ids)  # 30% para os outros 30 clientes

                # Gerar e inserir o item
                item = generate_item(id_usuario)
                last_generated_id = item['id_rastreador']  # Armazena o último id gerado
                batch.put_item(Item=item)
                total_records += 1
                

        print(f"{total_records} registros inseridos até agora...")
        print(f"Último id_rastreador gerado neste lote: {last_generated_id}")  # Imprime após o lote
        time.sleep(1)  # Garante uma taxa de 300 registros por segundo

    print(f"Inserção concluída! Total de registros inseridos: {total_records}")
    print(f"Último id_rastreador gerado: {last_generated_id}")  # Imprime no final


# Execução do script
if __name__ == "__main__":
    insert_records_per_second(records_per_second=300, duration_seconds=3600)
