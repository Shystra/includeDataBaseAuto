import mysql.connector
from dotenv import load_dotenv
import gspread
import os

# Carrega as variáveis de ambiente
load_dotenv()

# Conecta ao Google Sheets
gc = gspread.service_account(filename='api/key.json')
CODE = os.getenv('CODE')
sh = gc.open_by_key(CODE)
ws = sh.worksheet('Página1')

# Pega todas as linhas de dados (ignorando o cabeçalho)
records = ws.get_all_records()
if not records:
    print("Nenhum dado encontrado.")
    exit()

# Mapeamento dos nomes das colunas da planilha para os nomes das colunas no banco de dados
column_mapping = {
    'NUMERO': ('LINHA', None),
    'ICC': ('IMEI', None),  
    'MODELO': ('Operadora', None),
    'PLANO': ('Plano', None),
    'BASE': (None, ''),
    'CLIENTE': ('Cliente', None),
    'COD_GREGOR': (None, ''), 
    'TECNICO': ('Alocação', None),
    'DATA': ('Entregue', None),
    'USUARIO': (None, ''),
    'EMAIL_SOLICITANTE': (None, ''),
    'STATUS': ('PARECER DO COMPRAS', None)
}

configAWS = {
    'host': os.getenv('HOST'),
    'user': os.getenv('USER'),
    'password': os.getenv('PASSWORD'),
    'database': os.getenv('DATABASE'),
    'auth_plugin': os.getenv('AUTH_PLUGIN'),
}

try:
    cnx = mysql.connector.connect(**configAWS)
    cursor = cnx.cursor()
    print('Conexão bem sucedida no database.')

    # Preparando a lista de tuplas para a inserção de dados
    values_to_insert_list = []
    for record in records:
        values_to_insert = [record.get(sheet_column) or default_value for db_column, (sheet_column, default_value) in column_mapping.items()]
        values_to_insert_list.append(tuple(values_to_insert))
    
    # A instrução SQL INSERT reflete a ordem e os nomes das colunas como definido em `column_mapping`
    insert_query = f"INSERT INTO CHIPS_INVENTARIO ({', '.join(column_mapping.keys())}) VALUES ({', '.join(['%s'] * len(column_mapping))})"

    # Utilizando o método executemany para inserir todos os registros de uma vez
    cursor.executemany(insert_query, values_to_insert_list)
    cnx.commit()
    print(f"{len(records)} registros inseridos com sucesso.")
except mysql.connector.Error as e:
    print(f"Erro ao conectar ao banco de dados: {e}")
finally:
    if cnx.is_connected():
        cursor.close()
        cnx.close()
