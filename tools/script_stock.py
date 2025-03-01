import pandas as pd
from pymongo import MongoClient
import locale
from datetime import datetime
import os
from pathlib import Path
from pprint import pprint

# Configurar locale para PT-BR para tratar números com vírgula
try:
    locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
except locale.Error:
    try:
        locale.setlocale(locale.LC_ALL, 'Portuguese_Brazil.1252')
    except locale.Error:
        print("Aviso: Usando locale padrão do sistema")

def convert_currency(value):
    """Converte strings de moeda (ex: '1.288,45') para float"""
    try:
        if isinstance(value, str):
            # Remove possíveis pontos de milhar e substitui vírgula por ponto
            value = value.replace('.', '').replace(',', '.')
        return float(value)
    except:
        return value

def convert_percentage(value):
    """Converte strings de porcentagem para float"""
    try:
        if isinstance(value, str):
            value = value.replace(',', '.')
        return float(value)
    except:
        return value

try:
    # Usar Path para melhor manipulação do caminho do arquivo
    current_dir = Path(__file__).parent
    file_path = current_dir.parent / 'data' / 'stock.csv'  # Sair da pasta 'tools' e entrar na pasta 'data'

    # Definir as colunas de data
    date_columns = ['DATA_FUNDO', 'DATA_REFERENCIA', 'DATA_VENCIMENTO_ORIGINAL',
                    'DATA_VENCIMENTO_AJUSTADA', 'DATA_EMISSAO', 'DATA_AQUISICAO']
    
    # Definir as colunas numéricas que precisam de conversão
    currency_columns = ['VALOR_NOMINAL', 'VALOR_PRESENTE', 'VALOR_AQUISICAO', 'VALOR_PDD']
    percentage_columns = ['TAXA_CESSAO', 'TX_RECEBIVEL']

    # Ler o arquivo CSV
    df = pd.read_csv(file_path, 
                     sep=';',
                     encoding='utf-8',
                     low_memory=False,  # Evita warning de tipos mistos
                     dayfirst=True)     # Especifica formato de data brasileiro

    # Converter colunas de data
    for col in date_columns:
        df[col] = pd.to_datetime(df[col], format='%d/%m/%Y', errors='coerce')

    # Converter colunas de moeda
    for col in currency_columns:
        df[col] = df[col].apply(convert_currency)

    # Converter colunas de porcentagem
    for col in percentage_columns:
        df[col] = df[col].apply(convert_percentage)

    # Converter colunas numéricas inteiras
    df['PRAZO'] = pd.to_numeric(df['PRAZO'], errors='coerce')
    df['PRAZO_ATUAL'] = pd.to_numeric(df['PRAZO_ATUAL'], errors='coerce')
    df['SEU_NUMERO_MULTIPAG'] = pd.to_numeric(df['SEU_NUMERO_MULTIPAG'], errors='coerce')

    # Conectar ao MongoDB
    client = MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=5000)
    # Verificar conexão
    client.server_info()
    
    # Limpar a collection existente
    db = client['investment_funds']
    collection = db['current_loans']
    collection.delete_many({})
    print("Collection anterior removida com sucesso")
    
    # Usar banco de dados específico para fundos de investimentos
    db = client['investment_funds']
    collection = db['current_loans']

    # Converter DataFrame para lista de dicionários e inserir no MongoDB
    records = df.to_dict('records')
    result = collection.insert_many(records)

    print(f'Foram inseridos {len(result.inserted_ids)} documentos no MongoDB no banco investment_funds')

    # Criar índices para melhorar a performance das consultas
    collection.create_index([("NOME_FUNDO", 1)])
    collection.create_index([("DOC_FUNDO", 1)])
    collection.create_index([("NOME_SACADO", 1)])
    collection.create_index([("SEU_NUMERO", 1)])
    collection.create_index([("DATA_VENCIMENTO_ORIGINAL", 1)])
    collection.create_index([("SITUACAO_RECEBIVEL", 1)])
    print("Índices criados com sucesso no banco investment_funds")

    # Mostrar um exemplo dos dados inseridos para validação
    # print("\nExemplo do primeiro registro inserido no banco stock:")
    # primeiro_registro = collection.find_one()
    # for key, value in primeiro_registro.items():
    #     if key != '_id':  # Não mostrar o ID do MongoDB
    #         print(f"{key}: {value}")

except FileNotFoundError:
    print(f"Erro: Arquivo não encontrado em {file_path}")
except pd.errors.EmptyDataError:
    print("Erro: O arquivo CSV está vazio")
except Exception as e:
    print(f"Erro inesperado: {str(e)}")
finally:
    if 'client' in locals():
        client.close()
        print("\nConexão com MongoDB fechada")