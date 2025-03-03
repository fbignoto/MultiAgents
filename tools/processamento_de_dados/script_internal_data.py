import pandas as pd
from pymongo import MongoClient
import json
from datetime import datetime
import os
from pathlib import Path
from pprint import pprint

def convert_string_to_float(value):
    """Converte strings numéricas para float"""
    try:
        if isinstance(value, str):
            if value.strip() == '':
                return None
            return float(value)
        return value
    except:
        return None

def convert_json_string(value):
    """Converte string JSON para dicionário"""
    try:
        if isinstance(value, str) and value.strip():
            return json.loads(value)
        return None
    except:
        return None

def process_dataframe(df):
    """Processa o DataFrame aplicando as conversões necessárias"""
    # Colunas numéricas que precisam ser convertidas para float
    numeric_columns = [
        'contract_original_total_value', 'contract_original_principal',
        'contract_original_interest', 'contract_original_iof',
        'contract_original_operation_fee', 'contract_original_monthly_interest_rate',
        'contract_original_daily_interest_rate', 'installment_value',
        'contract_original_anual_interest_rate', 'contract_original_anual_daily_basis',
        'installment_original_total_value', 'installment_original_principal',
        'installment_original_interest', 'installment_current_penalties',
        'installment_current_discount', 'paid_total_value', 'paid_principal',
        'paid_interest', 'paid_penalties'
    ]

    # Converter colunas numéricas
    for col in numeric_columns:
        if col in df.columns:
            df[col] = df[col].apply(convert_string_to_float)

    # Converter coluna de pagamentos (JSON string para dicionário)
    if 'payments' in df.columns:
        df['payments'] = df['payments'].apply(convert_json_string)

    # Converter colunas de data
    date_columns = [
        'contract_created_date', 'contract_approved_date',
        'contract_signed_date', 'contract_granted_date',
        'contract_fully_paid_date', 'installment_created',
        'payment_plan_created', 'installment_due_date',
        'installment_paid_date'
    ]

    for col in date_columns:
        if col in df.columns:
            # Converter datas para string ISO format para evitar problemas de serialização
            df[col] = pd.to_datetime(df[col], errors='coerce').apply(
                lambda x: x.isoformat() if pd.notnull(x) else None
            )

    return df

try:
    # Definir caminhos dos arquivos
    data_dir = Path('/home/ofb100707/Documents/PDI/001_MultiAgents/data')
    files = [
        data_dir / 'internal_data_part_1.csv',
        data_dir / 'internal_data_part_2.csv',
        data_dir / 'internal_data_part_3.csv'
    ]

    # Lista para armazenar todos os DataFrames
    all_dfs = []

    # Ler e processar cada arquivo
    for file_path in files:
        if file_path.exists():
            print(f"Processando arquivo: {file_path.name}")
            # Adicionar low_memory=False para evitar warnings de tipos mistos
            df = pd.read_csv(file_path, low_memory=False)
            df_processed = process_dataframe(df)
            all_dfs.append(df_processed)
        else:
            print(f"Arquivo não encontrado: {file_path}")

    # Concatenar todos os DataFrames
    if all_dfs:
        final_df = pd.concat(all_dfs, ignore_index=True)
        print(f"Total de registros combinados: {len(final_df)}")

        # Conectar ao MongoDB
        client = MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=5000)
        # Verificar conexão
        client.server_info()
        
        # Limpar banco de dados existente
        client.drop_database('open')
        print("Banco de dados anterior removido com sucesso")
        
        # Criar novo banco e collection
        db = client['open']
        collection = db['loans']

        # Converter DataFrame para lista de dicionários e inserir no MongoDB
        # Substituir NaN por None para evitar problemas de serialização
        records = json.loads(final_df.replace({pd.NA: None}).to_json(orient='records'))
        result = collection.insert_many(records)

        print(f'Foram inseridos {len(result.inserted_ids)} documentos no MongoDB no banco open')

        # Criar índices para melhorar a performance das consultas
        collection.create_index([("contract_id", 1)])
        collection.create_index([("installment_id", 1)])
        collection.create_index([("ccb_number", 1)])
        collection.create_index([("contract_status", 1)])
        collection.create_index([("installment_status", 1)])
        collection.create_index([("contract_funding_source", 1)])
        print("Índices criados com sucesso")

        # Mostrar um exemplo dos dados inseridos para validação
        # print("\nExemplo do primeiro registro inserido:")
        # primeiro_registro = collection.find_one()
        # for key, value in primeiro_registro.items():
        #     if key != '_id':  # Não mostrar o ID do MongoDB
        #         print(f"{key}: {value}")

    else:
        print("Nenhum arquivo foi processado com sucesso")

except Exception as e:
    print(f"Erro inesperado: {str(e)}")
finally:
    if 'client' in locals():
        client.close()
        print("\nConexão com MongoDB fechada")