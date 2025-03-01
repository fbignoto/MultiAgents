from dotenv import load_dotenv
import os
from crewai import Agent, Task, Crew
from langchain.tools import Tool
from pathlib import Path
import sys
import importlib.util
from pymongo import MongoClient, ASCENDING
from typing import List, Dict
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
import json
from datetime import datetime

# Adiciona o diretório tools ao PYTHONPATH
tools_path = os.path.join(os.path.dirname(__file__), 'tools')
sys.path.append(tools_path)

# Carrega as variáveis de ambiente
load_dotenv()

# Configurações
BATCH_SIZE = 500  # Reduzindo o tamanho do lote para menor uso de memória
MAX_WORKERS = 2   # Reduzindo o número de workers para evitar sobrecarga
CACHE_SIZE = 100  # Limitando o tamanho do cache

# Definir o diretório base do projeto e criar pasta results
BASE_DIR = Path(__file__).resolve().parent
RESULTS_DIR = BASE_DIR / 'results'
RESULTS_DIR.mkdir(exist_ok=True)
print(f"Pasta 'results' criada/verificada em: {RESULTS_DIR}")

@lru_cache(maxsize=CACHE_SIZE)
def get_internal_loan(ccb_number: str, loans_collection) -> Dict:
    """Cache para busca de empréstimos internos"""
    return loans_collection.find_one({"ccb_number": ccb_number})

@lru_cache(maxsize=CACHE_SIZE)
def get_stock_loan(document: str, stock_collection) -> Dict:
    """Cache para busca de empréstimos no estoque"""
    return stock_collection.find_one({"NU_DOCUMENTO": document})

def process_batch(batch_data: List, func) -> List:
    """Processa um lote de dados"""
    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results.extend(list(executor.map(func, batch_data)))
    return results

def process_internal_data() -> str:
    """Processa os dados internos do sistema"""
    try:
        spec = importlib.util.spec_from_file_location(
            "script_internal_data", 
            os.path.join(tools_path, "script_internal_data.py")
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return "Dados internos processados com sucesso"
    except Exception as e:
        return f"Erro ao processar dados internos: {str(e)}"

def process_liquidated_data() -> str:
    """Processa os dados de empréstimos liquidados"""
    try:
        spec = importlib.util.spec_from_file_location(
            "script_liquidated", 
            os.path.join(tools_path, "script_liquidated.py")
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return "Dados de liquidação processados com sucesso"
    except Exception as e:
        return f"Erro ao processar dados liquidados: {str(e)}"

def process_stock_data() -> str:
    """Processa os dados de estoque atual"""
    try:
        spec = importlib.util.spec_from_file_location(
            "script_stock", 
            os.path.join(tools_path, "script_stock.py")
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return "Dados de estoque processados com sucesso"
    except Exception as e:
        return f"Erro ao processar dados de estoque: {str(e)}"

def check_loan_inconsistency(loan: Dict, loans_collection, stock_collection) -> List[Dict]:
    """Verifica inconsistências para um empréstimo específico"""
    inconsistencies = []
    
    # Usa cache para buscar empréstimo interno
    internal_loan = get_internal_loan(loan['DOCUMENTO'], loans_collection)
    
    if internal_loan:
        if internal_loan['contract_status'] != 'PAID':
            inconsistencies.append({
                'tipo': 'Status Inconsistente',
                'documento': loan['DOCUMENTO'],
                'status_liquidacao': 'LIQUIDADO',
                'status_interno': internal_loan['contract_status']
            })
    else:
        inconsistencies.append({
            'tipo': 'Não Encontrado',
            'documento': loan['DOCUMENTO'],
            'detalhes': 'Empréstimo liquidado não encontrado na base interna'
        })
    
    # Usa cache para verificar no estoque
    stock_loan = get_stock_loan(loan['DOCUMENTO'], stock_collection)
    if stock_loan:
        inconsistencies.append({
            'tipo': 'Conflito Estoque/Liquidação',
            'documento': loan['DOCUMENTO'],
            'detalhes': 'Empréstimo consta como liquidado mas ainda está no estoque'
        })
    
    return inconsistencies

def save_inconsistencies_batch(inconsistencies: List[Dict], date: str):
    """Salva um lote de inconsistências em um arquivo JSON, organizado por data"""
    filepath = RESULTS_DIR / f"inconsistencies_{date}.json"
    
    # Verifica se o arquivo já existe
    existing_data = []
    if filepath.exists():
        with open(filepath, 'r', encoding='utf-8') as f:
            existing_data = json.load(f)
    
    # Adiciona novos dados
    existing_data.extend(inconsistencies)
    
    # Salva o arquivo atualizado
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(existing_data, f, indent=2, ensure_ascii=False)
    
    return filepath

def get_summary(inconsistencies: List[Dict], date: str) -> str:
    """Gera um resumo das inconsistências encontradas para uma data específica"""
    summary = {
        'data': date,
        'total_inconsistencies': len(inconsistencies),
        'by_type': {}
    }
    
    for inc in inconsistencies:
        inc_type = inc['tipo']
        if inc_type not in summary['by_type']:
            summary['by_type'][inc_type] = 0
        summary['by_type'][inc_type] += 1
    
    return summary

def compare_databases() -> str:
    """Compara os dados entre os bancos para encontrar inconsistências, agrupando por dia"""
    try:
        client = MongoClient('mongodb://localhost:27017/')
        
        # Conectar aos bancos
        db_open = client['open']
        db_investment = client['investment_funds']
        
        # Collections
        loans = db_open['loans']
        settled = db_investment['settled_loans']
        stock = db_investment['current_loans']
        
        # Criar índices para otimizar as consultas
        loans.create_index([("ccb_number", ASCENDING)])
        settled.create_index([("DOCUMENTO", ASCENDING)])
        stock.create_index([("NU_DOCUMENTO", ASCENDING)])
        
        # Dicionário para contagem de inconsistências por dia
        daily_summary = {}
        
        # Processar em lotes menores
        cursor = settled.find(no_cursor_timeout=True).batch_size(BATCH_SIZE)
        total_processed = 0
        current_batch = []
        current_date = None
        
        print("Processando empréstimos liquidados...")
        for loan in cursor:
            ccb_number = loan.get('DOCUMENTO')
            if not ccb_number:
                continue
            
            # Extrair a data do movimento
            movement_date = loan.get('DATA_MOVIMENTO')
            if not movement_date:
                continue
                
            date_str = movement_date.strftime("%Y%m%d")
            
            # Inicializar contadores para o dia
            if date_str not in daily_summary:
                daily_summary[date_str] = {
                    'total': 0,
                    'by_type': {}
                }
            
            # Lista para armazenar inconsistências do registro atual
            current_inconsistencies = []
            
            # Verificar na base interna
            internal_loan = get_internal_loan(ccb_number, loans)
            if internal_loan:
                if internal_loan['contract_status'] != 'PAID':
                    inc = {
                        'tipo': 'Status Inconsistente',
                        'documento': ccb_number,
                        'status_liquidacao': 'LIQUIDADO',
                        'status_interno': internal_loan['contract_status'],
                        'data_movimento': movement_date.isoformat()
                    }
                    current_inconsistencies.append(inc)
                    daily_summary[date_str]['total'] += 1
                    daily_summary[date_str]['by_type'].setdefault('Status Inconsistente', 0)
                    daily_summary[date_str]['by_type']['Status Inconsistente'] += 1
            else:
                inc = {
                    'tipo': 'Não Encontrado',
                    'documento': ccb_number,
                    'detalhes': 'Empréstimo liquidado não encontrado na base interna',
                    'data_movimento': movement_date.isoformat()
                }
                current_inconsistencies.append(inc)
                daily_summary[date_str]['total'] += 1
                daily_summary[date_str]['by_type'].setdefault('Não Encontrado', 0)
                daily_summary[date_str]['by_type']['Não Encontrado'] += 1
            
            # Verificar no estoque
            stock_loan = get_stock_loan(ccb_number, stock)
            if stock_loan:
                inc = {
                    'tipo': 'Conflito Estoque/Liquidação',
                    'documento': ccb_number,
                    'detalhes': 'Empréstimo consta como liquidado mas ainda está no estoque',
                    'data_movimento': movement_date.isoformat()
                }
                current_inconsistencies.append(inc)
                daily_summary[date_str]['total'] += 1
                daily_summary[date_str]['by_type'].setdefault('Conflito Estoque/Liquidação', 0)
                daily_summary[date_str]['by_type']['Conflito Estoque/Liquidação'] += 1
            
            # Se mudou a data ou o lote está cheio, salva o lote atual
            if current_date != date_str or len(current_batch) >= BATCH_SIZE:
                if current_batch:
                    save_inconsistencies_batch(current_batch, current_date)
                    current_batch = []
                current_date = date_str
            
            # Adiciona inconsistências ao lote atual
            current_batch.extend(current_inconsistencies)
            
            total_processed += 1
            if total_processed % 500 == 0:
                print(f"Processados {total_processed} empréstimos...")
                # Limpa os caches periodicamente
                get_internal_loan.cache_clear()
                get_stock_loan.cache_clear()
        
        # Salva o último lote se houver
        if current_batch and current_date:
            save_inconsistencies_batch(current_batch, current_date)
        
        print(f"Total de {total_processed} empréstimos processados")
        
        # Fechar cursor
        cursor.close()
        
        # Gerar relatório final
        final_report = "Relatório de Inconsistências por Dia:\n\n"
        for date in sorted(daily_summary.keys()):
            summary = daily_summary[date]
            final_report += f"Data: {date}\n"
            final_report += f"Total de inconsistências: {summary['total']}\n"
            final_report += "Resumo por tipo:\n"
            for tipo, quantidade in summary['by_type'].items():
                final_report += f"- {tipo}: {quantidade}\n"
            final_report += f"Resultados completos salvos em: results/inconsistencies_{date}.json\n\n"
        
        client.close()
        return final_report
        
    except Exception as e:
        return f"Erro ao comparar bancos de dados: {str(e)}"

def main():
    try:
        # Criando as ferramentas para processamento
        processing_tools = [
            Tool(
                name="Processar dados internos",
                func=process_internal_data,
                description="Processa os dados internos do sistema"
            ),
            Tool(
                name="Processar dados liquidados",
                func=process_liquidated_data,
                description="Processa os dados de empréstimos liquidados"
            ),
            Tool(
                name="Processar dados de estoque",
                func=process_stock_data,
                description="Processa os dados de estoque atual"
            )
        ]
        
        # Criando a ferramenta de análise
        analysis_tools = [
            Tool(
                name="Comparar bancos",
                func=compare_databases,
                description="Compara os dados entre os bancos para encontrar inconsistências"
            )
        ]
        
        # Criando o agente de processamento de dados
        data_engineer = Agent(
            role="Engenheiro de Dados",
            goal="Processar e integrar todos os dados no MongoDB com eficiência e precisão",
            backstory="Especialista em engenharia de dados com foco em processamento ETL e integração com MongoDB",
            tools=processing_tools,
            verbose=True
        )

        # Criando o agente analisador de dados
        quality_analyst = Agent(
            role="Analista de Qualidade de Dados",
            goal="Garantir a qualidade e consistência dos dados entre as bases",
            backstory="Especialista em análise de qualidade de dados com foco em validação e identificação de inconsistências entre bases de dados",
            tools=analysis_tools,
            verbose=True
        )

        # Criar as tarefas
        tasks = [
            Task(
                description="Processar dados internos do sistema",
                expected_output="Dados internos processados e armazenados no MongoDB",
                agent=data_engineer
            ),
            Task(
                description="Processar dados de empréstimos liquidados",
                expected_output="Dados de liquidação processados e armazenados no MongoDB",
                agent=data_engineer
            ),
            Task(
                description="Processar dados de estoque atual",
                expected_output="Dados de estoque processados e armazenados no MongoDB",
                agent=data_engineer
            ),
            Task(
                description="Analisar inconsistências entre as bases de dados",
                expected_output="Relatório de inconsistências entre as bases de dados",
                agent=quality_analyst
            )
        ]
        
        # Criar a equipe
        crew = Crew(
            agents=[data_engineer, quality_analyst],
            tasks=tasks,
            verbose=True
        )
        
        # Executar as tarefas
        resultado = crew.kickoff()
        print("Resultado:", resultado)
        
    except Exception as e:
        print(f"Erro durante o processamento: {str(e)}")

if __name__ == "__main__":
    main()