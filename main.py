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
import subprocess
import time

# Adiciona o diretório tools ao PYTHONPATH
tools_path = os.path.join(os.path.dirname(__file__), 'tools/processamento_de_dados')
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

def ensure_mongodb_running():
    """Verifica se o MongoDB está rodando e inicia se necessário"""
    try:
        # Tenta conectar ao MongoDB
        client = MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=2000)
        client.server_info()
        print("MongoDB já está rodando")
        client.close()
        return True
    except Exception:
        print("MongoDB não está rodando. Tentando iniciar...")
        try:
            # Tenta iniciar o MongoDB
            subprocess.run(['sudo', 'systemctl', 'start', 'mongod'], check=True)
            
            # Espera alguns segundos para o serviço iniciar
            time.sleep(5)
            
            # Verifica se iniciou com sucesso
            client = MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=2000)
            client.server_info()
            print("MongoDB iniciado com sucesso")
            client.close()
            return True
        except subprocess.CalledProcessError:
            print("Erro ao iniciar MongoDB via systemctl. Tentando método alternativo...")
            try:
                # Tenta iniciar via comando mongod
                subprocess.Popen(['mongod', '--dbpath', '/var/lib/mongodb'], 
                               stdout=subprocess.DEVNULL,
                               stderr=subprocess.DEVNULL)
                time.sleep(5)
                
                # Verifica se iniciou
                client = MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=2000)
                client.server_info()
                print("MongoDB iniciado com sucesso (método alternativo)")
                client.close()
                return True
            except Exception as e:
                print(f"Erro ao iniciar MongoDB: {str(e)}")
                return False

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

# Funções de processamento de dados
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

# Funções de análise de dados
def check_loan_inconsistency(loan: Dict, loans_collection, stock_collection) -> List[Dict]:
    """Verifica inconsistências para um empréstimo específico"""
    inconsistencies = []
    
    # Usa cache para buscar empréstimo interno
    internal_loan = get_internal_loan(loan['DOCUMENTO'], loans_collection)
    
    if internal_loan:
        if internal_loan['contract_status'] != 'FULLY_PAID':
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

def save_inconsistencies_batch(inconsistencies: List[Dict], date: str, comparison_type: str):
    """Salva um lote de inconsistências em um arquivo JSON, organizado por data e tipo de comparação"""
    # Criar diretórios se não existirem
    base_dir = RESULTS_DIR / comparison_type
    base_dir.mkdir(exist_ok=True)
    
    filepath = base_dir / f"inconsistencies_{date}.json"
    
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

def save_general_report(daily_summary: Dict, total_loans: Dict, report_type: str):
    """Salva um relatório geral com estatísticas de todas as inconsistências"""
    general_stats = {
        'total_registros': {
            'open': total_loans['open'],
            'settled': total_loans['settled'],
            'stock': total_loans['stock']
        },
        'inconsistencias_por_tipo': {},
        'porcentagem_por_tipo': {}
    }

    # Calcular totais por tipo de inconsistência
    for date_stats in daily_summary.values():
        for tipo, quantidade in date_stats['by_type'].items():
            if tipo not in general_stats['inconsistencias_por_tipo']:
                general_stats['inconsistencias_por_tipo'][tipo] = 0
            general_stats['inconsistencias_por_tipo'][tipo] += quantidade

    # Calcular porcentagens
    total_settled = total_loans['settled']
    if total_settled > 0:
        for tipo, quantidade in general_stats['inconsistencias_por_tipo'].items():
            general_stats['porcentagem_por_tipo'][tipo] = (quantidade / total_settled) * 100

    # Gerar relatório em formato texto
    report = f"Relatório de Inconsistências - {report_type}\n"
    report += "=" * 40 + "\n\n"
    
    report += "Total de Registros por Base:\n"
    report += f"- Base Interna (open): {general_stats['total_registros']['open']:,}\n"
    report += f"- Base de Liquidações: {general_stats['total_registros']['settled']:,}\n"
    report += f"- Base de Estoque: {general_stats['total_registros']['stock']:,}\n\n"
    
    report += "Total de Inconsistências por Tipo:\n"
    for tipo, quantidade in general_stats['inconsistencias_por_tipo'].items():
        report += f"- {tipo}: {quantidade:,}\n"
    
    report += "\nPorcentagem de Inconsistências (em relação ao total de liquidações):\n"
    for tipo, porcentagem in general_stats['porcentagem_por_tipo'].items():
        report += f"- {tipo}: {porcentagem:.2f}%\n"

    # Salvar relatório em JSON
    base_dir = RESULTS_DIR / f"{report_type}_inconsistencies"
    base_dir.mkdir(exist_ok=True)
    
    filepath = base_dir / "general_report.json"
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(general_stats, f, indent=2, ensure_ascii=False)

    # Salvar relatório em texto
    filepath_txt = base_dir / "general_report.txt"
    with open(filepath_txt, 'w', encoding='utf-8') as f:
        f.write(report)

    return report

# Funções de comparação de bancos
def compare_internal_liquidated(loan: Dict, loans_collection, daily_summary: Dict, date_str: str) -> List[Dict]:
    """Verifica inconsistências entre base liquidada e base interna"""
    current_inconsistencies = []
    
    ccb_number = loan.get('DOCUMENTO')
    if not ccb_number:
        return []
        
    # Verificar na base interna
    internal_loan = get_internal_loan(ccb_number, loans_collection)
    if internal_loan:
        if internal_loan['contract_status'] != 'FULLY_PAID':
            inc = {
                'tipo': 'Status Inconsistente',
                'documento': ccb_number,
                'status_liquidacao': 'LIQUIDADO',
                'status_interno': internal_loan['contract_status'],
                'data_movimento': loan.get('DATA_MOVIMENTO').isoformat()
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
            'data_movimento': loan.get('DATA_MOVIMENTO').isoformat()
        }
        current_inconsistencies.append(inc)
        daily_summary[date_str]['total'] += 1
        daily_summary[date_str]['by_type'].setdefault('Não Encontrado', 0)
        daily_summary[date_str]['by_type']['Não Encontrado'] += 1
    
    return current_inconsistencies

def compare_stock_liquidated(loan: Dict, stock_collection, daily_summary: Dict, date_str: str) -> List[Dict]:
    """Verifica inconsistências entre base liquidada e estoque"""
    current_inconsistencies = []
    
    ccb_number = loan.get('DOCUMENTO')
    if not ccb_number:
        return []
        
    # Verificar no estoque
    stock_loan = get_stock_loan(ccb_number, stock_collection)
    if stock_loan:
        inc = {
            'tipo': 'Conflito Estoque/Liquidação',
            'documento': ccb_number,
            'detalhes': 'Empréstimo consta como liquidado mas ainda está no estoque',
            'data_movimento': loan.get('DATA_MOVIMENTO').isoformat()
        }
        current_inconsistencies.append(inc)
        daily_summary[date_str]['total'] += 1
        daily_summary[date_str]['by_type'].setdefault('Conflito Estoque/Liquidação', 0)
        daily_summary[date_str]['by_type']['Conflito Estoque/Liquidação'] += 1
    
    return current_inconsistencies

def compare_databases() -> str:
    """Compara os dados entre os bancos para encontrar inconsistências, agrupando por dia"""
    try:
        client = MongoClient('mongodb://localhost:27017/')
        
        # Conectar aos bancos
        db_open = client['open']
        db_investment = client['investment_funds']
        
        # Collections
        loans = db_open['loans']
        settled = db_investment['liquidated']
        stock = db_investment['stock']
        
        # Criar índices para otimizar as consultas
        loans.create_index([("ccb_number", ASCENDING)])
        settled.create_index([("DOCUMENTO", ASCENDING)])
        stock.create_index([("NU_DOCUMENTO", ASCENDING)])
        
        # Contar total de registros em cada base
        total_loans = {
            'open': loans.count_documents({}),
            'settled': settled.count_documents({}),
            'stock': stock.count_documents({})
        }
        
        # Dicionários para contagem de inconsistências por dia
        daily_summary_internal = {}
        daily_summary_stock = {}
        
        # Processar em lotes menores
        cursor = settled.find(no_cursor_timeout=True).batch_size(BATCH_SIZE)
        total_processed = 0
        current_batch_internal = []
        current_batch_stock = []
        current_date = None
        
        print("Processando empréstimos liquidados...")
        for loan in cursor:
            # Extrair a data do movimento
            movement_date = loan.get('DATA_MOVIMENTO')
            if not movement_date:
                continue
                
            date_str = movement_date.strftime("%Y%m%d")
            
            # Inicializar contadores para o dia em ambos os sumários
            for summary in [daily_summary_internal, daily_summary_stock]:
                if date_str not in summary:
                    summary[date_str] = {
                        'total': 0,
                        'by_type': {}
                    }
            
            # Verificar inconsistências com base interna
            internal_inconsistencies = compare_internal_liquidated(
                loan, loans, daily_summary_internal, date_str
            )
            
            # Verificar inconsistências com estoque
            stock_inconsistencies = compare_stock_liquidated(
                loan, stock, daily_summary_stock, date_str
            )
            
            # Se mudou a data ou o lote está cheio, salva os lotes atuais
            if current_date != date_str or len(current_batch_internal) >= BATCH_SIZE:
                if current_batch_internal:
                    save_inconsistencies_batch(current_batch_internal, current_date, "internal_inconsistencies")
                if current_batch_stock:
                    save_inconsistencies_batch(current_batch_stock, current_date, "stock_inconsistencies")
                current_batch_internal = []
                current_batch_stock = []
                current_date = date_str
            
            # Adiciona inconsistências aos lotes atuais
            current_batch_internal.extend(internal_inconsistencies)
            current_batch_stock.extend(stock_inconsistencies)
            
            total_processed += 1
            if total_processed % 500 == 0:
                print(f"Processados {total_processed} empréstimos...")
                # Limpa os caches periodicamente
                get_internal_loan.cache_clear()
                get_stock_loan.cache_clear()
        
        # Salva os últimos lotes se houver
        if current_batch_internal and current_date:
            save_inconsistencies_batch(current_batch_internal, current_date, "internal_inconsistencies")
        if current_batch_stock and current_date:
            save_inconsistencies_batch(current_batch_stock, current_date, "stock_inconsistencies")
        
        print(f"Total de {total_processed} empréstimos processados")
        
        # Fechar cursor
        cursor.close()
        
        # Gerar relatórios separados para cada tipo de comparação
        general_report_internal = save_general_report(daily_summary_internal, total_loans, "internal")
        general_report_stock = save_general_report(daily_summary_stock, total_loans, "stock")
        
        # Gerar relatório final combinado
        final_report = "Relatório de Inconsistências:\n\n"
        final_report += "=== Inconsistências com Base Interna ===\n"
        final_report += general_report_internal
        final_report += "\n\n=== Inconsistências com Base de Estoque ===\n"
        final_report += general_report_stock
        
        client.close()
        return final_report
        
    except Exception as e:
        return f"Erro ao comparar bancos de dados: {str(e)}"

# Função principal
def main():
    try:
        load_dotenv()

        # Verificar se o MongoDB está rodando antes de prosseguir
        if not ensure_mongodb_running():
            print("Não foi possível iniciar o MongoDB. Encerrando...")
            return

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