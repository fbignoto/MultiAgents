# Sistema Multi-Agente para Análise de Dados de Empréstimos

## Visão Geral
Este projeto implementa um sistema multi-agente usando CrewAI para analisar dados de empréstimos em diferentes bancos de dados. O sistema processa dados internos, empréstimos liquidados e dados de estoque atual para identificar inconsistências e gerar relatórios.

## Estrutura do Projeto
```
001_MultiAgents/
├── data/                    # Diretório de arquivos de dados
│   ├── internal_data_*.csv  # Arquivos de dados internos
│   ├── liquidated.csv       # Dados de empréstimos liquidados
│   └── stock.csv           # Dados de estoque atual
├── docs/                    # Documentação
│   ├── estrategia.md       # Documentação da estratégia
│   └── pdi.md              # Documentação do projeto
├── results/                 # Resultados da análise
│   └── inconsistencies_*.json
├── tools/                   # Scripts de processamento
│   ├── script_internal_data.py
│   ├── script_liquidated.py
│   └── script_stock.py
├── main.py                 # Arquivo principal
└── requirements.txt        # Dependências Python
```

## Funcionalidades
- Processamento de dados de múltiplas fontes (dados internos, liquidados e estoque)
- Integração com MongoDB para armazenamento de dados
- Detecção automatizada de inconsistências entre bases de dados
- Geração de relatórios diários de inconsistências
- Sistema multi-agente usando CrewAI para delegação e execução de tarefas

## Requisitos
- Python 3.10+
- MongoDB
- Pacotes Python necessários (ver requirements.txt)

## Instalação
1. Clone o repositório
2. Instale as dependências:
```bash
pip install -r requirements.txt
```
3. Certifique-se que o MongoDB está rodando localmente na porta padrão (27017)

## Uso
Execute o script principal para processar e analisar os dados:
```bash
python main.py
```

O script irá:
1. Processar dados internos de empréstimos
2. Processar dados de empréstimos liquidados
3. Processar dados de estoque atual
4. Analisar inconsistências entre as bases de dados
5. Gerar relatórios no diretório results

## Fluxo de Processamento de Dados
1. Processamento de Dados Internos
   - Lê e combina múltiplos arquivos CSV
   - Converte tipos de dados e formatos
   - Armazena no banco de dados 'open' do MongoDB

2. Processamento de Empréstimos Liquidados
   - Processa dados de empréstimos liquidados
   - Armazena no banco de dados 'investment_funds' do MongoDB

3. Processamento de Dados de Estoque
   - Processa informações de estoque atual
   - Armazena no banco de dados 'investment_funds' do MongoDB

4. Análise de Inconsistências
   - Compara dados entre as bases
   - Identifica inconsistências
   - Gera relatórios diários

## Saída
Os resultados são salvos em formato JSON no diretório 'results', organizados por data:
- inconsistencies_YYYYMMDD.json

Cada relatório contém:
- Inconsistências de status
- Conflitos entre estoque/liquidação
- Registros não encontrados
- Informações detalhadas para cada inconsistência

## Status do Projeto
Em desenvolvimento ativo