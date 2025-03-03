# Sistema Multi-Agente para Análise de Dados de Empréstimos

## Visão Geral
Este projeto implementa um sistema multi-agente usando CrewAI para analisar dados de empréstimos em diferentes bancos de dados. O sistema processa dados internos, empréstimos liquidados e dados de estoque atual para identificar inconsistências e gerar relatórios.

## Agentes do Sistema

O sistema possui dois agentes principais:

1. **Engenheiro de Dados**
   - Responsável por transformar dados de arquivos CSV
   - Cria representações no banco de dados MongoDB

2. **Analista de Dados**
   - Compara os bancos para encontrar inconsistências
   - Gera relatórios de análise

## Estrutura do Projeto
```
001_MultiAgents/
├── data/                        # Diretório de arquivos de dados
│   ├── internal_data_*.csv      
│   ├── liquidated.csv          
│   └── stock.csv               
├── docs/                        # Documentação
│   ├── estrategia.md          
│   └── pdi.md                
├── results/                     # Resultados da análise
│   ├── internal_inconsistencies/ # Inconsistências entre Liquidated e Internal
│   │   ├── general_report.json
│   │   ├── general_report.txt
│   │   └── inconsistencies_*.json
│   └── stock_inconsistencies/   # Inconsistências entre Liquidated e Stock
│       ├── general_report.json
│       ├── general_report.txt
│       └── inconsistencies_*.json
├── tools/processamento_de_dados # Scripts de processamento
│   ├── script_internal_data.py
│   ├── script_liquidated.py
│   └── script_stock.py
├── main.py                      # Arquivo principal
└── requirements.txt             # Dependências Python
```

## Lógica de Análise

### Comparação entre Liquidated e Internal
- Base Liquidated: `investment_funds.liquidated`
- Base Internal: `open.loans`

### Comparação entre Liquidated e Stock
- Base Liquidated: `investment_funds.liquidated`
- Base Stock: `investment_funds.stock`

## Requisitos
- Python 3.x
- MongoDB
- Dependências Python listadas em requirements.txt:
  - crewai==0.19.0
  - langchain==0.1.5
  - openai==1.12.0
  - python-dotenv==1.0.1
  - duckduckgo-search==4.2
  - pandas==2.2.0
  - pymongo==4.6.1

## Instalação e Configuração

1. Clone o repositório:
```bash
git clone https://github.com/fbignoto/MultiAgents.git
```

2. Instale as dependências:
```bash
pip install -r requirements.txt
```

## Uso

Execute o script principal:
```bash
python main.py
```


## Relatórios Gerados

Os relatórios são organizados em duas categorias principais na pasta `results`:

1. **internal_inconsistencies/**
   - Inconsistências entre Liquidated e Internal
   - Relatórios diários em formato JSON
   - Relatório geral em JSON e TXT

2. **stock_inconsistencies/**
   - Inconsistências entre Liquidated e Stock
   - Relatórios diários em formato JSON
   - Relatório geral em JSON e TXT

## Observações Importantes

- O sistema verifica automaticamente o status do MongoDB
- Será solicitada a senha do MongoDB quando necessário
- Os relatórios são gerados diariamente e consolidados
- O projeto está em desenvolvimento contínuo com melhorias planejadas

