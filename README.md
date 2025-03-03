# Sistema Multi-Agente para Análise de Dados de Empréstimos

## Visão Geral
Este projeto implementa um sistema multi-agente usando CrewAI para analisar dados de empréstimos em diferentes bancos de dados. O sistema processa dados internos, empréstimos liquidados e dados de estoque atual para identificar inconsistências e gerar relatórios.

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
│   └── inconsistencies_*.json
├── tools/processamento_de_dados # Scripts de processamento
│   ├── script_internal_data.py
│   ├── script_liquidated.py
│   └── script_stock.py
├── main.py                      # Arquivo principal
└── requirements.txt             # Dependências Python
```

## Funcionalidades
- Processamento de dados de múltiplas fontes (dados internos, liquidados e estoque)
- Integração com MongoDB para armazenamento de dados
- Detecção automatizada de inconsistências entre bases de dados
- Geração de relatórios diários de inconsistências
- Sistema multi-agente usando CrewAI para delegação e execução de tarefas

## Requisitos
- Python 3
- MongoDB
- Pacotes Python necessários (ver requirements.txt)

## Instalação
1. Clone o repositório
2. Instale as dependências:
```bash
pip install -r requirements.txt
```

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

## Saída
Os resultados são salvos em formato JSON no diretório 'results', organizados por comparações e data:
- inconsistencies_YYYYMMDD.json

