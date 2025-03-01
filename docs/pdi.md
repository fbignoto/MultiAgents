PDI - Tarefa 1

https://www.deeplearning.ai/short-courses/multi-ai-agent-systems-with-crewai/

Aprofundar a compreensão dos conceitos e técnicas relacionadas a sistemas com múltiplos agentes de IA.
Conhecer a plataforma CreaWAI e suas aplicações práticas em projetos de multi-agentes.
Aprender a projetar, implementar e coordenar sistemas de IA colaborativos.
Desenvolver habilidades na integração e comunicação entre agentes, essenciais para resolver problemas complexos.

Tarefa associada:
Implemente uma solução multi-agentes com CrewAI.
Essa solução deve ser capaz de:

[Visão de um fundo de investimentos]
1- Dados de Stock (Representa a visão financeira atual de todos os empréstimos de um determinado fundo de investimentos)
    - Processar um arquivo .csv com os dados financeiros do stock de um fundo de investimentos(stock.csv);
    - Salvar o conteúdo desse .csv em um banco de dados (pode ser local, relacional ou não);

2- Dados de Liquidação (Representa a visão financeira atual dos empréstimos já quitados em um determinado fundo de investimentos)
    - Processar um arquivo .csv com os dados financeiros dos empréstimos já quitados em um fundo de investimentos(liquidated.csv);
    - Salvar o conteúdo desse .csv em um banco de dados (pode ser local, relacional ou não);

[Visão interna da Open Co]
3- Dados de empréstimo (Representa a visão financeira de um empréstimo do ponto de vista da Open Co)
    - Processar arquivos .csv com dados financeiros(internal_data_part_1, 2 e 3.csv);
    - Salvar o conteúdo desse arquivo .csv em um banco de dados mongoDB;

[Cruzamento de dados]
4- As informações de pagamentos que ocorrem internamente, são enviadas ao fundo de investimentos diariamente;
Dessa forma, o status de um empréstimo deve ser a mesma na visão da Open Co e na visão do fundo;
Ou seja, o que está pago no fundo, deve estar pago na base interna, e vice-versa;
A solução deve ser capaz de cruzar essas informações e identificar inconsistências.
Para isso, encontre inconsistências entre o stock e os dados internos e entre liquidated e os dados internos.
Note que esse cruzamento é diário, ou seja, o que foi pago (nos dados internos no dia d) deve constar pago no stock no dia d, ou constar no liquidated no dia d;

[Correção das inconsistências]
5- Considere que existe um endpoint disponivel em uma API Rest;
Este endpoint recebe um arquivo com extensao .rem que contem os campos:
- ccb_number
- paid_value
- installment_number
- paid_date
Cada arquivo deve conter os pagamentos de um determinado dia.


[Instruções Gerais]
6- Você tem liberdade para definir a arquitetura, padrão de projeto, definir as atribuições de cada agente, tools e prompt.
Sempre analise os porquês de cada decisão.
Tente analisar os dados antes de projetar e implementar a solução, para saber quais dados são chaves para realizar o cruzamento.
Caso tenha qualquer dúvida ou dificuldade, podemos conversar para esclarecer.

Fora isso, teremos um encontro na semana que vem para isso também.

A data para encerrarmos essa tarefa é dia 25/02.
Vamos apresentar e discutir a solução de forma conjunta.
