Extração de Dados: Conectar ao banco de dados SQLite e extrair os dados.
Transformação: Processar os dados extraídos conforme solicitado.
Carregamento: Simular o carregamento dos dados transformados em uma estrutura de partições no S3 (usaremos um diretório local).
Consulta e Análise: Criar uma consulta SQL para o Amazon Athena.
Visualização: Descrever um mock-up de um dashboard.
Modelo de Machine Learning: Propor um modelo simples para prever vendas futuras.

1. Extração de Dados

python
Copy code
import sqlite3
import pandas as pd

# Conectar ao banco de dados SQLite
conn = sqlite3.connect('vendas.db')

# Criar a tabela e inserir os dados de exemplo (você deve executar isso apenas uma vez)
create_table_query = '''
CREATE TABLE IF NOT EXISTS vendas (
    id INTEGER PRIMARY KEY,
    data_venda DATE,
    id_produto INTEGER,
    id_cliente INTEGER,
    quantidade INTEGER,
    valor_unitario DECIMAL(10, 2),
    valor_total DECIMAL(10, 2),
    id_vendedor INTEGER,
    regiao VARCHAR(50)
);
'''
conn.execute(create_table_query)

# Inserir dados de exemplo
insert_data_query = '''
INSERT INTO vendas (id, data_venda, id_produto, id_cliente, quantidade, valor_unitario, valor_total, id_vendedor, regiao)
VALUES 
(1, '2023-01-15', 101, 1001, 2, 50.00, 100.00, 5, 'Sul'),
(2, '2023-01-16', 102, 1002, 1, 75.50, 75.50, 6, 'Sudeste'),
(3, '2023-01-16', 103, 1003, 3, 25.00, 75.00, 5, 'Sul'),
(4, '2023-01-17', 101, 1004, 1, 50.00, 50.00, 7, 'Norte'),
(5, '2023-01-17', 104, 1002, 2, 30.00, 60.00, 6, 'Sudeste'),
(6, '2023-01-18', 102, 1005, 1, 75.50, 75.50, 8, 'Nordeste'),
(7, '2023-01-18', 103, 1001, 2, 25.00, 50.00, 5, 'Sul'),
(8, '2023-01-19', 104, 1003, 4, 30.00, 120.00, 7, 'Norte'),
(9, '2023-01-19', 101, 1002, 1, 50.00, 50.00, 6, 'Sudeste'),
(10, '2023-01-20', 102, 1004, 2, 75.50, 151.00, 8, 'Nordeste');
'''
conn.execute(insert_data_query)
conn.commit()

# Extrair dados da tabela vendas
df_vendas = pd.read_sql_query("SELECT * FROM vendas", conn)

# Fechar a conexão
conn.close()

print(df_vendas)
2. Transformação
Vamos transformar os dados conforme solicitado:

python
Copy code
# Transformações
df_vendas['data_venda'] = pd.to_datetime(df_vendas['data_venda']).dt.strftime('%Y-%m-%d')  # Formato ISO
df_vendas['data_venda'] = pd.to_datetime(df_vendas['data_venda'])  # Volta para datetime para o cálculo
total_vendas_diarias = df_vendas.groupby('data_venda')['valor_total'].sum().reset_index()
total_vendas_diarias.rename(columns={'valor_total': 'total_vendas'}, inplace=True)

# Remover duplicatas (se existissem)
total_vendas_diarias.drop_duplicates(inplace=True)

print(total_vendas_diarias)
3. Carregamento

python
Copy code
import os

# Criar diretório para simulação do bucket S3
base_path = 's3_bucket'
os.makedirs(base_path, exist_ok=True)

# Salvar dados em formato CSV por ano/mês/dia
for _, row in total_vendas_diarias.iterrows():
    date_str = row['data_venda'].strftime('%Y/%m/%d')
    directory = os.path.join(base_path, date_str)
    os.makedirs(directory, exist_ok=True)
    file_path = os.path.join(directory, 'vendas.csv')
    
    # Salvar/Adicionar dados no CSV
    if os.path.exists(file_path):
        row.to_frame().T.to_csv(file_path, mode='a', header=False, index=False)
    else:
        row.to_frame().T.to_csv(file_path, index=False)

print(f'Dados carregados simuladamente em: {base_path}')
4. Consulta e Análise

sql
Copy code
SELECT
    year(data_venda) AS ano,
    month(data_venda) AS mes,
    SUM(valor_total) AS total_vendas
FROM vendas
GROUP BY ano, mes
ORDER BY ano, mes;

5. Visualização
   
Esboço de Dashboard:

Título: Dashboard de Vendas
Componentes:
Gráfico de barras: Total de vendas por mês.
Tabela: Detalhes de vendas por dia.
Filtros: Região, Vendedor, Período.
KPI: Total de vendas no período selecionado.

6. Modelo de Machine Learning
Modelo Sugerido: Regressão Linear

Objetivo: Prever vendas futuras baseadas em dados históricos.
Características: Data da venda, quantidade, preço unitário, região.
Implementação: Utilizar o Amazon SageMaker para treinar e implantar o modelo.
Considerações Finais
Essa solução alinha-se com uma arquitetura moderna de dados, utilizando ETL (extração, transformação e carregamento), armazenamento em nuvem e análise com SQL. É escalável, já que a estrutura de dados e o pipeline permitem fácil expansão conforme novas demandas de dados surgirem.
