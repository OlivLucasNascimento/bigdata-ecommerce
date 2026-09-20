
# Big Data E-commerce

Trabalho prático de Big Data: monitoramento de vendas e logística de um e-commerce.

## Etapa 1 — Geração e ingestão de dados

Tecnologias utilizadas:
- Python 3
- Apache Flume
- Apache Hadoop HDFS
- Docker Compose

### Funcionamento

O gerador Python produz eventos JSON continuamente.

Os eventos são armazenados no arquivo dados/eventos.jsonl.

O Apache Flume coleta os eventos e os envia ao HDFS.

### Estrutura do projeto

- gerador/: gerador de eventos Python.
- flume/: configurações e scripts do Apache Flume.
- dados/: armazenamento local dos eventos.
- hadoop/: arquivos de configuração adicionais, quando necessários.
- docker-compose.yml: configuração dos serviços Docker.

### Executar a primeira etapa

Iniciar os serviços:

docker compose up -d --build

Executar o gerador:

python gerador/gerador.py

Verificar os dados no HDFS:

docker compose exec namenode hdfs dfs -ls /dados/ecommerce

### Próximas etapas

- Apache Flink: processamento em tempo real.
- Apache Spark: processamento em lote.
- HBase e Hive: armazenamento dos resultados.