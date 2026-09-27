# Big Data E-commerce

Projeto acadêmico de Big Data que simula eventos de um e-commerce e implementa um pipeline completo de **geração, ingestão, armazenamento, processamento em tempo real e processamento em lote**.

## Arquitetura

```text
Python (Gerador)
        |
        v
Apache Flume
        |
        v
HDFS (/dados/ecommerce)
        |
        +-------------------------+
        |                         |
        v                         v
Apache Flink                 Apache Spark
Streaming                    Processamento Batch
        |                         |
        v                         v
Watermarks + Janelas         ETL + RDD + Spark SQL
        |                         |
        v                         v
Apache HBase                 Apache Hive
alertas_vendas               indicadores_vendas
```

## Tecnologias utilizadas

- Python 3
- Docker / Docker Compose
- Apache Hadoop HDFS
- Apache Flume
- Apache Flink / PyFlink
- Apache HBase
- Apache Spark / PySpark
- Apache Hive
- PostgreSQL

---

## Etapa 1 — Geração e ingestão de dados

A primeira etapa é responsável pela geração dos eventos simulados do e-commerce e pela ingestão desses dados no HDFS.

O gerador Python produz eventos em formato JSON dos seguintes tipos:

- clique
- carrinho
- compra
- entrega

Os eventos possuem informações como:

- `evento_id`
- `tipo_evento`
- `cliente_id`
- `produto_id`
- `produto_nome`
- `quantidade`
- `valor`
- `timestamp`

Eventos de carrinho e entrega também podem possuir informações específicas de status.

Parte dos eventos recebe timestamps atrasados para simular **eventos fora de ordem**, permitindo posteriormente testar o funcionamento de watermarks no Apache Flink.

O gerador grava continuamente os eventos no arquivo:

```text
dados/eventos.jsonl
```

O Apache Flume acompanha esse arquivo e envia os novos eventos para o HDFS:

```text
hdfs://namenode:9000/dados/ecommerce
```

---

## Etapa 2 — Processamento em tempo real

A segunda etapa utiliza o Apache Flink para realizar o processamento em tempo real dos eventos armazenados no HDFS.

O job Flink utiliza:

- Event Time
- Watermarks
- Sliding Windows
- Filtro de eventos
- Processamento de carrinhos abandonados

O Flink utiliza o timestamp presente no próprio evento como referência de tempo.

Foi configurada uma tolerância para eventos atrasados utilizando watermarks, permitindo que eventos fora de ordem ainda sejam processados corretamente.

Os eventos são agrupados por produto em janelas deslizantes.

Quando a quantidade de carrinhos abandonados de um produto atinge o limite configurado, um alerta é gerado.

Os alertas são armazenados no Apache HBase na tabela:

```text
alertas_vendas
```

Os principais campos armazenados são:

- produto
- total de carrinhos abandonados
- início da janela
- fim da janela

---

## Etapa 3 — Processamento em lote

A terceira etapa utiliza Apache Spark para realizar o processamento em lote do histórico de eventos armazenado no HDFS.

O job foi desenvolvido em PySpark e implementa um processo ETL dividido em:

1. Extração
2. Transformação
3. Carregamento

### Extração

O Spark lê todos os arquivos disponíveis em:

```text
hdfs://namenode:9000/dados/ecommerce/*
```

### Transformação

São considerados principalmente os eventos do tipo:

```text
compra
```

O processamento utiliza operações como:

- RDD
- `map`
- `filter`
- `reduceByKey`
- Spark SQL
- `CROSS JOIN`

O `reduceByKey` é utilizado como exemplo de **wide dependency**, pois exige redistribuição dos dados entre as partições.

O Spark SQL também é utilizado para calcular a participação percentual de cada produto no faturamento total.

### Indicadores gerados

São calculados por produto:

- total de pedidos
- quantidade de itens vendidos
- faturamento
- participação percentual no faturamento total

Os resultados são armazenados no Apache Hive na tabela:

```text
indicadores_vendas
```

A gravação utiliza modo `overwrite`, pois cada execução reprocessa todo o histórico disponível no HDFS.

---

## Etapa 4 — Integração e validação final

Na etapa final, todos os componentes foram executados no mesmo ambiente Docker.

O pipeline completo foi validado nos seguintes fluxos:

```text
Python → Flume → HDFS
```

```text
HDFS → Flink → HBase
```

```text
HDFS → Spark → Hive
```

Durante a validação final:

- novos arquivos `FlumeData.*` foram gerados no HDFS;
- o Flink processou os eventos e gerou alertas;
- os alertas foram confirmados na tabela `alertas_vendas` do HBase;
- o Spark processou o histórico de eventos;
- os indicadores foram gravados na tabela `indicadores_vendas`;
- os dados foram consultados diretamente no Hive através do Beeline.

---

# Como executar

## 1. Subir o ambiente

Com o Docker Desktop iniciado, execute:

```bash
docker compose up -d --build
```

---

## 2. Verificar os containers

```bash
docker compose ps
```

Os principais serviços do projeto são:

```text
namenode
datanode
flume
flink-jobmanager
flink-taskmanager
hbase
hive-metastore-postgresql
hive-metastore
hive-server
spark-master
spark-worker
```

---

## 3. Criar o diretório no HDFS

```bash
docker compose exec namenode hdfs dfs -mkdir -p /dados/ecommerce
```

---

## 4. Executar o gerador de eventos

```bash
python gerador/gerador.py
```

O gerador ficará executando continuamente.

Para interromper:

```text
Ctrl + C
```

---

## 5. Verificar os dados no HDFS

```bash
docker compose exec namenode hdfs dfs -ls /dados/ecommerce
```

Devem aparecer arquivos semelhantes a:

```text
FlumeData.1790528691705
FlumeData.1790528724118
FlumeData.1790528754666
```

---

## 6. Executar o processamento em tempo real com Flink

```bash
docker compose exec flink-jobmanager python3 -u /app/jobs/streaming_job.py
```

Quando os critérios de alerta forem atingidos, serão exibidas mensagens semelhantes a:

```text
[ALERTA GRAVADO]
```

---

## 7. Consultar os alertas no HBase

Entre no HBase Shell:

```bash
docker compose exec hbase hbase shell
```

Depois execute:

```text
scan 'alertas_vendas'
```

Para sair:

```text
exit
```

---

## 8. Executar o processamento em lote com Spark

O executável `spark-submit` está localizado em:

```text
/spark/bin/spark-submit
```

Execute:

```bash
docker compose exec spark-master /spark/bin/spark-submit /app/jobs/batch_job.py
```

O Spark exibirá no terminal os indicadores calculados por produto.

---

## 9. Consultar os indicadores no Hive

Execute:

```bash
docker compose exec hive-server /opt/hive/bin/beeline -u "jdbc:hive2://localhost:10000/default" -n root -e "SELECT * FROM indicadores_vendas;"
```

A consulta deverá retornar os indicadores armazenados na tabela Hive.

Exemplo de resultado obtido durante a validação integrada:

| Produto | Pedidos | Itens | Faturamento | Participação |
|---|---:|---:|---:|---:|
| Notebook | 12 | 23 | 80500.0 | 60.36% |
| Smartphone | 6 | 15 | 27000.0 | 20.25% |
| Monitor | 11 | 22 | 20900.0 | 15.67% |
| Teclado | 10 | 17 | 2718.3 | 2.04% |
| Mouse | 11 | 25 | 2247.5 | 1.69% |

Os valores podem variar de acordo com a quantidade de eventos armazenados no HDFS no momento da execução.

---

# Estrutura principal do projeto

```text
bigdata-ecommerce/
|
|-- dados/
|   `-- eventos.jsonl
|
|-- gerador/
|   `-- gerador.py
|
|-- flume/
|   |-- Dockerfile
|   |-- flume.conf
|   `-- acompanhar.sh
|
|-- flink/
|   |-- Dockerfile
|   `-- jobs/
|       `-- streaming_job.py
|
|-- spark/
|   |-- conf/
|   |   `-- hive-site.xml
|   `-- jobs/
|       `-- batch_job.py
|
|-- docker-compose.yml
|-- .gitignore
|-- .gitattributes
`-- README.md
```

---

# Resultado final

O projeto implementa uma arquitetura de Big Data capaz de trabalhar com processamento em tempo real e processamento em lote utilizando a mesma fonte de dados.

Fluxo de ingestão:

```text
Python → Flume → HDFS
```

Processamento em tempo real:

```text
HDFS → Flink → HBase
```

Processamento em lote:

```text
HDFS → Spark → Hive
```

Todos os componentes foram integrados e validados utilizando Docker Compose.

## Status

- [x] Geração de eventos com Python
- [x] Simulação de eventos fora de ordem
- [x] Ingestão com Apache Flume
- [x] Armazenamento no HDFS
- [x] Processamento em tempo real com Flink
- [x] Watermarks
- [x] Sliding Windows
- [x] Alertas de carrinhos abandonados
- [x] Persistência no HBase
- [x] Processamento em lote com Spark
- [x] ETL
- [x] RDD e Wide Dependencies
- [x] Spark SQL
- [x] Indicadores de negócio
- [x] Persistência no Hive
- [x] Integração completa do pipeline
