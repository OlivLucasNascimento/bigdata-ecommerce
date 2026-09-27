import json
from pyspark.sql import SparkSession, Row

CAMINHO_EVENTOS = "hdfs://namenode:9000/dados/ecommerce/*"
TABELA_HIVE = "indicadores_vendas"


def main():
    spark = (
        SparkSession.builder
        .appName("ETL - Indicadores de Vendas")
        .enableHiveSupport()
        .getOrCreate()
    )

    sc = spark.sparkContext

    # --- Extracao (RDD) ---
    linhas_rdd = sc.textFile(CAMINHO_EVENTOS)

    def parse_evento(linha):
        try:
            return json.loads(linha)
        except Exception:
            return None

    eventos_rdd = linhas_rdd.map(parse_evento).filter(lambda e: e is not None)

    # Mantem somente eventos de compra (pedidos)
    compras_rdd = eventos_rdd.filter(lambda e: e.get("tipo_evento") == "compra")

    # --- Transformacao (RDD) ---
    # Para cada compra: (produto_nome, (1 pedido, quantidade, valor))
    pares_rdd = compras_rdd.map(
        lambda e: (e["produto_nome"], (1, e.get("quantidade", 0), e.get("valor", 0.0)))
    )

    # reduceByKey provoca shuffle entre particoes = wide dependency
    agregado_rdd = pares_rdd.reduceByKey(
        lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2])
    )

    linhas_resultado = agregado_rdd.map(
        lambda kv: Row(
            produto_nome=kv[0],
            total_pedidos=kv[1][0],
            quantidade_itens=kv[1][1],
            faturamento=round(kv[1][2], 2),
        )
    )

    # --- Carregamento (Spark SQL) ---
    df_indicadores = spark.createDataFrame(linhas_resultado)
    df_indicadores.createOrReplaceTempView("indicadores_tmp")

    df_total = spark.sql(
        "SELECT SUM(faturamento) AS faturamento_total FROM indicadores_tmp"
    )
    df_total.createOrReplaceTempView("total_tmp")

    # Join (outra wide dependency) para calcular participacao percentual
    df_final = spark.sql("""
        SELECT
            i.produto_nome,
            i.total_pedidos,
            i.quantidade_itens,
            i.faturamento,
            ROUND(i.faturamento / t.faturamento_total * 100, 2) AS participacao_percentual
        FROM indicadores_tmp i
        CROSS JOIN total_tmp t
        ORDER BY i.faturamento DESC
    """)

    df_final.show(truncate=False)

    df_final.write.mode("overwrite").saveAsTable(TABELA_HIVE)

    print(f"Indicadores gravados na tabela Hive '{TABELA_HIVE}'.")

    spark.stop()


if __name__ == "__main__":
    main()