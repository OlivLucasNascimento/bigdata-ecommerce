import json
from datetime import datetime

from pyflink.common import Duration, WatermarkStrategy
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.file_system import FileSource, StreamFormat
from pyflink.datastream.window import SlidingEventTimeWindows
from pyflink.common.time import Time
from pyflink.datastream.functions import (
    MapFunction,
    ProcessWindowFunction,
    RuntimeContext,
)

CAMINHO_EVENTOS = "/app/dados/eventos.jsonl"
LIMIAR_ALERTA = 2  # a partir de quantos carrinhos abandonados do mesmo produto gera alerta


# Extrai o event time (timestamp do próprio evento, não o horário de chegada)
class ExtratorTimestamp:
    def extract_timestamp(self, evento, record_timestamp):
        return int(datetime.fromisoformat(evento["timestamp"]).timestamp() * 1000)


def parse_evento(linha: str):
    try:
        return json.loads(linha)
    except json.JSONDecodeError:
        return None


class ContarCarrinhosPorJanela(ProcessWindowFunction):
    def process(self, produto_nome, context, eventos):
        eventos = list(eventos)
        total = len(eventos)
        if total >= LIMIAR_ALERTA:
            inicio = context.window().start
            fim = context.window().end
            alerta = {
                "produto_nome": produto_nome,
                "total_abandonados": total,
                "janela_inicio": inicio,
                "janela_fim": fim,
            }
            yield json.dumps(alerta)


class GravarAlertaHBase(MapFunction):
    def open(self, runtime_context: RuntimeContext):
        import happybase

        self.conexao = happybase.Connection(host="hbase", port=9090)
        self.conexao.open()
        if b"alertas_vendas" not in self.conexao.tables():
            self.conexao.create_table(
                "alertas_vendas", {"info": dict()}
            )
        self.tabela = self.conexao.table("alertas_vendas")

    def map(self, alerta_json: str):
        alerta = json.loads(alerta_json)
        row_key = f"{alerta['produto_nome']}_{alerta['janela_fim']}".encode()
        self.tabela.put(
            row_key,
            {
                b"info:produto": alerta["produto_nome"].encode(),
                b"info:total_abandonados": str(alerta["total_abandonados"]).encode(),
                b"info:janela_inicio": str(alerta["janela_inicio"]).encode(),
                b"info:janela_fim": str(alerta["janela_fim"]).encode(),
            },
        )
        print(f"[ALERTA GRAVADO] {alerta_json}")
        return alerta_json

    def close(self):
        self.conexao.close()


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    source = FileSource.for_record_stream_format(
        StreamFormat.text_line_format(), CAMINHO_EVENTOS
    ).build()

    linhas = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "leitura-eventos-json",
    )

    eventos = (
        linhas.map(parse_evento, output_type=Types.PICKLED_BYTE_ARRAY())
        .filter(
            lambda e: e is not None
            and e.get("tipo_evento") == "carrinho"
            and e.get("status_carrinho") == "abandonado"
        )
    )

    eventos_com_watermark = eventos.assign_timestamps_and_watermarks(
        WatermarkStrategy.for_bounded_out_of_orderness(
            Duration.of_seconds(10)
        ).with_timestamp_assigner(ExtratorTimestamp())
    )

    alertas = (
        eventos_com_watermark.key_by(lambda e: e["produto_nome"])
        .window(SlidingEventTimeWindows.of(Time.minutes(1), Time.seconds(30)))
        .process(ContarCarrinhosPorJanela(), output_type=Types.STRING())
    )

    alertas.map(GravarAlertaHBase(), output_type=Types.STRING()).print()

    env.execute("Job Flink - Alertas de Carrinho Abandonado")


if __name__ == "__main__":
    main()