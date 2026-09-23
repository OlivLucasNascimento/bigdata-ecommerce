
import json
import random
import time
import uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path

# Tipos de eventos do e-commerce
TIPOS_EVENTO = [
    "clique",
    "carrinho",
    "compra",
    "entrega"
]

# Produtos disponíveis
PRODUTOS = [
    {"id": 1, "nome": "Notebook", "preco": 3500.00},
    {"id": 2, "nome": "Smartphone", "preco": 1800.00},
    {"id": 3, "nome": "Mouse", "preco": 89.90},
    {"id": 4, "nome": "Teclado", "preco": 159.90},
    {"id": 5, "nome": "Monitor", "preco": 950.00}
]


def gerar_evento():
    tipo = random.choice(TIPOS_EVENTO)
    produto = random.choice(PRODUTOS)
    quantidade = random.randint(1, 3)

    
    # Define o horario atual
    horario_atual = datetime.now(timezone.utc)

    # Simula atraso em aproximadamente 30% dos eventos
    if random.random() < 0.30:
        atraso = random.randint(5, 15)
        timestamp_evento = horario_atual - timedelta(seconds=atraso)
    else:
        timestamp_evento = horario_atual


    evento = {
        "evento_id": str(uuid.uuid4()),
        "tipo_evento": tipo,
        "cliente_id": random.randint(1, 100),
        "produto_id": produto["id"],
        "produto_nome": produto["nome"],
        "quantidade": quantidade,
        "valor": round(produto["preco"] * quantidade, 2),
        "timestamp": timestamp_evento.isoformat()
    }

    if tipo == "entrega":
        evento["status_entrega"] = random.choice([
            "pendente",
            "em_transporte",
            "entregue"
        ])

    if tipo == "carrinho":
        evento["status_carrinho"] = random.choice([
            "ativo",
            "abandonado"
        ])

    return evento



if __name__ == "__main__":

    Path("dados").mkdir(parents=True, exist_ok=True)

    print("Iniciando gerador de eventos...")

    with open("dados/eventos.jsonl", "a", encoding="utf-8") as arquivo:

        while True:

            evento = gerar_evento()

            json_evento = json.dumps(
                evento,
                ensure_ascii=False
            )

            print(json_evento, flush=True)

            arquivo.write(json_evento + "\n")
            arquivo.flush()

            time.sleep(1)