#!/bin/sh

ARQUIVO=/app/dados/eventos.jsonl
TOTAL=$(wc -l < "$ARQUIVO")
INICIO=$((TOTAL - 2))

echo "Mostrando as ultimas 3 linhas (de $INICIO a $TOTAL):"
sed -n "${INICIO},${TOTAL}p" "$ARQUIVO"
echo "FIM DO TESTE"