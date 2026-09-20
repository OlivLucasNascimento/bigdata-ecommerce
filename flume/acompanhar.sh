
#!/bin/sh

ARQUIVO="/app/dados/eventos.jsonl"

# Começa a acompanhar somente os novos eventos
if [ -f "$ARQUIVO" ]; then
    ULTIMA_LINHA=$(wc -l < "$ARQUIVO")
else
    ULTIMA_LINHA=0
fi

while true; do

    if [ -f "$ARQUIVO" ]; then

        TOTAL=$(wc -l < "$ARQUIVO")

        # Trata eventual reinicialização do arquivo
        if [ "$TOTAL" -lt "$ULTIMA_LINHA" ]; then
            ULTIMA_LINHA=0
        fi

        # Envia somente os novos eventos
        if [ "$TOTAL" -gt "$ULTIMA_LINHA" ]; then

            INICIO=$((ULTIMA_LINHA + 1))

            sed -n "${INICIO},${TOTAL}p" "$ARQUIVO"

            ULTIMA_LINHA=$TOTAL

        fi

    fi

    sleep 1

done
