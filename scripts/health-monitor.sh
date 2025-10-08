#!/bin/bash

# --- CONFIGURAÇÃO DO CLIENTE ---
NOME_EMPRESA="Proxmox Matheus"

# --- Configuração Técnica ---
DEBIAN_API_URL="http://192.168.1.52:5000/api/health"   # <- ROTA CORRETA
PROXMOX_HOST=$(hostname -s)

# Verifica se zpool existe
if ! command -v zpool &> /dev/null; then
    exit 0
fi

# Lê "zpool status" (compatível com ZFS antigo)
# Gera pares: "<pool> <state>"
PARSED_LIST=$(zpool status | awk '/^  pool:/ {pool_name=$2} /^ state:/ {if (pool_name) {print pool_name, $2; pool_name=""}}')

# Nada pra enviar?
[ -z "$PARSED_LIST" ] && exit 0

# Monta o JSON no formato aceito pelo backend
PAYLOAD=$(echo "$PARSED_LIST" | awk -v hn="$PROXMOX_HOST" -v cn="$NOME_EMPRESA" '
BEGIN {
    printf "{\"company_name\": \"%s\", \"proxmox_host\": \"%s\", \"pools\": [" , cn, hn
    first=1
}
{
    if (!first) printf ","
    printf "{\"name\": \"%s\", \"status\": \"%s\"}", $1, $2
    first=0
}
END {
    print "]}"
}
')

# Envia para a API
curl -s -o /dev/null -X POST \
     -H "Content-Type: application/json" \
     --data-binary "$PAYLOAD" \
     "$DEBIAN_API_URL"
