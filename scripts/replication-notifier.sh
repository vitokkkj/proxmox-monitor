#!/usr/bin/env bash
set -euo pipefail

export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
export LANG=C

# ==== CONFIGURE AQUI ====
COMPANY_NAME="Proxmox Paulo Weber"        # nome que aparece no dashboard
API_URL="http://177.39.36.12:5000/api/replication"
PROXMOX_HOST="$(hostname -s)"               # ex.: master
CURL_TIMEOUT=12
USER_AGENT="replication-notifier/1.1"
# ========================

have() { command -v "$1" >/dev/null 2>&1; }

if ! have pvesr || ! have jq || ! have curl; then
  echo "ERRO: preciso de pvesr, jq e curl" >&2
  exit 1
fi

# Saída do pvesr status tem cabeçalho e colunas:
# JobID  Enabled  Target           LastSync              NextSync  Duration  FailCount  State
# 100-0  Yes      local/slave      2025-10-01_14:30:01  ...       11.16     0          OK
pvesr status | awk 'NR>1 && NF>=8 {print}' | while read -r JOB ENABLED TARGET LASTSYNC NEXTSYNC DURATION FAILCOUNT STATE REST; do
  # vmid = parte antes do hífen
  VMID="${JOB%%-*}"

  # target_node = depois da barra em "local/slave"
  TARGET_NODE="${TARGET#*/}"

  # source_node = este host
  SOURCE_NODE="$PROXMOX_HOST"

  # last_sync epoch (ou 0 se "-")
  if [[ "$LASTSYNC" == "-" ]]; then
    LAST_EPOCH=0
  else
    # formato: YYYY-MM-DD_HH:MM:SS
    # GNU date entende "YYYY-MM-DD HH:MM:SS"
    LAST_EPOCH=$(date -d "${LASTSYNC//_/ }" +%s 2>/dev/null || echo 0)
  fi

  # duração em segundos (coluna vem em segundos com decimais normalmente)
  # normalize para inteiro
  DUR_SEC=$(printf "%.0f\n" "${DURATION:-0}" 2>/dev/null || echo 0)

  # status consolidado p/ dashboard
  case "${STATE^^}" in
    OK|READY|SYNCED) STATUS="SUCCESS" ;;
    *)               STATUS="ERROR" ;;
  esac

  # vm_name opcional
  VM_NAME="$(qm config "$VMID" 2>/dev/null | awk -F': ' '/^name:/ {print $2; exit}')"

  # schedule não vem do pvesr status; se quiser, puxa da API de jobs:
  # pvesh get /cluster/replication | jq ...
  # para manter simples, vamos enviar vazio:
  SCHEDULE=""

  # monta JSON e envia
  JSON=$(/usr/bin/jq -n \
    --arg host "$PROXMOX_HOST" \
    --arg company "$COMPANY_NAME" \
    --arg vmid "$VMID" \
    --arg vm_name "${VM_NAME:-}" \
    --arg source "$SOURCE_NODE" \
    --arg target "$TARGET_NODE" \
    --arg state "$STATE" \
    --arg status "$STATUS" \
    --arg schedule "$SCHEDULE" \
    --argjson last_sync "$LAST_EPOCH" \
    --argjson dur "$DUR_SEC" \
    --argjson fail "${FAILCOUNT:-0}" \
    '{
      proxmox_host: $host,
      company_name: $company,
      vmid: $vmid,
      vm_name: $vm_name,
      source_node: $source,
      target_node: $target,
      state: $state,
      status: $status,
      schedule: $schedule,
      last_sync: $last_sync,
      duration_sec: $dur,
      fail_count: $fail
    }')

  CODE=$(curl -sS -m "$CURL_TIMEOUT" \
    -H "User-Agent: $USER_AGENT" \
    -H "Content-Type: application/json" \
    -o /dev/null -w '%{http_code}' \
    -X POST "$API_URL" \
    -d "$JSON" || true)

  if [[ "$CODE" != "201" && "$CODE" != "200" ]]; then
    echo "WARN: envio replication job=$JOB HTTP $CODE" >&2
  fi
done
