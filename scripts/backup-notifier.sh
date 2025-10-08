#!/usr/bin/env bash
set -euo pipefail

################################
# CONFIG
################################
NOME_EMPRESA="Proxmox Matheus"
DEBIAN_API_URL="http://192.168.1.52:5000/api/backup"
PROXMOX_HOST="$(hostname -s)"

LOG_DIR="/var/log/pve/tasks"
STATE_FILE="/var/tmp/backup_notifier_last_timestamp.state"
CURL_TIMEOUT=15
USER_AGENT="backup-notifier/1.3-universal"

DEBUG="${DEBUG:-0}"
FORCE_RESCAN="${FORCE_RESCAN:-0}"
# ### CHANGE: opcional – limita varredura a N dias (0 = sem filtro)
MAX_AGE_DAYS="${MAX_AGE_DAYS:-0}"

################################
# FLAGS de linha de comando
################################
for arg in "${@:-}"; do
  case "$arg" in
    --rescan) FORCE_RESCAN=1 ;;
    --debug)  DEBUG=1 ;;
  esac
done

info(){ printf "[backup-notifier] %s\n" "$*" ; }
dbg(){ [[ "$DEBUG" -eq 1 ]] && printf "[debug] %s\n" "$*" || true; }

################################
# Estado
################################
LAST_TS=0
if [[ "$FORCE_RESCAN" -eq 1 ]]; then
  LAST_TS=0
else
  [[ -f "$STATE_FILE" ]] && LAST_TS="$(cat "$STATE_FILE" 2>/dev/null || echo 0)"
fi
# ### CHANGE: guardaremos o MAIOR ts visto nesta execução e só gravaremos no final
RUN_MAX_TS="$LAST_TS"

info "Empresa: $NOME_EMPRESA | Host: $PROXMOX_HOST"
info "Último timestamp processado: $LAST_TS"
[[ "$FORCE_RESCAN" -eq 1 ]] && info "# RESCAN habilitado."

################################
# VM map
################################
build_vm_map () {
  pvesh get /cluster/resources --output-format json 2>/dev/null \
  | jq -r '
    .[] | select(.type=="qemu" or .type=="lxc")
    | [(.vmid|tostring), .node, .type, (.tags // ""), (.name // "")]
    | @tsv
  ' || true
}

lookup_vm_line () {
  local vmid="$1"
  awk -v id="$vmid" -F'\t' '$1==id{print; exit;}' <<< "$VM_MAP"
}

fetch_tags_from_config () {
  local node="$1" vtype="$2" vmid="$3"
  local path
  case "$vtype" in
    qemu) path="/nodes/${node}/qemu/${vmid}/config" ;;
    lxc)  path="/nodes/${node}/lxc/${vmid}/config"  ;;
    *)    echo "" ; return 0 ;;
  esac
  pvesh get "$path" 2>/dev/null | jq -r '.tags // empty' || true
}

get_vm_display_name () {
  local vmid="$1"
  local line node vtype tags name
  line="$(lookup_vm_line "$vmid")" || true
  node="$(cut -f2 <<<"$line")"
  vtype="$(cut -f3 <<<"$line")"
  tags="$(cut -f4 <<<"$line")"
  name="$(cut -f5 <<<"$line")"

  if [[ -z "${tags:-}" || "$tags" == "n/a" || "$tags" == "null" ]]; then
    tags="$(fetch_tags_from_config "$node" "$vtype" "$vmid")"
  fi

  if [[ -n "${tags:-}" ]]; then
    echo "$tags"
  elif [[ -n "${name:-}" ]]; then
    echo "$name"
  else
    echo ""
  fi
}

################################
# Conversões
################################
to_bytes(){
  local size="$1" num unit
  num="$(awk '{print $1}' <<< "$size")"; unit="$(awk '{print $2}' <<< "$size")"
  num="${num//,/.}"
  case "$unit" in
    B|Bytes)   awk -v n="$num" 'BEGIN{printf "%.0f", n}' ;;
    KiB)       awk -v n="$num" 'BEGIN{printf "%.0f", n*1024}' ;;
    MiB)       awk -v n="$num" 'BEGIN{printf "%.0f", n*1024^2}' ;;
    GiB)       awk -v n="$num" 'BEGIN{printf "%.0f", n*1024^3}' ;;
    TiB)       awk -v n="$num" 'BEGIN{printf "%.0f", n*1024^4}' ;;
    KB)        awk -v n="$num" 'BEGIN{printf "%.0f", n*1000}' ;;
    MB)        awk -v n="$num" 'BEGIN{printf "%.0f", n*1000^2}' ;;
    GB)        awk -v n="$num" 'BEGIN{printf "%.0f", n*1000^3}' ;;
    TB)        awk -v n="$num" 'BEGIN{printf "%.0f", n*1000^4}' ;;
    K)         awk -v n="$num" 'BEGIN{printf "%.0f", n*1000}' ;;
    M)         awk -v n="$num" 'BEGIN{printf "%.0f", n*1000^2}' ;;
    G)         awk -v n="$num" 'BEGIN{printf "%.0f", n*1000^3}' ;;
    T)         awk -v n="$num" 'BEGIN{printf "%.0f", n*1000^4}' ;;
    *)         awk -v n="$num" 'BEGIN{printf "%.0f", n}' ;;
  esac
}

hms_to_seconds() {
  # aceita H:M:S ou M:S
  local h=0 m=0 s=0 IFS=:
  read -r a b c <<< "$1"
  if [[ -n "$c" ]]; then
    h="$a"; m="$b"; s="$c"
  else
    m="$a"; s="$b"
  fi
  awk -v hh="$h" -v mm="$m" -v ss="$s" 'BEGIN{print hh*3600 + mm*60 + ss}'
}

################################
# Extrações de log
################################
# --- SUBSTITUA A FUNÇÃO extract_times_from_block POR ESTA ---
extract_times_from_block() {
  local blk="$1"
  local start_fmt="" end_fmt="" line

  # tenta pegar "started" / "finished" / "failed"
  while IFS= read -r line; do
    case "$line" in
      ("INFO: Backup started at "*)
        start_fmt="${line#INFO: Backup started at }"
        ;;
      ("INFO: Backup finished at "*)
        end_fmt="${line#INFO: Backup finished at }"
        ;;
      ("INFO: Failed at "*)
        end_fmt="${line#INFO: Failed at }"
        ;;
    esac
  done <<< "$blk"

  local start_epoch end_epoch
  [[ -n "$start_fmt" ]] && start_epoch="$(LC_ALL=C date -d "$start_fmt" +%s 2>/dev/null || true)"
  [[ -n "$end_fmt"   ]] && end_epoch="$(LC_ALL=C date -d "$end_fmt"   +%s 2>/dev/null || true)"

  # fallback: se fim não veio, tenta a duração "(HH:MM:SS)" da linha "Finished Backup of ..."
  if [[ -z "${end_epoch:-}" || -z "$end_epoch" ]]; then
    local finish_line dur hms
    finish_line="$(grep -m1 -E '^INFO: Finished Backup of (VM|CT) ' <<<"$blk" || true)"
    if [[ -n "$finish_line" ]]; then
      hms="$(grep -oE '\(([0-9]+:)?[0-9]{1,2}:[0-9]{2}\)' <<<"$finish_line" | tr -d '()' || true)"
      if [[ -n "$hms" && -n "${start_epoch:-}" && "$start_epoch" -gt 0 ]]; then
        dur="$(hms_to_seconds "$hms")"
        end_epoch=$(( start_epoch + dur ))
      fi
    fi
  fi

  # fallback final
  if [[ -z "${start_epoch:-}" || -z "$start_epoch" ]]; then start_epoch=0; fi
  if [[ -z "${end_epoch:-}"   || -z "$end_epoch"   ]]; then end_epoch="$start_epoch"; fi
  if (( end_epoch < start_epoch )); then end_epoch="$start_epoch"; fi

  printf "%s %s\n" "$start_epoch" "$end_epoch"
}

extract_written_bytes() {
  local blk="$1"

  # 1) PBS (qemu/lxc para PBS): use SEMPRE o "transferred X <unit>"
  local t_qty t_unit
  t_qty="$(grep -Eo 'transferred[[:space:]]+[0-9.]+[[:space:]]+(KiB|MiB|GiB|TiB|KB|MB|GB|TB|Bytes)' <<<"$blk" | awk '{print $2}' | tail -n1 || true)"
  t_unit="$(grep -Eo 'transferred[[:space:]]+[0-9.]+[[:space:]]+(KiB|MiB|GiB|TiB|KB|MB|GB|TB|Bytes)' <<<"$blk" | awk '{print $3}' | tail -n1 || true)"
  if [[ -n "$t_qty" && -n "$t_unit" ]]; then
    to_bytes "$t_qty $t_unit"
    return
  fi

  # 2) vzdump para ARQUIVO (sem PBS): "archive file size: Z"
  local qty unit
  read -r qty unit < <(sed -nE 's/.*archive file size:[[:space:]]*([0-9.]+)([KMGTP]i?B|[KMGTP]B).*/\1 \2/p' <<<"$blk" | tail -n1)
  if [[ -n "${qty:-}" && -n "${unit:-}" ]]; then
    to_bytes "$qty $unit"
    return
  fi

  # 3) LXC pxar: "had to backup X ..."
  read -r qty unit < <(sed -nE 's/.*had to backup[[:space:]]+([0-9.]+)[[:space:]]+([KMGTP]i?B|[KMGTP]B).*/\1 \2/p' <<<"$blk" | head -n1)
  if [[ -n "${qty:-}" && -n "${unit:-}" ]]; then
    to_bytes "$qty $unit"
    return
  fi

  # fallback
  echo 0
}

extract_total_bytes() {
  local blk="$1" last num unit

  # QEMU: pega o último token do "include disk ... 450G"
  last="$(grep -E ' include disk' <<<"$blk" | awk '{print $NF}' | tail -n1 || true)"
  if [[ -n "$last" ]]; then
    case "$last" in
      *KiB) num="${last%KiB}"; unit="KiB" ;;
      *MiB) num="${last%MiB}"; unit="MiB" ;;
      *GiB) num="${last%GiB}"; unit="GiB" ;;
      *TiB) num="${last%TiB}"; unit="TiB" ;;
      *KB)  num="${last%KB}";  unit="KB"  ;;
      *MB)  num="${last%MB}";  unit="MB"  ;;
      *GB)  num="${last%GB}";  unit="GB"  ;;
      *TB)  num="${last%TB}";  unit="TB"  ;;
      *K)   num="${last%K}";   unit="KB"  ;;
      *M)   num="${last%M}";   unit="MB"  ;;
      *G)   num="${last%G}";   unit="GB"  ;;
      *T)   num="${last%T}";   unit="TB"  ;;
      *)    num=""; unit="" ;;
    esac
    [[ -n "$num" ]] && { to_bytes "$num $unit"; return; }
  fi

  # LXC: "had to backup 59.213 MiB of 1.28 GiB ..."
  # Pega o SEGUNDO par (1.28 GiB) = total
  read -r num unit < <(
    awk '
      match($0,/had to backup [0-9.]+ ([KMGT]i?B|[KMGT]B) of ([0-9.]+) ([KMGT]i?B|[KMGT]B)/,m){print m[2],m[3]; exit}
    ' <<<"$blk"
  )
  if [[ -n "${num:-}" && -n "${unit:-}" ]]; then
    to_bytes "$num $unit"
    return
  fi

  echo 0
}

extract_status() {
  local blk="$1"
  if grep -Eq '(^| )TASK ERROR|^ERROR: Backup of (VM|CT) [0-9]+ failed|backup write data failed|protocol canceled|^ERROR:' <<<"$blk"; then
    echo "ERROR"
  else
    echo "SUCCESS"
  fi
}

extract_storage() {
  local whole="$1" st
  st="$(grep -Eo -- '--storage[[:space:]]+[^ ]+' <<<"$whole" | awk '{print $2}' | tail -n1 || true)"
  [[ -n "$st" ]] && echo "$st" || echo "UNKNOWN"
}

extract_vm_name_from_block() {
  local blk="$1" name=""
  # QEMU
  name="$(grep -E '^INFO: VM Name:' <<<"$blk" | tail -n1 | sed -E 's/^INFO: VM Name:[[:space:]]*//')"
  if [[ -n "$name" ]]; then
    printf '%s\n' "$name"; return 0
  fi
  # LXC
  name="$(grep -E '^INFO: CT Name:' <<<"$blk" | tail -n1 | sed -E 's/^INFO: CT Name:[[:space:]]*//')"
  if [[ -n "$name" ]]; then
    printf '%s\n' "$name"; return 0
  fi
  printf '\n'
}

################################
# Envio do registro
################################
send_record () {
  local json="$1" tmp_body http_code
  tmp_body="$(mktemp)"
  http_code="$(
    curl -sS -m "$CURL_TIMEOUT" \
      -H "$USER_AGENT" \
      -H 'Content-Type: application/json' \
      -o "$tmp_body" \
      -w '%{http_code}' \
      -X POST "$DEBIAN_API_URL" \
      -d "$json" || echo "000"
  )"

  if [[ "$http_code" != "201" ]]; then
    dbg "ENVIO FALHOU (HTTP $http_code): $(cat "$tmp_body" || true)"
  else
    dbg "enviado (HTTP $http_code): $(cat "$tmp_body" || true)"
  fi
  rm -f "$tmp_body"
}

################################
# Recorte seguro do bloco da VM
################################
get_block_for_vmid() {
  local id="$1"
  printf "%s" "$RAW_CONTENT" | awk -v id="$id" '
    BEGIN { p=0 }
    /^INFO: Starting Backup of (VM|CT) [0-9]+/ {
      match($0, /^INFO: Starting Backup of (VM|CT) ([0-9]+)/, m)
      vm = m[2]
      if (p && vm != id) { exit }
      if (vm == id) { p=1; print; next }
    }
    p {
      print
      if ($0 ~ ("^INFO: Finished Backup of (VM|CT) " id) || \
          $0 ~ ("^ERROR: Backup of (VM|CT) " id " failed")) { exit }
    }
  '
}

################################
# Processa logs
################################
process_log_file(){
  local file="$1"
  local ts; ts="$(stat -c %Y "$file" 2>/dev/null || stat -f %m "$file")"
  if [[ "$FORCE_RESCAN" -eq 0 && "$ts" -le "$LAST_TS" ]]; then
    dbg "pular file (ts<=LAST_TS=$LAST_TS)"
    return
  fi

  dbg "lendo $file (ts=$ts)"
  RAW_CONTENT="$(cat "$file")"
  local STORAGE; STORAGE="$(extract_storage "$RAW_CONTENT")"

  mapfile -t VMIDS < <(
    grep -Eo 'Starting Backup of (VM|CT) [0-9]+' <<<"$RAW_CONTENT" \
    | awk '{print $NF}' | sort -u
  )

  for VMID in "${VMIDS[@]:-}"; do
    local BLOCK; BLOCK="$(get_block_for_vmid "$VMID")"
    [[ -z "$BLOCK" ]] && continue

    local VM_NAME; VM_NAME="$(get_vm_display_name "$VMID")"
       if [[ -z "$VM_NAME" || "$VM_NAME" == "$VMID" ]]; then
 	 n_from_log="$(extract_vm_name_from_block "$BLOCK")"
  	[[ -n "$n_from_log" ]] && VM_NAME="$n_from_log"
       fi
    local STATUS;    STATUS="$(extract_status "$BLOCK")"
    read -r START_EPOCH END_EPOCH <<<"$(extract_times_from_block "$BLOCK")"

    local WRITTEN TOTAL
    WRITTEN="$(extract_written_bytes "$BLOCK")"
    TOTAL="$(extract_total_bytes   "$BLOCK")"

    JSON="$(jq -n \
      --arg proxmox_host "$PROXMOX_HOST" \
      --arg company_name "$NOME_EMPRESA" \
      --arg vmid "$VMID" \
      --arg vm_name "$VM_NAME" \
      --arg status "$STATUS" \
      --arg storage_target "$STORAGE" \
      --argjson start_time "$START_EPOCH" \
      --argjson end_time   "$END_EPOCH" \
      --argjson total_size_bytes "$TOTAL" \
      --argjson written_size_bytes "$WRITTEN" \
      '{
        proxmox_host: $proxmox_host,
        company_name: $company_name,
        vmid: $vmid,
        vm_name: $vm_name,
        status: $status,
        storage_target: $storage_target,
        start_time: $start_time,
        end_time: $end_time,
        total_size_bytes: $total_size_bytes,
        written_size_bytes: $written_size_bytes
      }'
    )"

    dbg "POST -> $DEBIAN_API_URL : $(echo "$JSON" | jq -c '.')"
    send_record "$JSON"
  done

  # ### CHANGE: atualiza apenas o MAIOR timestamp visto
  if (( ts > RUN_MAX_TS )); then
    RUN_MAX_TS="$ts"
  fi
}

################################
# MAIN
################################
VM_MAP="$(build_vm_map || true)"

# monta expressão -mtime se MAX_AGE_DAYS vier setado (ex.: 1 = últimos 1 dia)
AGE_EXPR=()
if [[ -n "${MAX_AGE_DAYS:-}" && "$MAX_AGE_DAYS" =~ ^[0-9]+$ ]]; then
  AGE_EXPR=(-mtime "-$MAX_AGE_DAYS")
fi

find "$LOG_DIR" -type f \( -name '*vzdump*' -o -name '*backup*' \) "${AGE_EXPR[@]}" 2>/dev/null \
| while read -r LOG_FILE; do
    process_log_file "$LOG_FILE"
  done

# ### CHANGE: grava o estado uma vez, com o maior TS da execução
echo "$RUN_MAX_TS" > "$STATE_FILE"

exit 0
