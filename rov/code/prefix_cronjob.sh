#!/usr/bin/env bash

set -euo pipefail

LOG_FILE="${LOG_FILE:-/home/mmendes/bgp-tools/rov/code/cron_reports/prefix_cronjob.log}"
ROUTING_STATUS_URL="${ROUTING_STATUS_URL:-https://stat.ripe.net/data/routing-status/data.json}"
PREFIXES=(
	"${PREFIX_1:-138.185.229.0/24}"
	"${PREFIX_2:-138.185.230.0/24}"
	"${PREFIX_3:-138.185.228.0/24}"
	"${PREFIX_4:-204.9.170.0/24}"
    "${PREFIX_5:-138.185.231.0/24}"
)

mkdir -p "$(dirname "$LOG_FILE")"

{
	echo "--------------"
	echo "cronjob executado em $(date '+%Y-%m-%d %H:%M:%S')"
	echo "Consultando RIPE Stat em ${ROUTING_STATUS_URL}"

	for resource in "${PREFIXES[@]}"; do
		response=$(curl -fsS --get \
			--data-urlencode "resource=${resource}" \
			"${ROUTING_STATUS_URL}")

		python3 - "$resource" "$response" <<'PY'
import json
import sys

resource = sys.argv[1]
data = json.loads(sys.argv[2])
lastseen = data.get("data", {}).get("last_seen", {}).get("time")

if lastseen:
	print(f"{resource} -> lastseen: {lastseen}")
else:
	print(f"{resource} -> lastseen não encontrado")
PY
	done
} | tee -a "$LOG_FILE"
