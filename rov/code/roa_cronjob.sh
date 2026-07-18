#!/usr/bin/env bash

set -euo pipefail

LOG_FILE="${LOG_FILE:-/home/mmendes/bgp-tools/rov/code/cron_reports/roa_cronjob.log}"
VALIDITY_URL="${VALIDITY_URL:-https://rpki-validator.ripe.net/validity}"
ASN="61574"
PREFIXES=(
	"138.185.229.0/24"
	"138.185.230.0/24"
)

mkdir -p "$(dirname "$LOG_FILE")"

{
	echo "--------------"
	echo "cronjob executado em $(date '+%Y-%m-%d %H:%M:%S')"
	echo "Consultando validador RPKI em ${VALIDITY_URL}"

	for prefix in "${PREFIXES[@]}"; do
		response=$(curl -fsS --get \
			--data-urlencode "asn=${ASN}" \
			--data-urlencode "prefix=${prefix}" \
			"${VALIDITY_URL}")

		python3 - "$prefix" "$response" <<'PY'
import json
import sys

prefix = sys.argv[1]
data = json.loads(sys.argv[2])
route = data.get("validated_route", {})
validity = route.get("validity", {})
state = validity.get("state", "unknown")
matched = validity.get("VRPs", {}).get("matched", [])

if state == "valid" and matched:
	print(f"{prefix} -> VALID: existe ROA para AS61574")
else:
	print(f"{prefix} -> {state.upper()}: não encontrei ROA para AS61574")
PY
	done
} | tee -a "$LOG_FILE"
