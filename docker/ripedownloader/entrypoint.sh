#!/usr/bin/dumb-init /bin/bash
set -e

pushd /usr/source/app/

start_date=$(jq -r '.start_date' config.json)
end_date=$(jq -r '.end_date' config.json)
api_key=$(jq -r '.api_key' config.json)

exec python3 fetch-measurements.py --api-key "$api_key" --measurement-ids "./data/ids-file" --start-date "$start_date" --stop-date "$end_date" --outdir "./data/ripe-raw_$end_date/"
