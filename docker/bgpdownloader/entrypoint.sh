#!/usr/bin/dumb-init /bin/bash

set -e

pushd /usr/src/app

prefix=$(jq -r '.prefix' config.json)
dump_type=$(jq -r '.dump_type' config.json)
project=$(jq -r '.project' config.json)
start_date=$(jq -r '.start_date' config.json)
end_date=$(jq -r '.end_date' config.json)

exec python3 bgpstream-downloader.py --prefixes "$prefix" --dump_type "$dump_type" --project "$project" --start-date "$start_date" --stop-date "$end_date"
