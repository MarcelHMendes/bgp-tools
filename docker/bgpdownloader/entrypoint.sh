#!/usr/bin/dumb-init /bin/bash

set -e

pushd /usr/src/app

prefix=$(jq -r '.prefix' config.json)
dump_type=$(jq -r '.dump_type' config.json)
project=$(jq -r '.project' config.json)
start_date=$(jq -r '.start_date' config.json)
end_date=$(jq -r '.end_date' config.json)

singlefile=$(jq -r '.bgpdownload_info.singlefile // false' config.json)
data_dir=$(jq -r '.bgpdownload_info.data_dir // "data"' config.json)

singlefile_flag=""
if [ "$singlefile" = "true" ]; then
  singlefile_flag="--singlefile"
  python3 routeviews_updates_downloader.py --start-date "$start_date" --stop-date "$end_date" --data-dir "$data_dir"
fi

exec python3 bgpstream-downloader.py --prefixes "$prefix" --dump_type "$dump_type" --project "$project" --start-date "$start_date" --stop-date "$end_date" $singlefile_flag --data-dir "$data_dir"
