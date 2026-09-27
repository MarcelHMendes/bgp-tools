#!/usr/bin/dumb-init /bin/bash

set -e

pushd /usr/src/app

prefix_roa=$(jq -r '.prefix_roa' config.json)
prefix_no_roa=$(jq -r '.prefix_no_roa' config.json)
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

python3 bgpstream-downloader.py --prefixes "$prefix_roa" --dump_type "$dump_type" --project "$project" --start-date "$start_date" --stop-date "$end_date" $singlefile_flag --data-dir "$data_dir" --roa &
roa_pid=$!

python3 bgpstream-downloader.py --prefixes "$prefix_no_roa" --dump_type "$dump_type" --project "$project" --start-date "$start_date" --stop-date "$end_date" $singlefile_flag --data-dir "$data_dir" &
no_roa_pid=$!

status=0
if ! wait "$roa_pid"; then
  status=1
fi
if ! wait "$no_roa_pid"; then
  status=1
fi

exit "$status"
