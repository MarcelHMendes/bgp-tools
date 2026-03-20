#!/bin/bash
set -eu


# cleanup before exit
trap 'rm -f ips_file ips_file_sorted ips_mapped ips_mapped_sorted ips_mapped.csv ip_asn_mapping' EXIT

function remove_duplicates_addr () {
    awk -F ',' -v col="2" '
    BEGIN {
        getline;  # Read the header line and print it
        print;
    }
    {
        if (!($col in seen)) {
            seen[$col] = 1;
            print;
        }
    }
'
}
DATE=$(jq -r '.end_date' config.json)

# Create db
python3 db.py

python3 traceroutes_ip2file.py --ripedir ./ripedata/data/ripe-raw_"$DATE"/ --outdir ips_file

# remove duplicates and sort IPs
sort -u ips_file > ips_file_sorted

# AS lookup
netcat whois.cymru.com 43 < ips_file_sorted | sort -n > ips_mapped

# remove duplicates and sort mapped ips
sort -u ips_mapped > ips_mapped_sorted

# remove spaces, add comma separation and remove duplicate mapping
sed 's/ //g; s/|/,/g'  ips_mapped_sorted  | cut -d',' -f1-3 | remove_duplicates_addr > ips_mapped.csv

python3 process-ripe-mesurements.py --ripedir ./ripedata/data/ripe-raw_"$DATE"/ --db_file cymru.db --outdir ./data/measurements_"$DATE".json

#python3 identify_rov_enforcement.py --measurements_file  "/var/monitor/data/ripe-compiled/measurements_$DATE.json"
