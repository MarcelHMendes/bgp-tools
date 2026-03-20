#!/usr/bin/env python3

import argparse
import json
import os
import pathlib
import sys
import logging
import sqlite3
import ip2as
import lib
import radix

HIT = 0
MISS = 0

def routeviews_db():
    """Return routeviews mapping"""
    radix_tree = radix.Radix()
    ip2asn = ip2as.IP2ASRadix(radix_tree)
    current_path = pathlib.Path().absolute()
    local = ip2asn.download_latest_caida_pfx2as(current_path)
    ip2asn = ip2asn.from_caida_prefix2as(local)
    return ip2asn


def ip2asn_mapping(radixdb, traceroute_hops=None):
    """Map IP to ASN using routeviews and team cymru data"""
    hops = []
    for hop in traceroute_hops:
        result = hop.get("result", None)
        if result:
            ip_str = result[0].get("from", None)
        else:
            ip_str = None
        asn = resolve_asn(radixdb, ip_str)
        hops.append(asn)
    return hops


def resolve_asn(radixdb, ip_str=None):
    if not ip_str:
        return None

    # resolve private IPs
    if lib.is_private_ip(ip_str):
        asn = "private"
        return asn

    # resolve PEERING ips
    asn = lib.peering_resolver(ip_str)
    # resolve via routeviews.
    if not asn:
        asn = radixdb.get(ip_str)
        global HIT, MISS
        if asn:
            HIT += 1
        else:
            MISS += 1

    return asn


def remove_adjacent_duplicates(input_list):
    if not input_list:
        return []

    # Initialize a new list with the first element
    result = [input_list[0]]

    # Iterate through the input list starting from the second element
    for i in range(1, len(input_list)):
        # Check if the current element is different from the previous one
        if input_list[i] != input_list[i - 1]:
            result.append(input_list[i])

    return result


def remove_asterisk_from_adjacent_ases(input_list):
    if len(input_list) < 3:
        return input_list

    result = [input_list[0]]

    # Iterate through the input list starting from the second element
    for i in range(1, len(input_list) - 1):
        # check if the next and previous elements are the same
        if (
            input_list[i] == "*" or input_list[i] == "private" or input_list[i] == None
        ) and input_list[i - 1] == input_list[i + 1]:
            continue
        result.append(input_list[i])

    result.append(input_list[-1])

    return result


def evaluate_private_origin(input_list, origin):
    if len(input_list) < 2:
        return (input_list, origin)

    if origin != "private":
        return (input_list, origin)

    for iasn in range(0, len(input_list)):
        if input_list[iasn] == "private":
            continue
        if input_list[iasn] != None and input_list[iasn] != "*":
            origin = input_list[iasn]
            input_list = input_list[iasn:]
            return (input_list, origin)
        else:
            break
    return (input_list, origin)


def sanitize_path(asn_path, origin):
    """Remove private IPs, remove unresponsive hops and remove path prepending"""
    asn_path, origin = evaluate_private_origin(asn_path, origin)
    asn_path = remove_adjacent_duplicates(asn_path)
    asn_path = remove_asterisk_from_adjacent_ases(asn_path)
    asn_path = remove_adjacent_duplicates(asn_path)

    return (asn_path, origin)


def create_parser():
    desc = """Convert IP traceroutes to ASN traceroutes"""
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument(
        "--ripedir",
        dest="ripedir",
        action="store",
        required=True,
        help="Directory where ripe files are located",
    )
    parser.add_argument(
        "--db_file",
        dest="db_file",
        action="store",
        required=True,
        help="sqlite3 file db",
    )
    parser.add_argument(
        "--outdir",
        dest="outdir",
        action="store",
        default="parsed_traceroutes",
        required=False,
        help="Directory where the dump file will be located",
    )
    return parser


def main():
    lib.set_logging()

    parser = create_parser()
    opts = parser.parse_args()

    rv_ip2asn = routeviews_db()

    parsed_data = []
    for file in lib.get_ripe_files_list(opts.ripedir):
        fd = open(os.path.join(opts.ripedir, file), "r")
        data = json.load(fd)

        for traceroute in data:
            parsed_traceroute = {}
            parsed_traceroute["src_addr"] = traceroute.get("src_addr", "*")
            parsed_traceroute["dst_addr"] = traceroute.get("dst_addr", "*")
            parsed_traceroute["endtime"] = traceroute.get("endtime", "*")

            origin = resolve_asn(rv_ip2asn, ip_str=traceroute.get("src_addr", None))
            asn_path = ip2asn_mapping(
                rv_ip2asn, traceroute_hops=traceroute.get("result", None)
            )

            asn_path_sanitized, origin_sanitized = sanitize_path(asn_path, origin)

            parsed_traceroute["origin_asn"] = origin_sanitized
            parsed_traceroute["result"] = asn_path_sanitized

            parsed_data.append(parsed_traceroute)

    with open(opts.outdir, "w") as fd_out:
        json.dump(parsed_data, fd_out)

    print(f"HIT: {HIT}, MISS: {MISS}")

if __name__ == "__main__":
    sys.exit(main())
