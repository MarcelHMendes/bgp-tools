#!/usr/bin/env python3

import argparse
import ipaddress
import json
import logging
import os
import sys

import lib


def dump_traceroute_ips(fd, traceroute_hops):
    for hop in traceroute_hops:
        if "error" in hop:
            continue
        result = hop.get("result", None)
        if not result:
            continue
        ip_str = result[0].get("from", None)
        if not ip_str:
            continue
        ip_addr = ipaddress.IPv4Address(ip_str)
        ip_net = ipaddress.ip_network(ip_addr)

        fd.write(str(ip_net.network_address))
        fd.write("\n")


def create_parser():
    desc = """Dump IPs list"""
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument(
        "--ripedir",
        dest="ripedir",
        action="store",
        required=True,
        help="Directory where ripe files are located",
    )
    parser.add_argument(
        "--outdir",
        dest="outdir",
        action="store",
        default="ips_list",
        required=False,
        help="Directory where the dump file will be located",
    )
    return parser


def main():
    lib.set_logging()

    parser = create_parser()
    opts = parser.parse_args()
    fd_out = open(opts.outdir, "a")

    for file in lib.get_ripe_files_list(opts.ripedir):
        fd = open(os.path.join(opts.ripedir, file), "r")
        data = json.load(fd)
        if not isinstance(data, list):
            print(f"File {file} is not a list, skipping")
            continue
        for traceroute in data:
            if isinstance(traceroute, str) or isinstance(traceroute, list):
                print(file)
                break
            dump_traceroute_ips(fd_out, traceroute.get("result", []))


if __name__ == "__main__":
    sys.exit(main())
