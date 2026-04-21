#!/usr/bin/env python3
# coding: utf-8

import argparse
import ipaddress
import json
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from multiprocessing import Pool
from typing import List, Optional, Union

import dateutil
from lib import *


@dataclass
class BGPDump:
    start_time: str
    end_time: str
    dump_type: str
    prefixes: List[ipaddress.IPv4Network]
    project: Optional[str] = None
    collectors: Optional[List[str]] = None
    singlefile: bool = False
    data_dir: str = "data"

    def __post_init__(self):
        self._validate_prefixes()
        self._validate_times()
        self._validate_dump_type()
        self._validate_collectors()

    def _validate_prefixes(self):
        for pfx in self.prefixes:
            prefix = ipaddress.IPv4Network(pfx)

    def _validate_times(self):
        try:
            dateutil.parser.parse(self.start_time)
            dateutil.parser.parse(self.end_time)
        except ValueError:
            sys.exit("Incorrect date values. Must be in UTC datetime string format")

    def _validate_dump_type(self):
        if self.dump_type != "updates" and self.dump_type != "ribs":
            sys.exit("Incorrect record type. Must be 'updates' or 'ribs'")

    def _validate_collectors(self):
        if not self.collectors:
            if self.project == "ripe_ris":
                self.collectors = RIPE_RIS
            elif self.project == "route_views":
                self.collectors = ROUTE_VIEWS
            elif self.project == "ris+views":
                self.collectors = RIPE_RIS + ROUTE_VIEWS

        for collector in self.collectors:
            if collector not in ROUTE_VIEWS and collector not in RIPE_RIS:
                sys.exit("The collector's choice doesn't match any project")

    def download_bgpdump(self):
        stream = download_bgpstream(
            start_time=self.start_time,
            end_time=self.end_time,
            record_type=self.dump_type,
            prefixes=self.prefixes,
            collectors=self.collectors,
            singlefile=self.singlefile,
            data_dir=self.data_dir,
        )

        return stream


def create_parser():
    desc = """Download BGPData"""
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument(
        "--prefixes",
        nargs="+",
        dest="prefixes",
        action="store",
        required=True,
        help="List of prefixes to be filtered",
    )
    parser.add_argument(
        "--project",
        dest="project",
        type=str,
        action="store",
        default=None,
        required=False,
        help="Name of the project (route_views, ripe_ris, ris+views)",
    )
    parser.add_argument(
        "--start-date",
        dest="start_date",
        action="store",
        metavar="ISO",
        type=datetime.date.fromisoformat,
        required=True,
        default=None,
        help="Start time of measurements",
    )
    parser.add_argument(
        "--stop-date",
        dest="stop_date",
        action="store",
        metavar="ISO",
        type=datetime.date.fromisoformat,
        required=True,
        default=None,
        help="Stop time of measurements",
    )
    parser.add_argument(
        "--dump_type",
        dest="dump_type",
        type=str,
        action="store",
        default=None,
        required=True,
        help="Type of dump (ribs or updates)",
    )
    parser.add_argument(
        "--collectors",
        nargs="+",
        dest="collectors",
        action="store",
        required=False,
        default=None,
        help="Collectors to be used",
    )
    parser.add_argument(
        "--singlefile",
        action="store_true",
        help="Use singlefile interface to read from local files instead of broker",
    )
    parser.add_argument(
        "--data-dir",
        dest="data_dir",
        type=str,
        action="store",
        default="data",
        help="Directory containing local data files (used with --singlefile)",
    )
    return parser


def process_bgpdump(bgp_dump):
    try:
        return bgp_dump.download_bgpdump()
    except RuntimeError as e:
        if "Could not get next record" in str(e):
            print(str(e))
            return []




def generate_times_list(opts):
    # Generate a list of datetime objects for each day
    date_list = []
    current_date = opts.start_date
    while current_date <= opts.stop_date:
        dt = datetime.datetime.combine(current_date, datetime.time(0, 0, 0))
        date_list.append(dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ"))
        current_date += timedelta(days=1)

    return date_list

def sort_records_by_timestamp(records):
    try:
        return sorted(records, key=lambda x: x["timestamp"])
    except KeyError:
        raise ValueError("Algum registro não possui o campo 'timestamp'")

def main():
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    root.addHandler(handler)

    parser = create_parser()
    opts = parser.parse_args()

    TIMES = generate_times_list(opts)

    bgp_dumps = []
    for ti in range(0, len(TIMES) - 1):
        bgp_dumps.append(
            BGPDump(
                start_time=TIMES[ti],
                end_time=TIMES[ti + 1],
                dump_type=opts.dump_type,
                prefixes=opts.prefixes,
                project=opts.project,
                collectors=opts.collectors,
                singlefile=opts.singlefile,
                data_dir=opts.data_dir,
            )
        )

    pool = Pool(processes=8)
    results_iterator = list(pool.imap(process_bgpdump, bgp_dumps))

    pool.close()
    pool.join()

    stream_list = []

    for stream in results_iterator:
        stream_list.extend(stream)

    # Sort records before save
    stream_list = sort_records_by_timestamp(stream_list)

    file_name = f"bgpdump_{TIMES[0]}_{TIMES[-1]}_{opts.dump_type}_{opts.project}.json"
    base_path = "/usr/src/app/data"

    with open(os.path.join(base_path, file_name), "w", encoding="utf-8") as fd:
        json.dump(stream_list, fd, ensure_ascii=False, indent=4)


if __name__ == "__main__":
    sys.exit(main())
