#!/usr/bin/env python3

import argparse
import datetime
import json
import logging
import os
import pathlib
import sys
from multiprocessing import Pool

from metalib import MeasurementInfo
from ripe.atlas import cousteau


def create_parser():
    desc = """Fetch measurement results"""
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument(
        "--api-key",
        dest="api_key",
        action="store",
        metavar="KEY",
        type=str,
        required=True,
        help="RIPE Atlas API key to use",
    )
    measurement_spec = parser.add_mutually_exclusive_group(required=True)
    measurement_spec.add_argument(
        "--measurement-ids",
        dest="measurement_ids_fn",
        action="store",
        metavar="FILE",
        type=pathlib.Path,
        required=False,
        default=None,
        help="File with list of measurement IDs to fetch",
    )
    measurement_spec.add_argument(
        "--measurement-infos",
        dest="measurement_infos_fn",
        action="store",
        metavar="FILE",
        type=pathlib.Path,
        required=False,
        default=None,
        help="File with measurement information (created by create-measurements.py)",
    )
    parser.add_argument(
        "--outdir",
        dest="output_dir",
        action="store",
        metavar="DIR",
        type=pathlib.Path,
        required=True,
        help="Directory where to store output JSON files, one per measurement ID",
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
    return parser


def load_measurement_ids(opts) -> list[int]:
    if opts.measurement_ids_fn is not None:
        return list(int(l.strip()) for l in open(opts.measurement_ids_fn, "r"))
    if opts.measurement_infos_fn is not None:
        return list(
            m.measurement_id
            for m in MeasurementInfo.load_from_file(opts.measurement_infos_fn)
        )
    raise RuntimeError("Failed to load measurement IDs")


def process_atlas_request(args) -> None:
    request, mid, opts = args
    success, response = request.create()
    if not success:
        logging.error("Error downloading result for measurement %d", mid)
        logging.error("%r", response)
        return

    with open(opts.output_dir / f"{mid}.json", "w") as outfd:
        results = list(response)
        outfd.write(json.dumps(results))


def main():
    # resource.setrlimit(resource.RLIMIT_AS, (1 << 32, 1 << 32))
    logging.basicConfig(format="%(message)s", level=logging.DEBUG)

    parser = create_parser()
    opts = parser.parse_args()

    measurement_ids = load_measurement_ids(opts)
    os.makedirs(opts.output_dir, exist_ok=True)

    global_kwargs = dict()
    if opts.start_date is not None:
        global_kwargs["start"] = opts.start_date
    if opts.stop_date is not None:
        global_kwargs["stop"] = opts.stop_date

    args_list = []
    for mid in measurement_ids:
        kwargs = dict(global_kwargs)
        kwargs["msm_id"] = mid

        request = cousteau.AtlasResultsRequest(**kwargs)
        args_list.append((request, mid, opts))

    pool = Pool(processes=8)
    results_iterator = list(pool.imap(process_atlas_request, args_list))

    pool.close()
    pool.join()


if __name__ == "__main__":
    sys.exit(main())
