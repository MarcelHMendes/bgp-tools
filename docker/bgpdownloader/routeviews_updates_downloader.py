#!/usr/bin/env python3
# coding: utf-8

"""RouteViews UPDATES downloader.

This script downloads RouteViews UPDATES .bz2 files for a given interval.
Files are stored under <data_dir>/<collector>/<YYYY-MM-DD>/.
"""

import argparse
import datetime
import os
import re
import subprocess
import sys
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin
from urllib.request import urlopen

from lib import ROUTE_VIEWS

LISTING_RE = re.compile(r'href="([^"]+\.bz2)"')
FILE_RE = re.compile(r'updates\.(\d{8})\.(\d{4})\.bz2$')

BASE_URL = 'https://archive.routeviews.org/'


def parse_args():
    parser = argparse.ArgumentParser(
        description='Download RouteViews UPDATES bz2 files into a local collector/day tree.'
    )
    parser.add_argument(
        '--start-date',
        required=True,
        type=datetime.date.fromisoformat,
        help='Start date in ISO format (YYYY-MM-DD).',
    )
    parser.add_argument(
        '--stop-date',
        required=True,
        type=datetime.date.fromisoformat,
        help='Stop date in ISO format (YYYY-MM-DD).',
    )
    parser.add_argument(
        '--data-dir',
        default='data',
        help='Destination directory for downloaded files.',
    )
    parser.add_argument(
        '--collectors',
        nargs='+',
        default=None,
        help='Optional list of collectors to download. Defaults to ROUTE_VIEWS from lib.py.',
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Print download plan without fetching files.',
    )
    return parser.parse_args()


def month_list(start_date, stop_date):
    months = []
    current = datetime.date(start_date.year, start_date.month, 1)
    end_month = datetime.date(stop_date.year, stop_date.month, 1)
    while current <= end_month:
        months.append(current)
        year = current.year + (current.month // 12)
        month = current.month % 12 + 1
        current = datetime.date(year, month, 1)
    return months


def normalize_collector(name):
    return name.strip()


def list_update_files(url):
    try:
        result = subprocess.run(['wget', '--inet6', '--quiet', '--show-progress', '--read-timeout=600', '--output-document=-', url],
                                capture_output=True, text=True, timeout=600)
        if result.returncode != 0:
            raise RuntimeError(f'wget failed for {url}: {result.stderr}')
        html = result.stdout
    except subprocess.TimeoutExpired:
        raise RuntimeError(f'Timeout listing {url}')
    except FileNotFoundError:
        raise RuntimeError(f'wget not found')

    files = LISTING_RE.findall(html)
    return sorted(set(files))


def parse_file_date(filename):
    match = FILE_RE.search(filename)
    if not match:
        return None
    return datetime.date.fromisoformat(match.group(1)[:4] + '-' + match.group(1)[4:6] + '-' + match.group(1)[6:8])


def download_file(source_url, destination_path):
    os.makedirs(os.path.dirname(destination_path), exist_ok=True)
    if os.path.exists(destination_path):
        print(f'Skipping existing file: {destination_path}')
        return

    print(f'Downloading {source_url} -> {destination_path}')
    try:
        result = subprocess.run(['wget', '--inet6', '--quiet', '--read-timeout=600', '--output-document', destination_path, source_url],
                                timeout=600)
        if result.returncode != 0:
            raise RuntimeError(f'wget failed for {source_url}')
    except subprocess.TimeoutExpired:
        raise RuntimeError(f'Timeout downloading {source_url}')


def main():
    args = parse_args()
    if args.stop_date < args.start_date:
        raise SystemExit('stop-date must be equal or later than start-date')

    collectors = [normalize_collector(c) for c in (args.collectors or ROUTE_VIEWS)]
    months = month_list(args.start_date, args.stop_date)

    for collector in collectors:
        downloaded_any = False
        for month in months:
            month_path = f'{month.year}.{month.month:02d}'
            url = urljoin(BASE_URL, f'{collector}/bgpdata/{month_path}/UPDATES/')
            try:
                files = list_update_files(url)
            except RuntimeError as exc:
                print(f'Skipping {collector} {month_path}: {exc}', file=sys.stderr)
                continue

            if not files:
                print(f'No update files found for {collector} {month_path}')
                continue

            for filename in files:
                file_date = parse_file_date(filename)
                if file_date is None:
                    continue
                if file_date < args.start_date or file_date > args.stop_date:
                    continue

                destination_dir = os.path.join(args.data_dir, collector, file_date.isoformat())
                destination_file = os.path.join(destination_dir, filename)
                source_url = urljoin(url, filename)
                if args.dry_run:
                    print(f'[DRY RUN] {source_url} -> {destination_file}')
                else:
                    try:
                        download_file(source_url, destination_file)
                        downloaded_any = True
                    except RuntimeError as exc:
                        print(f'Failed to download {source_url}: {exc}', file=sys.stderr)
        if downloaded_any:
            print(f'Successfully downloaded files for collector: {collector}')

    return 0


if __name__ == '__main__':
    sys.exit(main())
