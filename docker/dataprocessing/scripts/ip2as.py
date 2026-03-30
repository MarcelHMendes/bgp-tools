#!/usr/bin/env python3

from __future__ import annotations

import bz2
import gzip
import json
import logging
import os
import pathlib
import sqlite3
from typing import Optional

import radix
import requests

__all__ = ["IP2ASDict", "IP2ASRadix", "loaders"]

OriginSets = list[set[int]]  # list is sorted by decreasing frequency

REQUESTS_TIMEOUT = 15.0
REQUESTS_DOWNLOAD_TIMEOUT = 60.0  # files are around 4MB
CAIDA_PFX2AS_BASEURL = (
    "https://publicdata.caida.org/datasets/routing/routeviews-prefix2as"
)
INDEX = "pfx2as-creation.log"


class IP2ASDict:
    @staticmethod
    def from_team_cymru_sqlite3(fn: pathlib.Path) -> dict[str, int]:
        conn = sqlite3.connect(fn)
        cur = conn.cursor()
        ip2as = dict(
            (ip, asn)
            for ip, asn in cur.execute("SELECT addr, asn FROM ip_asn_mapping;")
            if isinstance(asn, int) and asn > 0
        )
        # logging.info("Loaded bdrmapit ip2as with %d IPs from %s", len(ip2as), fn)
        return ip2as

    @staticmethod
    def from_json(fn: pathlib.Path) -> dict[str, int]:
        with open(fn, encoding="utf8") as fd:
            ip2asraw = json.load(fd)
        assert isinstance(ip2asraw, dict)
        logging.info("Loaded JSON ip2as with %d IPs from %s", len(ip2asraw), fn)
        return {k: int(v) for k, v in ip2asraw.items()}

    @staticmethod
    def dump_to_json(db: dict[str, int], fn: pathlib.Path):
        with open(fn, "w", encoding="utf8") as fd:
            data = {k: v for k, v in db.items()}
            json.dump(data, fd, indent=2)


class IP2ASRadix:
    def __init__(self, rt: radix.Radix):
        self.radix = rt

    def get(self, addr: str) -> Optional[int]:
        """Return the smallest ASN in the most frequent origin AS-set"""
        node = self.radix.search_best(addr)
        if node is None:
            return None
        return node.data.get("asn")

    def get_origins(self, addr: str) -> Optional[OriginSets]:
        """Return list of origin AS-sets sorted by frequency"""
        node = self.radix.search_best(addr)
        if node is None:
            return None
        return node.data.get("origins")

    def get_prefix(self, addr: str) -> Optional[str]:
        """Return most specific prefix containing addr in DB"""
        node = self.radix.search_best(addr)
        if node is None:
            return None
        return node.prefix

    @staticmethod
    def from_caida_prefix2as(fn: pathlib.Path) -> IP2ASRadix:
        rt = radix.Radix()
        if fn.suffix == ".gz":
            fd = gzip.open(fn, "rt")
        elif fn.suffix == ".bz2":
            fd = bz2.open(fn, "rt")
        else:
            fd = open(fn, encoding="utf8")
        nlines = 0
        for line in fd:
            nlines += 1
            # example worst case: 66.81.240.0 \t 20 \t 40627_7224,14618,16509
            addr, pfxstr, asnstr = line.split("\t")
            pfxlen = int(pfxstr)
            node = rt.add(addr, pfxlen)
            node.data["origins"] = [
                set(int(asn) for asn in t.split(",")) for t in asnstr.split("_")
            ]
            node.data["asn"] = min(node.data["origins"][0])
        logging.info("Loaded prefix2as with %d prefixes from %s", nlines, fn)
        fd.close()
        return IP2ASRadix(rt)

    @staticmethod
    def download_latest_caida_pfx2as(basedir: pathlib.Path) -> pathlib.Path:
        index = f"{CAIDA_PFX2AS_BASEURL}/{INDEX}"
        r = requests.get(index, timeout=REQUESTS_TIMEOUT)
        r.raise_for_status()

        lastline = r.text.splitlines()[-1].strip()
        try:
            serialstr, tstampstr, pathstr = lastline.split("\t")
            _serial = int(serialstr)
            _tstamp = int(tstampstr)
            fpath = pathlib.Path(pathstr)
        except ValueError:
            logging.error("Error parsing BGP prefix-to-AS index (line=%s)", lastline)
            raise

        localfp = basedir / fpath
        if localfp.exists():
            logging.debug(
                "Last BGP prefix-to-AS mapping already downloaded (path=%s)", localfp
            )
            return localfp

        remoteurl = f"{CAIDA_PFX2AS_BASEURL}/{fpath}"
        r = requests.get(remoteurl, timeout=REQUESTS_DOWNLOAD_TIMEOUT)
        r.raise_for_status()
        localfp.parent.mkdir(parents=True, exist_ok=True)  # Mudança aqui
        with open(localfp, "wb") as fd:
            fd.write(r.content)
        return localfp

    @staticmethod
    def from_bdrmapit_prefix2as(fn: pathlib.Path):
        rt = radix.Radix()
        with open(fn, "r", encoding="utf8") as fd:
            for line in fd:
                prefix, asstr = line.split()
                asn = int(asstr)
                if asn < 0:
                    continue
                node = rt.add(prefix)
                node.data["asn"] = asn
        return IP2ASRadix(rt)
