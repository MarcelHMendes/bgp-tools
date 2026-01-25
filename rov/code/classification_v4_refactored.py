#!/usr/bin/env python3

import lib
from collections import defaultdict
import pickle
import sys
import os
import re
import json
import argparse
from multiprocessing import Pool
from typing import Dict, List, Tuple, Set, Any

# Constants
BAD_ORIGIN = "61574"  # change after new exp
GOOD_ORIGIN = "47065"
MAX_PROCESSES = 8  # max processes running in parallel
ASREL_FILE = "../data/20231201.as-rel.txt"
CONFIG_FILE = "../config.json"
DUMP_DIR = "../dump/"

class RouteProcessor:
    """Class to handle route processing and classification logic"""
    
    def __init__(self, measurement: str):
        self.measurement = measurement
        self.asrel_data = self._load_asrel_data()
        
    def _load_asrel_data(self) -> List[Tuple[str, str, str]]:
        """Load and parse AS relationship data"""
        pattern = r"^(\d+)\|(\d+)\|(\d+)$"
        with open(ASREL_FILE, "r") as file:
            return re.findall(pattern, file.read(), re.MULTILINE)
    
    @staticmethod
    def check_intersection(asn_t: str, p2: Dict, p5: Dict, p3: Dict, p1: Dict) -> bool:
        """Check if ASN appears in all route sets with non-origin neighbors"""
        intersection = set(p2[asn_t]) & set(p5[asn_t]) & set(p3[asn_t]) & set(p1[asn_t])
        for asn in intersection:
            if int(asn) == int(asn_t):
                continue
            if int(asn) not in (int(GOOD_ORIGIN), int(BAD_ORIGIN)):
                return True
        return False
    
    @staticmethod
    def remove_adjacent_duplicates(input_list: List) -> List:
        """Remove consecutive duplicates from a list"""
        if not input_list:
            return []
        return [input_list[0]] + [
            input_list[i] for i in range(1, len(input_list)) 
            if input_list[i] != input_list[i - 1]
        ]
    
    @staticmethod
    def complete_routes(route_per_asn: Dict) -> Dict:
        """Complete partial routes in the route dictionary"""
        temp = defaultdict(list)
        route_per_asn = dict(sorted(route_per_asn.items(), key=lambda x: len(x[1])))
        
        for asn, as_list in route_per_asn.items():
            for ientry in range(len(as_list)):
                if (as_list[ientry] not in route_per_asn) and (as_list[ientry] not in temp):
                    temp[as_list[ientry]] = as_list[ientry:]
        
        route_per_asn.update(temp)
        return route_per_asn
    
    @staticmethod
    def parse_routes(route_per_asn: Dict, as_list: List) -> Tuple[Dict, int]:
        """Parse routes and remove duplicates, returning max route length"""
        max_len = 0
        for asn in as_list:
            if asn in route_per_asn:
                route_per_asn[asn] = RouteProcessor.remove_adjacent_duplicates(route_per_asn[asn])
                max_len = max(max_len, len(route_per_asn[asn]))
            else:
                route_per_asn[asn] = []
        return (route_per_asn, max_len)
    
    @staticmethod
    def add_appends(data: Dict) -> Dict:
        """Extend routes ending with GOOD_ORIGIN"""
        for asn, route in data.items():
            if route and route[-1].isdigit() and int(route[-1]) == int(GOOD_ORIGIN):
                data[asn].extend(6 * [GOOD_ORIGIN])
        return data
    
    def find_neighbors(self, asn_t: str) -> List[str]:
        """Find neighbors of an ASN using AS relationship data"""
        neighbors = []
        for match in self.asrel_data:
            if asn_t in match:
                neighbors.append(match[1] if asn_t == match[0] else match[0])
        return neighbors
    
    def relationship(self, asn1: str, asn2: str) -> str:
        """Determine relationship between two ASNs"""
        for match in self.asrel_data:
            if asn1 in match and asn2 in match:
                if asn1 == match[0] and asn2 == match[1]:
                    if int(match[2]) == 0:
                        return "p2p"
                    elif int(match[2]) == -1:
                        return "prov-cli"
                elif asn1 == match[1] and asn2 == match[0]:
                    if int(match[2]) == 0:
                        return "p2p"
                    elif int(match[2]) == -1:
                        return "cli-prov"
        return "p2p"
    
    def is_valley_free(self, route: List[str], asn_t: str) -> bool:
        """Check if route is valley-free"""
        try:
            index = route.index(GOOD_ORIGIN)
        except ValueError:
            return True
            
        route_wo_origin = route[:index]
        if route_wo_origin and route_wo_origin[0] != asn_t:
            route_wo_origin.insert(0, asn_t)
        
        if len(route_wo_origin) < 3:
            return True
            
        for i in range(len(route_wo_origin) - 2):
            rel1 = self.relationship(route_wo_origin[i], route_wo_origin[i+1])
            rel2 = self.relationship(route_wo_origin[i+1], route_wo_origin[i+2])
            
            if (rel1 == "prov-cli" and rel2 == "cli-prov") or \
               (rel1 == "p2p" and rel2 == "p2p") or \
               (rel1 == "prov-cli" and rel2 == "p2p") or \
               (rel1 == "p2p" and rel2 == "cli-prov"):
                return False
        return True
    
    def check_target_receive_route(self, asn_t: str, route: Dict, origin: str) -> bool:
        """Check if target ASN receives route from neighbors"""
        if len(route.get(asn_t, [])) < 2:
            return False
            
        neighbors = self.find_neighbors(asn_t)
        candidates = [
            n for n in neighbors 
            if n in route and len(route[n]) >= 2 and route[n][-1] == origin
        ]
        
        if not candidates:
            return False
            
        return all(self.is_valley_free(route[c], asn_t) for c in candidates)
    
    @staticmethod
    def assert_one_classification(asn_t: str, data: Dict, class_t: List[str], class_dict: Dict) -> bool:
        """Check if at least one ASN in route has classification in class_t"""
        for asn in data.get(asn_t, []):
            if asn == asn_t:
                continue
            if asn in class_dict and class_dict[asn] in class_t:
                return True
        return False
    
    @staticmethod
    def assert_all_classification(asn_t: str, data: Dict, class_t: List[str], class_dict: Dict) -> bool:
        """Check if all ASNs in route have classification in class_t"""
        for asn in data.get(asn_t, []):
            if asn in (GOOD_ORIGIN, BAD_ORIGIN, asn_t):
                continue
            if asn not in class_dict or class_dict[asn] not in class_t:
                return False
        return True
    
    @staticmethod
    def get_stable_trace(asn_tracerout_list: List[List]) -> List:
        """Get stable trace from traceroute results"""
        if len(asn_tracerout_list) > 1 and asn_tracerout_list[-1] == asn_tracerout_list[-2]:
            return list(map(str, asn_tracerout_list[-1]))
        return []
    
    @staticmethod
    def get_records(bgpdump_file: str, start: int, end: int, prefix: str) -> Dict:
        """Get BGP records for a specific prefix and time range"""
        records = lib.read_bgpdump_file(
            bgpdump_file=bgpdump_file,
            start_timestamp=start,
            end_timestamp=end,
        )
        return {
            rec["peer_asn"]: rec["as-path"]
            for rec in records
            if rec["prefix"] == prefix and rec["type"] == "A"
        }
    
    def integrate_traces(self, traceroutes: List, pfx_routes: Dict, 
                        start: int, end: int, prefix: str) -> Dict:
        """Integrate traceroute data with BGP routes"""
        mapping_asn_traces = defaultdict(list)
        for trace in traceroutes:
            if (lib.is_timestamp_between(start, end, trace["endtime"]) and 
                prefix == trace["dst_addr"]):
                mapping_asn_traces[str(trace["origin_asn"])].append(trace["result"])
        
        asn_trace = {
            asn: self.get_stable_trace(traces)
            for asn, traces in mapping_asn_traces.items()
        }
        
        for asn, trace in asn_trace.items():
            if asn.isdigit() and asn not in pfx_routes:
                for i, asn_i in enumerate(trace):
                    if not asn_i.isdigit():
                        break
                    if asn_i == asn:
                        continue
                    if asn_i in pfx_routes:
                        pfx_routes[asn] = trace[:i] + pfx_routes[asn_i]
                        if pfx_routes[asn][0] != asn:
                            pfx_routes[asn].insert(0, asn)
        return pfx_routes
    
    def classification_phase1(self, asn_t: str, class_dict: Dict, 
                            p2: Dict, p4: Dict, p5: Dict, p3: Dict, p1: Dict) -> None:
        """First phase of ASN classification"""
        if not asn_t.isdigit() or asn_t in class_dict:
            return
            
        p2_ends_bad = p2.get(asn_t, []) and int(p2[asn_t][-1]) == int(BAD_ORIGIN)
        p4_ends_bad = p4.get(asn_t, []) and int(p4[asn_t][-1]) == int(BAD_ORIGIN)
        p5_ends_good = p5.get(asn_t, []) and int(p5[asn_t][-1]) == int(GOOD_ORIGIN)
        p3_ends_good = p3.get(asn_t, []) and int(p3[asn_t][-1]) == int(GOOD_ORIGIN)
        p1_ends_bad = p1.get(asn_t, []) and int(p1[asn_t][-1]) == int(BAD_ORIGIN)
        p5_ends_bad = p5.get(asn_t, []) and int(p5[asn_t][-1]) == int(BAD_ORIGIN)
        
        if p2_ends_bad and p4_ends_bad and p5_ends_bad and p3_ends_good and p1_ends_bad:
            class_dict[asn_t] = "ignore-roa"
        elif p2_ends_bad and p4_ends_bad and p5_ends_good and p3_ends_good and p1_ends_bad:
            class_dict[asn_t] = "prefer-valid"
        elif p2_ends_bad and not p4.get(asn_t, []) and p5_ends_good and p3_ends_good and p1_ends_bad:
            class_dict[asn_t] = "drop-invalid"
        else:
            class_dict[asn_t] = "unknown"
    
    def classification_phase2(self, asn_t: str, class_dict: Dict, corner_cases: Dict, 
                            total_corner_case: Dict, p2: Dict, p5: Dict, p3: Dict, p1: Dict) -> None:
        """Second phase of ASN classification with corner case handling"""
        if (not asn_t.isdigit() or asn_t in (BAD_ORIGIN, GOOD_ORIGIN, "20473") or 
            asn_t not in class_dict):
            return
            
        classification = class_dict[asn_t]
        total_corner_case[asn_t] = classification
        
        if classification == "drop-invalid":
            if self.assert_one_classification(
                asn_t, p2, ["drop-invalid", "unknown", "prefer-valid"], class_dict
            ):
                class_dict[asn_t] = "unknown-protected"
                corner_cases[asn_t] = ("drop-invalid", "unknown-protected")
                
        elif classification == "ignore-roa":
            if not self.check_target_receive_route(asn_t, p5, GOOD_ORIGIN):
                class_dict[asn_t] = "prefer-ignore"
                corner_cases[asn_t] = ("ignore-roa", "prefer-ignore")
                
        elif classification == "prefer-valid":
            if self.check_intersection(asn_t, p2, p5, p3, p1):
                class_dict[asn_t] = "prefer-peer/prefer-valid"
                corner_cases[asn_t] = ("prefer-valid", "prefer-peer/prefer-valid")
            elif not self.check_target_receive_route(asn_t, p5, BAD_ORIGIN):
                class_dict[asn_t] = "prefer-ignore"
                corner_cases[asn_t] = ("prefer-valid", "prefer-ignore")
    
    def process_routes(self, dump_file: str, start: int, end: int, 
                      prefix: str, traceroutes: List, trace_prefix: str) -> Dict:
        """Process routes for a specific prefix"""
        routes = self.get_records(dump_file, start, end, prefix)
        routes = self.complete_routes(routes)
        routes = self.integrate_traces(traceroutes, routes, start, end, trace_prefix)
        return self.complete_routes(routes)
    
    def save_results(self, city: str, data: Dict, filename: str) -> None:
        """Save results to pickle file"""
        base_path = os.path.join(DUMP_DIR, self.measurement, city)
        os.makedirs(base_path, exist_ok=True)
        with open(os.path.join(base_path, filename), "wb") as f:
            pickle.dump(data, f)
    
    def classify(self, city: str, time_range: Dict, 
                base_dump: str, traceroutes: List) -> Tuple[str, Dict]:
        """Main classification method"""
        start, end = time_range["start"], time_range["end"]
        nicbr_dump = f"{base_dump}_roa_sorted.json"
        arin_dump = f"{base_dump}_no_roa_sorted.json"
        
        # Process all route sets
        p2 = self.process_routes(nicbr_dump, start, end, "138.185.228.0/24", 
                               traceroutes, "138.185.228.1")
        p4 = self.process_routes(nicbr_dump, start, end, "138.185.229.0/24", 
                               traceroutes, "138.185.229.1")
        p5 = self.process_routes(nicbr_dump, start, end, "138.185.230.0/24", 
                               traceroutes, "138.185.230.1")
        p3 = self.process_routes(nicbr_dump, start, end, "138.185.231.0/24", 
                               traceroutes, "138.185.231.1")
        p1 = self.process_routes(arin_dump, start, end, "204.9.170.0/24", 
                               traceroutes, "138.185.228.1")  # change after new exp
        
        # Get all unique ASNs
        as_list = list(set(p2) | set(p4) | set(p5) | set(p3) | set(p1))
        
        # Parse routes and get max length
        p2, max_p2 = self.parse_routes(p2, as_list)
        p4, max_p4 = self.parse_routes(p4, as_list)
        p5, max_p5 = self.parse_routes(p5, as_list)
        p3, max_p3 = self.parse_routes(p3, as_list)
        p1, max_p1 = self.parse_routes(p1, as_list)
        
        max_len = max(max_p2, max_p4, max_p5, max_p3, max_p1)
        
        # Initialize classification dictionary
        class_dict = {
            GOOD_ORIGIN: "ignore-roa",
            BAD_ORIGIN: "ignore-roa",
            "20473": "drop-invalid"
        }
        
        # Perform classification in reverse order
        for i in range(max_len - 1, -1, -1):
            for route_set in (p2, p4, p5, p3):
                for asn in route_set:
                    if len(route_set[asn]) > i:
                        self.classification_phase1(route_set[asn][i], class_dict, p2, p4, p5, p3, p1)
        
        # Handle corner cases
        corner_cases = {}
        total_cases_phase1 = {}
        for i in range(max_len - 1, -1, -1):
            for route_set in (p2, p4, p5, p3):
                for asn in route_set:
                    if len(route_set[asn]) > i:
                        self.classification_phase2(
                            route_set[asn][i], class_dict, corner_cases, 
                            total_cases_phase1, p2, p5, p3, p1
                        )
        
        # Add appends and save results
        p2 = self.add_appends(p2)
        p4 = self.add_appends(p4)
        p5 = self.add_appends(p5)
        p3 = self.add_appends(p3)
        p1 = self.add_appends(p1)
        
        self.save_results(city, class_dict, "classification")
        self.save_results(city, p2, "p2")
        self.save_results(city, p4, "p4")
        self.save_results(city, p5, "p5")
        self.save_results(city, p3, "p3")
        self.save_results(city, p1, "p1")
        
        return (city, class_dict)

def create_parser() -> argparse.ArgumentParser:
    """Create command line argument parser"""
    desc = "Process BGP measurements"
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument(
        "-m", "--measurement",
        dest="measurement",
        action="store",
        required=True,
        help="Name of target measurement"
    )
    return parser

def main() -> None:
    """Main function to process measurements"""
    parser = create_parser()
    opts = parser.parse_args()
    
    with open(CONFIG_FILE, "r") as config_fd:
        config = json.load(config_fd)
    
    traceroutes = []
    traceroute_file = config[opts.measurement].get("traceroute_file")
    if traceroute_file:
        with open(f"../data/{traceroute_file}", "r") as trace_data:
            traceroutes = json.load(trace_data)
    
    processor = RouteProcessor(opts.measurement)
    locations = [
        (city, config[opts.measurement]["location"][city], 
         config[opts.measurement]["bgpdump"], traceroutes)
        for city in config[opts.measurement]["location"]
    ]
    
    # Process locations in parallel
    with Pool(processes=min(MAX_PROCESSES, len(locations))) as pool:
        results = pool.map(processor.classify, [
            (city, time, base_dump, traces) 
            for city, time, base_dump, traces in locations
        ])
    
    # Save final classification
    final_classification = {city: class_dict for city, class_dict in results}
    with open(f"{DUMP_DIR}/{opts.measurement}.json", "w") as fd_out:
        json.dump(final_classification, fd_out, indent=4)

if __name__ == "__main__":
    sys.exit(main())
