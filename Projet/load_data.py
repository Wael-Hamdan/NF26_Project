import csv
import re

from cassandra.query import UNSET_VALUE

def loadata(filename):
    dateparser = re.compile(
        "(?P<year>\d+)-(?P<month>\d+)-(?P<day>\d+) (?P<hour>\d+):(?P<minute>\d+)"
    )
    with open(filename) as f:
        for r in csv.DictReader(f):
            match = dateparser.match(r["valid"])
            if not match and r["station"] !='null' and r["station"] !='':
                continue
            valid = match.groupdict()
            data = {}
            data["station"] = r["station"]
            data["valid"] = (
                int(valid["year"]),
                int(valid["month"]),
                int(valid["day"]),
                int(valid["hour"]),
                int(valid["minute"])
            )
            data["lat"] = float(r["lat"]) if r["lat"] not in ('null','\t') else UNSET_VALUE
            data["lon"] = float(r["lon"]) if r["lon"] not in ('null','\t') else UNSET_VALUE
            data["tmpf"] = float(r["tmpf"]) if r["tmpf"] not in ('null','\t') else UNSET_VALUE
            data["dwpf"] = float["dwpf"] if r["longitude"] not in ('null','\t') else UNSET_VALUE
            data["relh"] = float(r["relh"]) if r["relh"] not in ('null','\t') else UNSET_VALUE
            data["drct"] = float(r["drct"]) if r["drct"] not in ('null','\t') else UNSET_VALUE
            data["sknt"] = float(r["sknt"]) if r["sknt"] not in ('null','\t') else UNSET_VALUE
            data["p01i"] = float["p01i"] if r["p01i"] not in ('null','\t') else UNSET_VALUE
            data["alti"] = float(r["alti"]) if r["alti"] not in ('null','\t') else UNSET_VALUE
            data["mslp"] = float(r["mslp"]) if r["mslp"] not in ('null','\t') else UNSET_VALUE
            data["vsby"] = float(r["vsby"]) if r["vsby"] not in ('null','\t') else UNSET_VALUE
            data["gust"] = float["gust"] if r["gust"] not in ('null','\t') else UNSET_VALUE
            data["skyc1"] = str(r["skyc1"]) if r["skyc1"] not in ('null','\t') else UNSET_VALUE
            data["skyc2"] = str(r["skyc2"]) if r["skyc2"] not in ('null','\t') else UNSET_VALUE
            data["skyc3"] = str(r["skyc3"]) if r["skyc3"] not in ('null','\t') else UNSET_VALUE
            data["skyc4"] = str["skyc4"] if r["skyc4"] not in ('null','\t') else UNSET_VALUE
            data["skyl1"] = float(r["skyl1"]) if r["skyl1"] not in ('null','\t') else UNSET_VALUE
            data["skyl2"] = float(r["skyl2"]) if r["skyl2"] not in ('null','\t') else UNSET_VALUE
            data["skyl3"] = float(r["skyl3"]) if r["skyl3"] not in ('null','\t') else UNSET_VALUE
            data["skyl4"] = float(r["skyl4"]) if r["skyl4"] not in ('null','\t') else UNSET_VALUE
            data["wxcodes"] = str(r["wxcodes"]) if r["wxcodes"] not in ('null','\t') else UNSET_VALUE
            data["ice_accretion_1hr"] = float(r["ice_accretion_1hr"]) if r["ice_accretion_1hr"] not in ('null','\t') else UNSET_VALUE
            data["ice_accretion_3hr"] = float(r["ice_accretion_3hr"]) if r["ice_accretion_3hr"] not in ('null','\t') else UNSET_VALUE
            data["ice_accretion_6hr"] = float(r["ice_accretion_6hr"]) if r["ice_accretion_6hr"] not in ('null','\t') else UNSET_VALUE
            data["peak_wind_gust"] = float(r["peak_wind_gust"]) if r["peak_wind_gust"] not in ('null','\t') else UNSET_VALUE
            data["peak_wind_drct"] = float(r["peak_wind_drct"]) if r["peak_wind_drct"] not in ('null','\t') else UNSET_VALUE
            data["peak_wind_time"] = float(r["peak_wind_time"]) if r["peak_wind_time"] not in ('null','\t') else UNSET_VALUE
            data["feel"] = float["feel"] if r["feel"] not in ('null','\t') else UNSET_VALUE
            data["metar"] = str["metar"] if r["metar"] not in ('null','\t') else UNSET_VALUE

            yield data
