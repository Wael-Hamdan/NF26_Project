import csv
import re


def loadata(filename):
    dateparser = re.compile(
        "(?P<year>\d+)-(?P<month>\d+)-(?P<day>\d+) (?P<hour>\d+):(?P<minute>\d+):(?P<seconds>\d+\.?\d*)"
    )
    with open(filename) as f:
        for r in csv.DictReader(f):
            match = dateparser.match(r["valid"])
            if not match:
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
            data["latitude"] = float(r["latitude"])
            data["longitude"] = float(r["longitude"])
            data["tmpf"] = float(r["tmpf"])
            data["dwpf"] = float["dwpf"]
            data["relh"] = float(r["relh"])
            data["drct"] = float(r["drct"])
            data["sknt"] = float(r["sknt"])
            data["p01i"] = float["p01i"]
            data["alti"] = float(r["alti"])
            data["mslp"] = float(r["mslp"])
            data["vsby"] = float(r["vsby"])
            data["gust"] = float["gust"]
            data["skyc1"] = str(r["skyc1"])
            data["skyc2"] = str(r["skyc2"])
            data["skyc3"] = str(r["skyc3"])
            data["skyc4"] = str["skyc4"]
            data["skyl1"] = float(r["skyl1"])
            data["skyl2"] = float(r["skyl2"])
            data["skyl3"] = float(r["skyl3"])
            data["skyl4"] = float(r["skyl4"])
            data["wxcodes"] = str(r["wxcodes"])
            data["ice_accretion_1hr"] = float(r["ice_accretion_1hr"])
            data["ice_accretion_3hr"] = float(r["ice_accretion_3hr"])
            data["ice_accretion_6hr"] = float(r["ice_accretion_6hr"])
            data["peak_wind_gust"] = float(r["peak_wind_gust"])
            data["peak_wind_drct"] = float(r["peak_wind_drct"])
            data["peak_wind_time"] = float(r["peak_wind_time"])
            data["feel"] = float["feel"]
            data["metar"] = str["metar"]

            yield data
