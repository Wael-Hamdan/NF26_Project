#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun 21 08:51:17 2019

@author: quentin
"""

from cassandra.cluster import Cluster
import itertools as it
import argparse
import csv
import re

#from cassandra.query import UNSET_VALUE

# construct the argument parse and parse the arguments
ap = argparse.ArgumentParser()
# suppose the connection to an existing cluster
ap.add_argument("-f", "--filename", required=False, default="asos.csv",
	help="file containing data to be inserted")
ap.add_argument("-k", "--keyspace", required=False, default="penonque_project",
	help="name of the keyspace of open a session")
ap.add_argument("-l", "--limit", required=False, default=100,
	help="number of lines to be added")
args = vars(ap.parse_args())

# load the keyspace name
filename = args["filename"]
keyspace = args["keyspace"]
limit = int(args["limit"])

cluster = Cluster()
session = cluster.connect(keyspace)

limiteur = lambda data,limit: (d for i,d in it.takewhile(lambda w: w[0]<limit, enumerate(data)))

# =============================================================================
# RECUPERATION DES DONNEES DEPUIS CSV
# =============================================================================

def load_data(filename):
    dateparser = re.compile(
        "(?P<year>\d+)-(?P<month>\d+)-(?P<day>\d+) (?P<hour>\d+):(?P<minute>\d+)"
    )
    with open(filename) as f:
        for r in csv.DictReader(f):
            match = dateparser.match(r["valid"])
            if not match:
                continue
            valid = match.groupdict()
            data = {}
            data["station"] = r["station"]
            data["year"] = int(valid["year"])
            data["month"] = int(valid["month"])
            data["day"] = int(valid["day"])
            data["hour"] = int(valid["hour"])
            data["minute"] = int(valid["minute"])            
            data["lat"] = float(r["lat"]) if r["lat"] not in ('null','\t') else None
            data["lon"] = float(r["lon"]) if r["lon"] not in ('null','\t') else None
            data["tmpf"] = float(r["tmpf"]) if r["tmpf"] not in ('null','\t') else None
            data["dwpf"] = float(r["dwpf"]) if r["dwpf"] not in ('null','\t') else None
            data["relh"] = float(r["relh"]) if r["relh"] not in ('null','\t') else None
            data["drct"] = float(r["drct"]) if r["drct"] not in ('null','\t') else None
            data["sknt"] = float(r["sknt"]) if r["sknt"] not in ('null','\t') else None
            data["p01i"] = float(r["p01i"]) if r["p01i"] not in ('null','\t') else None
            data["alti"] = float(r["alti"]) if r["alti"] not in ('null','\t') else None
            data["mslp"] = float(r["mslp"]) if r["mslp"] not in ('null','\t') else None
            data["vsby"] = float(r["vsby"]) if r["vsby"] not in ('null','\t') else None
            data["gust"] = float(r["gust"]) if r["gust"] not in ('null','\t') else None
            data["skyc1"] = r["skyc1"] if r["skyc1"] not in ('null','\t') else None
            data["skyc2"] = r["skyc2"] if r["skyc2"] not in ('null','\t') else None
            data["skyc3"] = r["skyc3"] if r["skyc3"] not in ('null','\t') else None
            data["skyc4"] = r["skyc4"] if r["skyc4"] not in ('null','\t') else None
            data["skyl1"] = float(r["skyl1"]) if r["skyl1"] not in ('null','\t') else None
            data["skyl2"] = float(r["skyl2"]) if r["skyl2"] not in ('null','\t') else None
            data["skyl3"] = float(r["skyl3"]) if r["skyl3"] not in ('null','\t') else None
            data["skyl4"] = float(r["skyl4"]) if r["skyl4"] not in ('null','\t') else None
            data["wxcodes"] = r["wxcodes"] if r["wxcodes"] not in ('null','\t') else None
            data["ice_accretion_1hr"] = float(r["ice_accretion_1hr"]) if r["ice_accretion_1hr"] not in ('null','\t') else None
            data["ice_accretion_3hr"] = float(r["ice_accretion_3hr"]) if r["ice_accretion_3hr"] not in ('null','\t') else None
            data["ice_accretion_6hr"] = float(r["ice_accretion_6hr"]) if r["ice_accretion_6hr"] not in ('null','\t') else None
            data["peak_wind_gust"] = float(r["peak_wind_gust"]) if r["peak_wind_gust"] not in ('null','\t') else None
            data["peak_wind_drct"] = float(r["peak_wind_drct"]) if r["peak_wind_drct"] not in ('null','\t') else None
            data["peak_wind_time"] = r["peak_wind_time"] if r["peak_wind_time"] not in ('null','\t') else None
            data["feel"] = float(r["feel"]) if r["feel"] not in ('null','\t') else None
            yield data

# =============================================================================
# ALIMENTATION DES TABLES
# =============================================================================

# From Elias Hariz method
def write_data(csvfilename, table, session) :
    inserted = 0
    for data in load_data(csvfilename):
        values = []
        for x in data.values() :
            if x == None:
                values.append('NULL')
            elif isinstance(x,str):
                values.append("'{}'".format(x))
            else:
                values.append(x)
        session.execute("INSERT INTO {} (station,year,month,day,hour,minute,lon,lat,tmpf,dwpf,relh,drct,sknt,p01i,alti,mslp,vsby,gust,skyc1,skyc2,\
            skyc3,skyc4,skyl1,skyl2,skyl3,skyl4,wxcodes,ice_accretion_1hr,ice_accretion_3hr,ice_accretion_6hr,\
            peak_wind_gust,peak_wind_drct,peak_wind_time,feel) \
        VALUES({});".format(table,','.join(str(x) for x in values))
        )
        inserted += 1
        print(inserted)
    return inserted
        

# =============================================================================
# CREATION DES TABLES
# =============================================================================

session.execute("DROP TABLE IF EXISTS metar_byposition ;")
session.execute(
    """
    CREATE TABLE metar_byposition (
            station text,                
            year int,                   
            month int,                  
            day int,                    
            hour int,                   
            minute int,                 
            lon float,                  
            lat float,                  
            tmpf float,                 
            dwpf float,                 
            relh float,                 
            drct float,                 
            sknt float,                 
            p01i float,                 
            alti float,                 
            mslp float,                 
            vsby float,                 
            gust float,                 
            skyc1 text,                 
            skyc2 text,                 
            skyc3 text,                 
            skyc4 text,                 
            skyl1 float,                
            skyl2  float,               
            skyl3 float,                
            skyl4 float,                
            wxcodes text,               
            ice_accretion_1hr float,    
            ice_accretion_3hr float,    
            ice_accretion_6hr float,    
            peak_wind_gust float,       
            peak_wind_drct float,       
            peak_wind_time timestamp,       
            feel float,                 
        PRIMARY KEY ((lat, lon), year, month, day, hour, minute)     
    )
    """
)


session.execute("DROP TABLE IF EXISTS metar_bydate ;")
session.execute(
    """
    CREATE TABLE metar_bydate (
            station text, 
            year int, 
            month int, 
            day int, 
            hour int, 
            minute int, 
            lon float, 
            lat float, 
            tmpf float, 
            dwpf float, 
            relh float, 
            drct float, 
            sknt float, 
            p01i float, 
            alti float, 
            mslp float, 
            vsby float, 
            gust float, 
            skyc1 text, 
            skyc2 text, 
            skyc3 text, 
            skyc4 text, 
            skyl1 float, 
            skyl2  float, 
            skyl3 float, 
            skyl4 float, 
            wxcodes text, 
            ice_accretion_1hr float, 
            ice_accretion_3hr float, 
            ice_accretion_6hr float, 
            peak_wind_gust float, 
            peak_wind_drct float, 
            peak_wind_time timestamp, 
            feel float, 
        PRIMARY KEY ((year, month), day, hour, minute)
    )
    """
)

# =============================================================================
# DEBUT
# =============================================================================

N = write_data(filename, "metar_byposition", session)
print("[INFO] {} lignes insérées".format(N))
N = write_data(filename, "metar_bydate", session)
print("[INFO] {} lignes insérées".format(N))
