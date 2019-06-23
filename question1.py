#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jun 23 13:43:46 2019

@author: quentin
"""
from cassandra.cluster import Cluster
from pyspark import SparkContext
import itertools as it
import argparse
import matplotlib.pyplot as plt
import numpy as np
import csv

# construct the argument parse and parse the arguments
ap = argparse.ArgumentParser()
# suppose the connection to an existing cluster
ap.add_argument("-la", "--latitude", required=True,
	help="Latitude of position you want to study")
ap.add_argument("-lo", "--longitude", required=True,
	help="Longitude of position you want to study")
ap.add_argument("-y", "--year", required=True,
	help="year you want to compare with")
ap.add_argument("-i", "--indicator", required=True,
	help="indicator you want to study")
ap.add_argument("-k", "--keyspace", required=False, default="penonque_project",
	help="name of the keyspace of open a session")
ap.add_argument("-t", "--table", required=False, default="metar_byposition",
	help="table in which doing requests")
args = vars(ap.parse_args())

# load the arguments
latitude = np.around(float(args["latitude"]), 4)
longitude = np.around(float(args["longitude"]), 4)
year = int(args["year"])
indicator = args["indicator"]
keyspace = args["keyspace"]
table = args["table"]

cluster = Cluster()
session = cluster.connect(keyspace)

limiteur = lambda data,limit: (d for i,d in it.takewhile(lambda w: w[0]<limit, enumerate(data)))

def euclidean_distance(p1, p2):
    return np.sqrt(np.power(p1[0] - p2[0], 2), np.power(p1[1] - p2[1], 2))

def calc_moy_key(t):
    (key, (s0, s1)) = t
    return (key, s1/s0)


def check_position(lon, lat):
    eps = 0.001
    new_lon, new_lat = lon, lat
    with open("stations.csv") as f:
        dist = float('Inf')
        for r in csv.DictReader(f):
            ed = euclidean_distance([lon,lat], np.around([r["lon"],r["lat"]], 4))
            if ed < eps:
                return lon, lat, r["station"]
            elif ed < dist:
                dist = ed 
                new_lon, new_lat, station = r["lon"], r["lat"], r["station"]
        return new_lon, new_lat, station

def read_byposition(session, lat, lon, ind):
    return session.execute("SELECT year, month, {}    \
                            FROM metar_byposition           \
                            WHERE lat = {} AND lon = {}"
                            .format(ind, lat, lon))  

def compute_seasonal(session, lat, lon, y, ind):
    sc = SparkContext.getOrCreate()
    
    # retrieve informations from db
    res_query = read_byposition(session, lat, lon, ind)
    
    rdd = sc.parallelize(res_query)
    rdd = rdd.filter(lambda d: d[2] is not None)

    rdd_mean = rdd.filter(lambda d: d[0] != y)
    mapping_mean = rdd_mean.map(lambda d: (d.month, np.array([1, d[2]])))
    reducing_mean = mapping_mean.reduceByKey(lambda a,b: a+b)  
    result_mean = reducing_mean.map(calc_moy_key)
    
    rdd_year = rdd.filter(lambda d: d[0] == y)
    mapping_year = rdd_year.map(lambda d: (d.month, np.array([1, d[2]])))
    reducing_year = mapping_year.reduceByKey(lambda a,b: a+b)  
    result_year = reducing_year.map(calc_moy_key)

    months_mean = []
    indicator_mean = []    

    for r in sorted(result_mean.collect(), key=lambda x: x[0]):
        months_mean.append(r[0])
        indicator_mean.append(r[1])

    year_months = []
    year_values = []    

    for r in sorted(result_year.collect(), key=lambda x: x[0]):
        year_months.append(r[0])
        year_values.append(r[1]) 
            
    plt.clf()
    fig, ax1 = plt.subplots()
    
    ax1.plot(months_mean, indicator_mean, '-ro', label="Seasonal mean")
    ax1.plot(year_months, year_values, '-bo', label=str(y)+" values")
    
    ax1.set_xlim(0, 13)
    #ax1.set_ylim(min(year_values + indicator_mean), max(year_values + indicator_mean))
    ax1.set_ylim(0,15)

    ax1.set_xlabel("Month of the year")
    ax1.set_ylabel("Indicator: "+ind)
    
    ax1.yaxis.label.set_color('r')
    ax1.legend()
    
    plt.title("Comparison between year "+str(y)+" and seasonal mean\n station ("+str(lat)+","+str(lon)+")")
    plt.savefig(ind+"_"+str(y)+".png")

# replace the lon and lat if not exist and retrieve station name
longitude, latitude, station = check_position(longitude, latitude)
compute_seasonal(session, latitude, longitude, year, indicator)