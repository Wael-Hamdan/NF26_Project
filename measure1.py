#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun 21 19:18:23 2019

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
ap.add_argument("-k", "--keyspace", required=False, default="penonque_project",
	help="name of the keyspace of open a session")
ap.add_argument("-t", "--table", required=False, default="metar_byposition",
	help="table in which doing requests")
args = vars(ap.parse_args())

# load the keyspace name
keyspace = args["keyspace"]
table = args["table"]

cluster = Cluster()
session = cluster.connect(keyspace)

limiteur = lambda data,limit: (d for i,d in it.takewhile(lambda w: w[0]<limit, enumerate(data)))

def euclidean_distance(p1, p2):
    return np.sqrt(np.power(p1[0] - p2[0], 2), np.power(p1[1] - p2[1], 2))

def fahrenheit_to_celcius(t):
    return (t - 32) * 5.0/9.0

def calc_moy_key(t):
    (key, (s0, s1, s2)) = t
    return (key, s1/s0)

def calc_std_key(t):
    (key, (s0, s1, s2)) = t
    return np.sqrt(s2/s0 - (s1/s0)**2)

def check_position(lon, lat):
    eps = 0.0001
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


def read_byposition(session, lat, lon, indicator):
    return session.execute("SELECT year, month, {}    \
                            FROM metar_byposition           \
                            WHERE lat = {} AND lon = {}"
                            .format(indicator, lat, lon))  

def read_bydate(session, year, month, day=None, hour=None, minute=None):
    return session.execute("SELECT * FROM metar_byposition  \
                            WHERE year = {} AND month = {}  \
                            AND day = {} AND hour = {} AND minute = {} \
                            ALLOW FILTERING".format(year, month, day, hour, minute))  


def compute_historical(session, lat, lon, indicator):
    sc = SparkContext.getOrCreate()
    res_query = limiteur(read_byposition(session, lat, lon, indicator), 100)
    rdd = sc.parallelize(res_query)
    
    rdd = rdd.filter(lambda res_query: res_query[2] is not None)
    mapping = rdd.map(lambda res_query: (str(res_query.year)+'-'+str(res_query.month), np.array([1, res_query[2], res_query[2]**2])))
    
    reducing = mapping.reduceByKey(lambda a,b: a+b)
    result = reducing.map([calc_moy_key, calc_std_key])
    return result.collect()


# taking two inputs at a time 
# longitude, latitude = input("Enter longitude and latitude: ").split() 

# replace the lon and lat if not exist and retrieve station name
#longitude, latitude, station = check_position(longitude, latitude)
# compute the statistique for these lon and lat
#result = compute_historical(session, -3.7894, 37.1897, "tmpf")

sc = SparkContext.getOrCreate()
res_query = (read_byposition(session, -3.7894, 37.1897, "tmpf"))
rdd = sc.parallelize(res_query)

rdd = rdd.filter(lambda res_query: res_query[2] is not None)
mapping_months = rdd.map(lambda res_query: (str(res_query.year)+'-'+str(res_query.month), np.array([1, np.around(res_query[2], 1), np.around(res_query[2],1)**2])))
mapping_allmonths = rdd.map(lambda res_query: (res_query.month, np.array([1, np.around(res_query[2], 1), np.around(res_query[2],1)**2])))
#for i in mapping.collect():
#    print(i[0], int(i[1][0]), np.around(i[1][1], 2), np.around(i[1][2], 2))
reducing_months = mapping_months.reduceByKey(lambda a,b: a+b)  
reducing_allmonths = mapping_allmonths.reduceByKey(lambda a,b: a+b)  
#for i in reducing.collect():
#    print(i[0], int(i[1][0]), np.around(i[1][1], 2), np.around(i[1][2], 2))

result_mean_months = reducing_months.map(calc_moy_key)
result_mean_allmonths = reducing_allmonths.map(calc_moy_key)

#result_std = reducing.map(calc_std_key)

plt.clf()
plt.rcParams['axes.prop_cycle'].by_key()['color']

months = []
values = []

for r in sorted(result_mean_allmonths.collect(), key=lambda x: x[0]):
    months.append(r[0])
    values.append(fahrenheit_to_celcius(r[1]))
plt.plot(months, values, '-ro', markersize=2, label="seasonal mean")


months = []
values = []
year, pred_year = 1, 0
temp_m, temp_v = [], []
init = True

# Retrieve values for each year
for r in sorted(result_mean_months.collect(), key=lambda x: (int(x[0].split('-')[0]),int(x[0].split('-')[1]))):
    year = int(r[0].split('-')[0])
    if init:
        pred_year = year
        temp_m.append(r[0].split('-')[0])
        temp_v.append(None)        
        init = False
    elif year != pred_year:
        months.append(temp_m)
        values.append(temp_v)
        pred_year = year
        temp_m, temp_v = [], []
        temp_m.append(r[0].split('-')[0])
        temp_v.append(None)
    # Add datas
    temp_m.append(int(r[0].split('-')[1]))    
    temp_v.append(fahrenheit_to_celcius(r[1]))    

months.append(temp_m)
values.append(temp_v)




for m,v in zip(months, values):    
    #print(m, v)
    plt.plot(m[1:], v[1:], '--o', linewidth=1, markersize=2, alpha=0.5, label=m[0])


plt.ylabel("Temperature (°Celcius)")
plt.xlabel("Month of the year")
plt.axis([0,13,0,35])
plt.grid(True, linestyle='--')
plt.title("Comparison between monthly temperature and seasonal temperature\n for station LEGR (-3.7894,37.1897)")
plt.legend()
plt.savefig('LEGR_Temp_2.png')

# =============================================================================
# 
# 
# 
# for i in result.collect():
#     print(result[0], result[1], result[2])
# 
# =============================================================================
