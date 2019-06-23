#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jun 22 15:38:36 2019

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
    (key, (s0, s1)) = t
    return (key, s1/s0)

def read_byposition(session, lat, lon, indicator):
    return session.execute("SELECT year, month, {}    \
                            FROM metar_byposition           \
                            WHERE lat = {} AND lon = {}"
                            .format(indicator, lat, lon))  

sc = SparkContext.getOrCreate()

# Computing temperature
res_query_tmpf = read_byposition(session, -6.8292, 38.8833, "tmpf")

rdd_tmpf = sc.parallelize(res_query_tmpf)
rdd_tmpf = rdd_tmpf.filter(lambda d: d[2] is not None)
mapping_allmonths_tmpf = rdd_tmpf.map(lambda d: (d.month, np.array([1, d[2]])))
reducing_allmonths_tmpf = mapping_allmonths_tmpf.reduceByKey(lambda a,b: a+b)  
result_mean_allmonths_tmpf = reducing_allmonths_tmpf.map(calc_moy_key)

months_tmpf = []
temperatures = []

for r in sorted(result_mean_allmonths_tmpf.collect(), key=lambda x: x[0]):
    months_tmpf.append(r[0])
    temperatures.append(fahrenheit_to_celcius(r[1]))

# Computing humidity
res_query_relh = read_byposition(session, -6.8292, 38.8833, "relh")

rdd_relh = sc.parallelize(res_query_relh)
rdd_relh = rdd_relh.filter(lambda d: d[2] is not None)
mapping_allmonths_relh = rdd_relh.map(lambda d: (d.month, np.array([1, d[2]])))
reducing_allmonths_relh = mapping_allmonths_relh.reduceByKey(lambda a,b: a+b)  
result_mean_allmonths_relh = reducing_allmonths_relh.map(calc_moy_key)

months_relh = []
humidity = []

for r in sorted(result_mean_allmonths_tmpf.collect(), key=lambda x: x[0]):
    months_relh.append(r[0])
    humidity.append(float(r[1]))  

plt.clf()
fig, ax1 = plt.subplots()
fig.subplots_adjust(right=0.75)
ax2 = ax1.twinx()

ax1.plot(months_tmpf, temperatures, '-ro', label="Temperature")
ax2.bar(months_relh, humidity, color='blue', alpha=0.5, label="Humidity")

ax1.set_xlim(0, 13)
ax1.set_ylim(0, 35)
ax2.set_ylim(0, 100)

ax1.set_xlabel("Month of the year")
ax1.set_ylabel("Temperature (°Celcius)")
ax2.set_ylabel("Humidity (%)")

ax1.yaxis.label.set_color('r')
ax2.yaxis.label.set_color('b')

tkw = dict(size=4, width=1.5)
ax1.tick_params(axis='y', colors='r', **tkw)
ax2.tick_params(axis='y', colors='b', **tkw)
ax1.tick_params(axis='x', **tkw)

#lines = [p1, p2]
ax1.legend()#lines, [l.get_label() for l in lines])

plt.title("Seasonal temperature and humidity in LEBZ (-6.8292,38.8833)")
plt.savefig('Temp_Humi_Bar2.png')