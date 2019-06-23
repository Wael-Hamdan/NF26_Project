#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jun 23 16:18:09 2019

@author: quentin
"""

from cassandra.cluster import Cluster
import numpy as np

cluster = Cluster()
session = cluster.connect("penonque_project")

for r in session.execute("SELECT DISTINCT lat, lon FROM metar_byposition;"):
    for x in session.execute("SELECT station, lat, lon FROM metar_byposition\
                     WHERE lat = {} AND lon = {}\
                     LIMIT 1".format(r.lat, r.lon)):
        print("{},{},{}".format(x.station, np.around(x.lat,4), np.around(x.lon,4)))