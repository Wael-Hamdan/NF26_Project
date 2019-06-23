import os
import argparse
from mpl_toolkits.basemap import Basemap
import matplotlib.pyplot as plt
from cassandra.cluster import Cluster
from cassandra.query import named_tuple_factory
from pyspark import SparkContext
import pandas as pd
import numpy as np
import itertools as it

ap = argparse.ArgumentParser()
ap.add_argument("-Y","--year",required=True,help="year")
ap.add_argument("-M","--month",required=True,help="month")
ap.add_argument("-k","--k_kmeans",required=True,help="number of clusters")

args = vars(ap.parse_args())
Y = int(args["year"])
M = int(args["month"])
K = int(args["k_kmeans"])

keyspace = 'penonque_project'

cluster = Cluster()
# open session
session = cluster.connect(keyspace)

limiteur = lambda data,limit: (d for i,d in it.takewhile(lambda w: w[0]<limit, enumerate(data)))

def calc_moy_key(t):
    (key, (s0, s1, s2, s3, s4)) = t
    return (key, s1/s0, s2/s0, s3/s0, s4/s0)


#indicators of interest for k-means "tmpf","dwpf","alti"
def compute_kmeans(session, year, month, nb_clusters):
    result_set = session.execute(f"select lon,lat,tmpf,alti,relh, sknt from metar_bydate where year={Y} and month={M};")    
    sc = SparkContext.getOrCreate()
    rdd = sc.parallelize(limiteur(result_set, 100))
    rdd = rdd.filter(lambda d: d[k] is not None for k in range(6))
    mapping = rdd.map(lambda d: (str(np.around(d.lat, 4))+','+str(np.around(d.lon,4)), np.array([1, d[2], d[3], d[4], d[5]])))
    reducing = mapping.reduceByKey(lambda a,b: a+b)  
    result = reducing.map(calc_moy_key)

    for r in result.collect():
        print(r)
    
# labels = ['lon','lat','tmpf','dwpf','alti']
# dataset = pd.DataFrame.from_records(rows,columns=labels)
# X = dataset.iloc[:,[2,4]].values
#
# m=X.shape[0] #number of examples
# n=X.shape[1] #number of features
#
# n_iter = 100
#
# #initialize the cendtroids randomly from the data points
#
# Centroids = np.array([]).reshape(n,0)
#
# for i in range(k):
#     rand=rd.randint(0,m-1)
#     Centroids=np.c_[Centroids,X[rand]]
# =============================================================================
# 
# n_iter = 100
# 
# centroids = random.sample(rows,len(rows)) #to do : garder que colonnes des variables quantitatives de rows
# 
# for i in range(n_iter):
#     new_centroids = [_,_,_]
#     count = [0,0,0]
#     for x in rows:  #to do : garder que colonnes des variables quantitatives de rows
#         EuclidianDistance=[]
#         for k in range(K):
#             tempDist=np.sum((x-centroids[0,k])**2)
#             EuclidianDistance=np.c_[EuclidianDistance,tempDist]
#         C=np.argmin(EuclidianDistance,axis=1)
#         newcentroids[C]+=x
#         count[C]+=1
# result = {}
# for k in range(K):
#         result += {k:[new_centroids[k],count[k]]}
# 
# colors=['red','blue','green']
# labels=['cluster1','cluster2','cluster3']
# 
# =============================================================================
# Output ={}
#
# for i in range(n_iter):
#
#       EuclidianDistance=np.array([]).reshape(m,0)
#       for k in range(K):
#           tempDist=np.sum((X-Centroids[:,k])**2,axis=1)
#           EuclidianDistance=np.c_[EuclidianDistance,tempDist]
#       C=np.argmin(EuclidianDistance,axis=1)+1
#
#       Y={}
#       for k in range(K):
#           Y[k+1]=np.array([]).reshape(2,0)
#       for i in range(m):
#           Y[C[i]]=np.c_[Y[C[i]],X[i]]
#
#       for k in range(K):
#           Y[k+1]=Y[k+1].T
#
#       for k in range(K):
#           Centroids[:,k]=np.mean(Y[k+1],axis=0)
#       Output=Y
#
# colors=['red','blue','green']
# labels=['cluster1','cluster2','cluster3']
#
# #faire le lien entre clusters et lignes de dataset initial pour mettre la bonne couleur sur chaque station
#


#plot coordinates of stations (lat et long)
# =============================================================================
# map = Basemap(llcrnrlon=-10.07,llcrnrlat=35.67,urcrnrlon=5.34,urcrnrlat=43.8,
#          resolution='i', projection='tmerc', lat_0 = 39.5, lon_0 = -3.25)
# 
# map.drawmapboundary(fill_color='aqua')
# map.fillcontinents(color='#cc9955',lake_color='aqua')
# map.drawcoastlines()
# 
# map.drawmapscale(-7., 35.8, -3.25, 39.5, 500, barstyle='fancy')
# 
# map.drawmapscale(-0., 35.8, -3.25, 39.5, 500, fontsize = 14)
# 
# lons = []
# lats = []
# #labels = []
# for r in rows:
# #    if(r[2] is not None):
#     lons.append(r[0])
#     lats.append(r[1])
# #        labels.append(round(float(r[2]),2))
# x,y = map(lats,lons)
# map.plot(x,y,'o',markersize=6)
# 
# #for label,xpt,ypt in zip(labels,x,y):
# #        plt.text(xpt,ypt,label)
# 
# plt.show()
# if os.path.isfile("k_means.png"):
#    os.remove("k_means.png")   # Opt.: os.system("rm "+strFile)
# plt.savefig("k_means.png")
# 
# =============================================================================


compute_kmeans(session, 2012, 3, 3)