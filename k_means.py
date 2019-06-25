import os
import argparse
from mpl_toolkits.basemap import Basemap
import matplotlib.pyplot as plt
from cassandra.cluster import Cluster
from pyspark import SparkContext
import numpy as np
import itertools as it

ap = argparse.ArgumentParser()
ap.add_argument("-Y","--year",required=True,help="year")
ap.add_argument("-M","--month",required=True,help="month")
ap.add_argument("-D1","--day1",required=True, help="first day of the period")
ap.add_argument("-D2","--day2",required=True, help="first day of the period")
ap.add_argument("-k","--k_means",required=True,help="number of clusters")

args = vars(ap.parse_args())
Y = int(args["year"])
M = int(args["month"])
D1 = int(args["day1"])
D2 = int(args["day2"])
K = int(args["k_means"])

keyspace = 'penonque_project'

cluster = Cluster()
# open session
session = cluster.connect(keyspace)

limiteur = lambda data,limit: (d for i,d in it.takewhile(lambda w: w[0]<limit, enumerate(data)))

def calc_moy_key(t):
    (key, (s0, s1, s2, s3, s4)) = t
    return (key, s1/s0, s2/s0, s3/s0, s4/s0)


def euclidean_distance(p1, p2):
    # /!\ it considers arrays only
    return np.sqrt(np.sum(np.power(p1 - p2, 2)))


def read_byday(session, year, month, day1, day2):
    return session.execute("SELECT lat,lon,tmpf,alti,relh,sknt \
                            FROM metar_bydate \
                            WHERE year={} and month={} \
                                and day >= {} and day <= {}"
                            .format(year,month,day1, day2))    

def read_bymonth(session, year, month):
    return session.execute("SELECT lat,lon,tmpf,alti,relh,sknt \
                            FROM metar_bydate \
                            WHERE year={} and month={}"
                            .format(year,month))    
                            

#indicators of interest for k-means "tmpf","dwpf","alti"
def compute_means(session, year, month, day1, day2):
    if day1 == -1 or day2 == -1:
        print("[Error] ") # Next do bymonth
    else:
        result_set = read_byday(session, year, month, day1, day2)

    sc = SparkContext.getOrCreate()        
    rdd = sc.parallelize(result_set)
    for k in range(2,6):
        rdd = rdd.filter(lambda d: d[k] is not None)
    mapping = rdd.map(lambda d: (str(np.around(d.lat, 4))+','+str(np.around(d.lon,4)), np.array([1, d[2], d[3], d[4], d[5]])))
    reducing = mapping.reduceByKey(lambda a,b: a+b)  
    result = reducing.map(calc_moy_key)

    return result.collect()
    

means = np.array([.0]*4)
stds = np.array([.0]*4)
count = 0

dataset = compute_means(session, Y, M, D1, D2)

for r in dataset:
    count += 1
    means += r[1:]
    stds += np.power(r[1:],2)        

means = means/count
stds = np.sqrt(stds/count - np.power(means,2))

n = 4       #number of features
n_iter = 100    #number of iterations

#initialize the cendtroids randomly from the data points
centroids = np.zeros((K,n))

for i,x in enumerate(limiteur(dataset, K)):
    centroids[i] = x[1:]
 
for i in range(n_iter):
    new_centroids = np.zeros((K,n))
    counter = np.array([0]*K)
 
    for x in dataset:  
        x_norm = (x[1:]-means)/stds
        eucli_dist = [.0]*K
        for k in range(K):
            eucli_dist[k] = euclidean_distance(x_norm, (centroids[k]-means)/stds)
        cluster = np.argmin(eucli_dist)
        new_centroids[cluster] += x[1:]
        counter[cluster] += 1
    
    for k in range(K):
        centroids[k] = new_centroids[k]/counter[k]
    

colors = ['red','blue','green','purple','brown','orange','pink','yellow']
labels = ['cluster_'+str(k) for k in range(1,K+1)]

#plot coordinates of stations (lat et long)
map = Basemap(llcrnrlon=-18.87,llcrnrlat=26.75,urcrnrlon=5.34,urcrnrlat=43.8,
         resolution='i', projection='tmerc', lat_0 = 39.5, lon_0 = -3.25)

map.drawmapboundary(fill_color='aqua')
map.fillcontinents(color='#cc9955',lake_color='aqua')
map.drawcoastlines()

#map.drawmapscale(-0., 35.8, -3.25, 39.5, 500, fontsize = 14)
inertie_totale = 0

for x in dataset:
    x_norm = (x[1:]-means)/stds
    eucli_dist = [.0]*K
    for k in range(K):
        eucli_dist[k] = euclidean_distance(x_norm, (centroids[k]-means)/stds)
    cluster = np.argmin(eucli_dist)
    inertie_totale += eucli_dist[cluster]
    lat, lon = x[0].split(',')
    x,y = map(np.around(float(lat),2),np.around(float(lon),2))
    map.plot(x,y,'o',color=colors[cluster],markersize=6)
    

print("[INERTIE_TOTALE : {}]".format(inertie_totale))

if os.path.isfile("k_means_"+str(Y)+"-"+str(M)+"-("+str(D1)+","+str(D2)+")_K"+str(K)+".png"):
   os.remove("k_means_"+str(Y)+"-"+str(M)+"-("+str(D1)+","+str(D2)+")_K"+str(K)+".png")   # Opt.: os.system("rm "+strFile)
plt.savefig("k_means_"+str(Y)+"-"+str(M)+"-("+str(D1)+","+str(D2)+")_K"+str(K)+".png")

