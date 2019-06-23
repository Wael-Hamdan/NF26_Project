import os
import argparse
from mpl_toolkits.basemap import Basemap
import matplotlib.pyplot as plt
import cassandra
import cassandra.cluster
from cassandra.query import named_tuple_factory

ap = argparse.ArgumentParser()
ap.add_argument("-i","--indicator",required=True,help="name of the meteorologic indicator")
ap.add_argument("-Y","--year",required=True,help="year")
ap.add_argument("-M","--month",required=True,help="month")
ap.add_argument("-D","--day",required=True,help="day")
ap.add_argument("-ho","--hour",required=True,help="hour")
ap.add_argument("-m","--minute",required=True,help="minute")
ap.add_argument("-o","--outfilename",required=True,help="output fig filename")

args = vars(ap.parse_args())
Y = int(args["year"])
M = int(args["month"])
D = int(args["day"])
h = int(args["hour"])
m = int(args["minute"])

keyspace = 'penonque_project'

# load the keyspace name
indicator = args["indicator"]
outfile = args["outfilename"]


cluster = cassandra.cluster.Cluster(['localhost'])
session=cluster.connect(keyspace)
session.row_factory = named_tuple_factory
rows = session.execute(f"select lon,lat,{indicator} from metar_bydate where year={Y} and month={M} and day={D} and hour={h} and minute={m};")

#plot les points des stations (lat et long) et afficher valeur de l'indicateur à côté
map = Basemap(llcrnrlon=-18.87,llcrnrlat=26.75,urcrnrlon=5.34,urcrnrlat=43.8,
         resolution='i', projection='tmerc', lat_0 = 39.5, lon_0 = -3.25)

map.drawmapboundary(fill_color='aqua')
map.fillcontinents(color='#cc9955',lake_color='aqua')
map.drawcoastlines()



lons = []
lats = []
labels = []
for r in rows:
    if(r[2] is not None):
        lons.append(r[0])
        lats.append(r[1])
        labels.append(round(float(r[2]),2))
x,y = map(lats,lons)
map.plot(x,y,'o',markersize=6)

for label,xpt,ypt in zip(labels,x,y):
        plt.text(xpt,ypt,label)

plt.show()
if os.path.isfile(outfile):
   os.remove(outfile)   # Opt.: os.system("rm "+strFile)
plt.savefig(outfile)
