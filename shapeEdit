import os
from osgeo import ogr
import psycopg2
import json
import sqlalchemy
import time

### CONNECTING DATABASE ###

_config_lokal =json.loads(open('config_lokal').read())


_lokal_engine_slave = sqlalchemy.create_engine('postgresql+psycopg2://' + _config_lokal["user"] + ':' +
                                               _config_lokal["password"] + '@' + _config_lokal["host"] + ':' +
                                               _config_lokal["port"] + '/' + _config_lokal["database"] + '')


dict = {'residential':'1',
'tertiary':'2',
'secondary':'3',
'primary':'4',
'trunk':'5',
'trunk_link':'6',
'unclassified':'7',
'service':'8',
'pedestrian':'9',
'cycleway':'10',
'secondary_link':'11',
'primary_link':'12',
'tertiary_link':'13',
'footway':'14',
'motorway_link':'15',
'motorway':'16',
'living_street':'17',
'construction':'18',
'steps':'19',
'None':'20'
}

st_dict = { }


#### READING SHAPEFILE AND CREATING PGROUTING TABLE  ###


dosyam = r"C:\Users\meltem.erdol\Desktop\osm_work\osm_divided3.shp"

driver = ogr.GetDriverByName('ESRI Shapefile')

dataSource = driver.Open(dosyam, 0)

layer = dataSource.GetLayer()

with open("route_data.csv","w") as yazilan:

    for feature in layer:

        highway = feature.GetField("highway")
        id = feature.GetField("mel")
        tag = feature.GetField("other_tags")

        geom = feature.GetGeometryRef()
        X1 = geom.GetPoint(0)[0]
        Y1= geom.GetPoint(0)[1]

        X2 = geom.GetPoint(geom.GetPointCount()-1)[0]
        Y2 = geom.GetPoint(geom.GetPointCount() - 1)[1]



        if tag is not None:
            if 'oneway' in tag:
                sonuc ="(" + str(id) + "," + str(X1) +","+ str(Y1)+","+ str(X2)+","+str(Y2)+ "," + str("0,1")+str(",'FT',")+ dict[str(highway)]+","+dict[str(highway)] +")"
                yazilan.write(sonuc + "\n")
                print(sonuc)
            else:
                sonuc ="("+ str(id) + "," + str(X1) +","+ str(Y1)+","+ str(X2)+","+str(Y2)+"," + str("1,1")+str(",'B',")+ dict[str(highway)]+","+dict[str(highway)]+")"
                yazilan.write(sonuc + "\n")
                print(sonuc)
        else:
            sonuc ="(" + str(id) + "," + str(X1) +","+ str(Y1)+","+ str(X2)+","+str(Y2)+ "," + str("1,1")+str(",'B',")+ dict[str(highway)]+","+dict[str(highway)]+")"
            yazilan.write(sonuc + "\n")
            print(sonuc)


        query = "INSERT INTO rota.edges (id,x1,y1,x2,y2,cost,reverse_cost,dir,category_id,reverse_category_id) VALUES "+sonuc

        _lokal_engine_slave.execute(query)


query2 = "UPDATE rota.edges SET the_geom = ST_SETSRID(st_makeline(st_point(x1,y1),st_point(x2,y2)),4326);"

_lokal_engine_slave.execute(query2)
time.sleep(3)

#CREATING TOPOLOGY
query3 = "SELECT pgr_createTopology('rota.edges',0.000001);"
_lokal_engine_slave.execute(query3)
