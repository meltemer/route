import psycopg2
import json
import sqlalchemy
import time
import pandas

_config_lokal = json.loads(open('config_lokal').read())


_lokal_engine_slave = sqlalchemy.create_engine('postgresql+psycopg2://' + _config_lokal["user"] + ':' +
                                               _config_lokal["password"] + '@' + _config_lokal["host"] + ':' +
                                               _config_lokal["port"] + '/' + _config_lokal["database"] + '')

lon = "28.791877"
lat = "41.106480"

midlon = "28.789840"
midlat = "41.102405"

lon2 = "28.789999"
lat2 = "41.103916"


query1 = "SELECT \
  id, \
  st_distance(the_geom,(ST_SETSRID(st_point("+str(lon)+","+str(lat)+"),4326))) as mesafe \
 FROM rota.edges_vertices_pgr vertex \
 WHERE st_intersects(the_geom, st_buffer(ST_SETSRID(st_point("+str(lon)+","+str(lat)+"),4326), 0.0005)) \
  order by mesafe LIMIT 1;"

df = pandas.read_sql_query(con=_lokal_engine_slave, sql=query1)
startPoint = df["id"].get(0)

query2 = "SELECT \
  id, \
  st_distance(the_geom,(ST_SETSRID(st_point("+str(lon2)+","+str(lat2)+"),4326))) as mesafe \
 FROM rota.edges_vertices_pgr vertex \
 WHERE st_intersects(the_geom, st_buffer(ST_SETSRID(st_point("+str(lon2)+","+str(lat2)+"),4326), 0.0005)) \
 order by mesafe LIMIT 1;"

df = pandas.read_sql_query(con=_lokal_engine_slave, sql=query2)
endPoint = df["id"].get(0)

query3 = "SELECT edge from pgr_dijkstraVia('SELECT id,source,target,cost FROM rota.edges',\
         ARRAY["+str(startPoint)+","+str(endPoint)+"]) djk INNER JOIN rota.edges ed ON djk.edge = ed.id ;"

df = pandas.read_sql_query(con=_lokal_engine_slave, sql=query3)
print(df)

