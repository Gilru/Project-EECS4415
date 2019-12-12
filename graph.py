from pyspark.sql import SparkSession
from graphframes import *

spark = SparkSession \
    .builder \
    .appName("Graph") \
    .getOrCreate()

def getAllAirports(df):
  all_airports_vertices = df.select('ORIGIN_AIRPORT_ID','ORIGIN_CITY_NAME').distinct()
  return all_airports_vertices.withColumnRenamed('ORIGIN_AIRPORT_ID', "id")


def getAllTrips(df):
  all_trip_edges = df.select('ORIGIN_AIRPORT_ID','ORIGIN_CITY_NAME','DEST_AIRPORT_ID','DEST_CITY_NAME','DEP_DELAY','ARR_DELAY','CARRIER_DELAY','WEATHER_DELAY')
  return all_trip_edges.withColumnRenamed("ORIGIN_AIRPORT_ID","src").withColumnRenamed("DEST_AIRPORT_ID","dst")




# ------------------------------TEST----------------------------------------------
# The result of loading a parquet file is also a DataFrame.
df = spark.read.option("header", "true").csv("2016_full.csv")


all_airports_vertices = getAllAirports(df)
all_trip_edges = getAllTrips(df)

all_airports_vertices.head()
all_trip_edges.head()


tripGraph = GraphFrame(all_airports_vertices,all_trip_edges)
#tripGraph.vertices.head()
#tripGraph.edges.head()
tripGraph.degrees.show()

