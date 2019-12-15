from pyspark.sql import SparkSession
from graphframes import *
from pyspark.sql.types import *

spark = SparkSession \
    .builder \
    .appName("Graph") \
    .getOrCreate()


def clean_data(df):
    df = df.withColumn("DEP_DELAY", df["DEP_DELAY"].cast(DoubleType()))
    df = df.withColumn("ARR_DELAY", df["ARR_DELAY"].cast(DoubleType()))

    return df


def getAllAirports(df):
    all_airports_vertices = df.select('ORIGIN_AIRPORT_ID', 'ORIGIN_CITY_NAME').distinct()
    return all_airports_vertices.withColumnRenamed('ORIGIN_AIRPORT_ID', "id")


def getAllTrips(df):
    all_trip_edges = df.select('ORIGIN_AIRPORT_ID', 'ORIGIN_CITY_NAME', 'DEST_AIRPORT_ID', 'DEST_CITY_NAME',
                               'DEP_DELAY', "ARR_DELAY", 'CARRIER_DELAY', 'WEATHER_DELAY', 'OP_UNIQUE_CARRIER',
                               'QUARTER')
    return all_trip_edges.withColumnRenamed("ORIGIN_AIRPORT_ID", "src").withColumnRenamed("DEST_AIRPORT_ID", "dst")


def city_with_most_delay(tripGraph):
    delay = tripGraph.edges.groupBy("src", "dst", "ORIGIN_CITY_NAME", "DEST_CITY_NAME").avg("ARR_DELAY").sort(
        "avg(ARR_DELAY)", ascending=False)
    delay.show()


def airline_with_most_delay(tripGraph):
    delay = tripGraph.edges.groupBy('OP_UNIQUE_CARRIER').avg("ARR_DELAY").sort("avg(ARR_DELAY)", ascending=False)
    delay.show()


def time_with_high_prob_delay(tripGraph):
    delay = tripGraph.edges.groupBy('QUARTER').avg("ARR_DELAY").sort("avg(ARR_DELAY)", ascending=False)
    delay.show()


def cause_of_the_delay(tripGraph):
    pass


# ------------------------------TEST----------------------------------------------
# The result of loading a parquet file is also a DataFrame.
df = spark.read.option("header", "true").csv("2016_full.csv")
df = clean_data(df)

all_airports_vertices = getAllAirports(df)
all_trip_edges = getAllTrips(df)

all_airports_vertices.head()
all_trip_edges.head()

tripGraph = GraphFrame(all_airports_vertices, all_trip_edges)
# tripGraph.edges.head()
# city_with_most_delay(tripGraph)
# airline_with_most_delay(tripGraph)
time_with_high_prob_delay(tripGraph)