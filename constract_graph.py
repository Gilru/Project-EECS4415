from pyspark.sql import SparkSession



spark = SparkSession \
    .builder \
    .appName("Graph") \
    .getOrCreate()


df = spark.read.load("airlines.csv",
                     format="csv", sep=":", inferSchema="true", header="true")

df = df.withColumnRenamed("IATA_CODE","IATACODE")
df.write.parquet("airline.parquet")



