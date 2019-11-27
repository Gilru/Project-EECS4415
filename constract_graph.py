from pyspark.sql import SparkSession



spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()


df = spark.read.load("airlines.csv",
                     format="csv", sep=":", inferSchema="true", header="true")

df = df.withColumnRenamed("IATA_CODE","IATACODE")
df.write.parquet("airline.parquet")



