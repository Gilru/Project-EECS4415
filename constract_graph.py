from pyspark.sql import SparkSession

#--------------------------------CONVERTION CODE------------------------------------------
spark = SparkSession \
    .builder \
    .appName("Graph") \
    .getOrCreate()

df = spark.read.option("header", "true").csv("airlines.csv")
df.write.parquet("airline_parquet")





# ------------------------------TEST----------------------------------------------
# The result of loading a parquet file is also a DataFrame.
# parquetFile = spark.read.parquet("airline_parquet")
#
# # Parquet files can also be used to create a temporary view and then used in SQL statements.
# parquetFile.createOrReplaceTempView("parquetFile")
# everything = spark.sql("SELECT * from parquetFile limit 10")
#
# everything.show()



