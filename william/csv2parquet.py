from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("csv2parquetApp").getOrCreate()

df = spark.read.option("header", "true").csv("/1618_full.csv")
df.write.parquet("1618_full_parquet")

#parquetFile = spark.read.parquet("2016_full_parquet")

#parquetFile.createOrReplaceTempView("parquetFile")
#everything = spark.sql("SELECT * from parquetFile limit 10")

#everything.show()


