import findspark
import os
from pyspark.sql.types import StructType,FloatType
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

findspark.init("/home/phani/SPARK/spark-2.4.5-bin-hadoop2.7")

os.environ["PYSPARK_PYTHON"] = '/usr/bin/python3'

spark = SparkSession \
    .builder \
    .appName("SSKafka") \
    .getOrCreate()

bootstrap_servers = "localhost:9092"
topic = "stock"


# Construct a streaming DataFrame that reads from testtopic
df3 = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers) \
    .option("subscribe", topic) \
    .option("startingOffsets", "latest") \
    .load()

print("Printing Schema of transaction_detail_df: ")
df3.printSchema()

df4 = df3.selectExpr("CAST(value AS STRING)", "timestamp")

# Define a schema for the transaction_detail data
df3_schema = StructType() \
    .add("Open", FloatType()) \
    .add("Close", FloatType()) \
    .add("Volume", FloatType()) \
    .add("High", FloatType()) \
    .add("Low", FloatType())


print(df3.isStreaming)

#df3_schema.printSchema()

spark.conf.set("spark.sql.shuffle.partitions", "2")

df3.writeStream \
    .outputMode("append") \
    .format("console") \
    .start() \
    .awaitTermination()

