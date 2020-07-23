from pyspark.sql.types import TimestampType, StringType, StructType, StructField
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
sc=spark.sparkContext
# Path to our 20 JSON files
inputPath = "/home/phani/Desktop/Structured_streaming/spark-streaming-sample-data"

# Explicitly set schema
schema = StructType([StructField("time", TimestampType(), True),
                      StructField("customer", StringType(), True),
                      StructField("action", StringType(), True),
                      StructField("device", StringType(), True)])

# Create DataFrame representing data in the JSON files
inputDF = (
  spark
    .read
    .schema(schema)
    .json(inputPath)
)

print(inputDF.show())

# Aggregate number of actions
actionsDF = (
    inputDF
    .groupBy(
        inputDF.action
    )
    .count()
)
actionsDF.cache()

# Create temp table named 'iot_action_counts'
actionsDF.createOrReplaceTempView("iot_action_counts")
spark.sql("select action, sum(count) as total_count from iot_action_counts group by action").show()

#To load data into a streaming DataFrame, we create a DataFrame just how we did with
# inputDF with one key difference: instead of .read, we'll be using .readStream:

# Create streaming equivalent of `inputDF` using .readStream
streamingDF = (
  spark
    .readStream
    .schema(schema)
    .option("maxFilesPerTrigger", 1)
    .json(inputPath)
)

# Stream `streamingDF` while aggregating by action
streamingActionCountsDF = (
  streamingDF
    .groupBy(
      streamingDF.action
    )
    .count()
)

# Is `streamingActionCountsDF` actually streaming?
print(streamingActionCountsDF.isStreaming)


#Now we have a streaming DataFrame, but it isn't streaming anywhere. To
# stream to a destination, we need to call writeStream() on our DataFrame
spark.conf.set("spark.sql.shuffle.partitions", "2")

# View stream in real-time
query = (
  streamingActionCountsDF
    .writeStream
    .format("console")
    .queryName("counts")
    .outputMode("complete")
    .start()
)

from time import sleep
sleep(5)