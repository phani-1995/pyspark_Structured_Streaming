from pyspark.sql.types import TimestampType, StringType, StructType, StructField
import datetime
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
sc=spark.sparkContext
inputPath = "/home/phani/Downloads/users.clean.json"

schema = StructType([StructField("userName", StringType(), True),
                     StructField("jobs", StringType(), True),
                     StructField("currentPlace", StringType(), True),
                     StructField("previousPlaces", StringType(), True),
                     StructField("education", StringType(), True),
                     StructField("gPlusUserId", StringType(), True)])

 #Create DataFrame representing data in the JSON files
inputDF = (
  spark
    .read
    .schema(schema)
    .json(inputPath)
)

print(inputDF.show())

