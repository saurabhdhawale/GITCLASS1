from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark=SparkSession.builder.appName("json").enableHiveSupport().getOrCreate()

df=spark.read.format("JSON").option("multiline","true").load("file:///D:/csvjson1.json")

df.printSchema()
df.show()