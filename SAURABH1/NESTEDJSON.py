from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark=SparkSession.builder.appName("SAmple").getOrCreate()

df=spark.read.format("JSON").option("multiline","true").load("file:///D:/nes.json")

df1=df.withColumn("Source",explode(array("source.*"))).\
    withColumn("id",col("source.id")).\
    withColumn("ip",col("source.ip")).\
    withColumn("temp",col("source.temp")).\
    withColumn("description",col("source.description")).\
    withColumn("c02_level",col("source.c02_level")).\
    withColumn("lat",col("source.geo.lat")).\
    withColumn("long",col("source.geo.long")).drop("source")


df.printSchema()
#df1.show()

