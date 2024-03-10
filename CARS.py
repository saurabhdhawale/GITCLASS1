from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("CARS").getOrCreate()

df=spark.read.format("JSON").load("file:///D:/cars.json")


df.printSchema()
df1=df.filter(df["mode"]=="sedan")
df1.show()
