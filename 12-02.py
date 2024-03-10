from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark=SparkSession.builder.appName("json").enableHiveSupport().getOrCreate()

df=spark.read.format("JSON").option("multiline","true").load("file:///D:/sample2.json")

df1=df.withColumn("address",explode(array("address.*"))).\
    withColumn("number",explode("phoneNumbers.number")).\
    withColumn("type",explode("phoneNumbers.number")).drop("phoneNumbers")

df1.printSchema()
df1.show()

df1.write.format("CSV").save("file:///D:/sanket2.csv")