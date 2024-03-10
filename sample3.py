from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark=SparkSession.builder.appName("json").enableHiveSupport().getOrCreate()

df=spark.read.format("JSON").option("multiline","true").load("file:///D:/sample4.json")

df1=df.withColumn("people",explode(df["people"])).\
       withColumn("age",col("people.age")).\
       withColumn("firstName",col("people.firstName")).\
       withColumn("gender",col("people.gender")).\
       withColumn("lastName",col("people.lastName")).\
       withColumn("number",col("people.number")).drop("people")
df1.printSchema()
df1.show()

df1.write.format("CSV").save("file:///D:/sanket.csv")