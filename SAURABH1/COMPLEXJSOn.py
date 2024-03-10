from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark=SparkSession.builder.appName("SAmple").getOrCreate()

df=spark.read.format("JSON").option("multiline","true").load("file:///D:/persons.json")

df1=df.withColumn("persons",explode(df["persons"])).\
    withColumn("cars",explode("persons.cars")).\
    withColumn("models",explode("cars.models")).\
    selectExpr("persons.name","persons.age","cars.name as cars_name","models")

#df2=df1.groupBy("name").count()
df.printSchema()
#df2.show()