from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("Sample").getOrCreate()

df=spark.read.format("JSON").load("file:///D:/s1.json")


df1=df.withColumn("person",explode(array("person"))).\
       withColumn("address",explode(array("person.address"))).\
       withColumn("city",col("address.city")).\
       withColumn("country",col("address.country")).\
       withColumn("street",col("address.street")).\
       withColumn("age",col("person.age")).\
       withColumn("emails",explode("person.emails")).\
       withColumn("name",col("person.name")).drop("person").drop("address")

df1.printSchema()
df1.show(20,False)

df2=df1.filter(df1["emails"]=="johndoe@gmail.com").select("name","age","city","emails")
df2.show()