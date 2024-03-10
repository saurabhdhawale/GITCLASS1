from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("Sample").getOrCreate()

df=spark.read.format("JSON").option("multiline","true").load("file:///D:/s2.json")


df1=df.withColumn("problems",explode("problems")).\
       withColumn("Diabetes",explode("problems.Diabetes")).drop("problems").\
       withColumn("labs",explode("Diabetes.labs")).\
       withColumn("missing_field",col("labs.missing_field")).drop("labs").\
       withColumn("medications",explode("Diabetes.medications")).\
       withColumn("medicationsClasses",explode("medications.medicationsClasses"))


df1.printSchema()
df1.show()





df1.printSchema()
df1.show()


