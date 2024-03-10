from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("Sample").getOrCreate()

df=spark.read.format("JSON").option("multiline","true").load("file:///D:/s2.json")


df1=df.withColumn("problems",explode("problems")).\
       withColumn("Diabetes",explode("problems.Diabetes")).\
       withColumn("labs",explode("Diabetes.labs")).\
       withColumn("missing_field",col("labs.missing_field")).\
       withColumn("medications",explode("Diabetes.medications")).\
       withColumn("medicationsClasses",explode("medications.medicationsClasses")).\
       withColumn("className",explode("medicationsClasses.className")).\
       withColumn("associatedDrug",explode("className.associatedDrug")).\
       withColumn("dose",col("associatedDrug.dose")).\
       withColumn("name",col("associatedDrug.name")).\
       withColumn("strength",col("associatedDrug.strength")). \
       withColumn("associatedDrug#2",explode("className.associatedDrug#2")).\
       withColumn("dose",col("associatedDrug#2.dose")).\
       withColumn("name",col("associatedDrug#2.name")).\
       withColumn("strength",col("associatedDrug#2.strength")).drop("associatedDrug").drop("associatedDrug#2").drop("className").drop("problems").drop("medicationsClasses")



df1.printSchema()
df1.show(100,False)


