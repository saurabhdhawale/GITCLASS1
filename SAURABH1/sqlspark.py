from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("Spark SQl Testing").getOrCreate()

df=spark.read.format("CSV").option("header","true")\
    .load("file:///D:/emp1.csv")
df.show()