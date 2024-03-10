from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("SAmple").getOrCreate()

df=spark.read.format("JSON").option("multiline","true").load("file:///D:/sample_mul.json")

df.show()
