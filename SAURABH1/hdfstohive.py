from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark=SparkSession.builder.appName("RDBMS Data").enableHiveSupport().getOrCreate()

df=spark.read.format("CSV").option("header","true").option("inferschema","true").load("/hd/part-00000-44f2305c-92c2-473a-88fa-3e4d844011e0-c000.csv")

df.show()
df.write.saveAsTable("saurabh")