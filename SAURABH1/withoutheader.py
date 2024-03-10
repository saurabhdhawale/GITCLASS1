from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark=SparkSession.builder.appName("Without Header").getOrCreate()

header=StructType([
    StructField("eid",IntegerType(),True),
    StructField("ename",StringType(),True),
    StructField("did",IntegerType(),True),
    StructField("sal",IntegerType(),True),
    StructField("city",StringType(),True)
])


df=spark.read.format("CSV").option("inferschema","true")\
    .schema(header)\
    .load("file:///D:/emp.csv")

df.show()