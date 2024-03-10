from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("BROADCAST JOIN").getOrCreate()

df_emp1=spark.read.format("CSV").option("header","true") \
        .option("delimiter",",").option("inferschema","true").load("file:///D:/emp1.csv")
df_emp1.show()

df_dept1=spark.read.format("CSV").option("header","true")\
        .option("delimiter",",").option("inferschema","true").load("file:///D:/dept1.csv")
df_dept1.show()

df_res=df_emp1.join(broadcast(df_dept1),"did","inner")

df_res.show()

