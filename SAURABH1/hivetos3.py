from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark=SparkSession.builder.appName("RDBMS Data").enableHiveSupport().getOrCreate()

df=spark.sql("select * from cust_saurabh where fname ='john'")

df.show()

df.write.format("json").save("s3://adity77/DSA/cust_saurabh")