from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark=SparkSession.builder.appName("Without Header").getOrCreate()

header=StructType([
    StructField("ID",IntegerType(),True),
    StructField("Name",StringType(),True),
    StructField("Age",IntegerType(),True),
    StructField("City",StringType(),True),
    StructField("Salary",IntegerType(),True),
    StructField("__corrupt_record",StringType(),True)
])

#df=spark.read.format("CSV").option("inferschema","true")\
    #.schema(header)\
    #.option("mode","FAILFAST")\
    #.load("file:///D:/population_data.csv")

df=spark.read.format("CSV").option("inferschema","true")\
    .schema(header)\
    .option("mode","DROPMALFORMED")\
    .load("file:///D:/population_data.csv")


#df=spark.read.format("CSV").option("inferschema","true")\
 #   .schema(header)\
  #  .option("mode","PERMISSIVE")\
   # .option("columnNameOfCorruptRecord","__corrupt_record")\
    #.load("file:///D:/population_data.csv")

df.show()

#df_v=df.filter(df["__corrupt_record"].isNull()).drop("__corrupt_record")

#df_v.show()

#df_i=df.filter(df["__corrupt_record"].isNotNull())

#df_i.show()
