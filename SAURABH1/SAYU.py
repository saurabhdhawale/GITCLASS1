from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("Spark SQL Test").getOrCreate()

#read
df=spark.read.format("CSV").option("Header","True").\
     option("delimiter",":")\
    .option("inferschema","True")\
    .load("file:///D:/emp.csv")
df.show()
#temp view
df.createOrReplaceTempView("emp")
#SQl query
df1=spark.sql("select * from emp where sal >= 30000")
#show result from df1
df1.show()

#write into lfs
df1.write.format("CSV").option("header","true").\
    option("delimiter","-").save("file:///D:/sql_op1")

