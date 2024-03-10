from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("Spark SQL Test").getOrCreate()

#read
df_emp=spark.read.format("CSV").option("Header","True").\
     option("delimiter",":")\
    .option("inferschema","True")\
    .load("file:///D:/emp.csv")

df_emp.show()
df_dept=spark.read.format("CSV").option("Header","True").\
     option("delimiter",":")\
    .option("inferschema","True")\
    .load("file:///D:/dept.csv")

df_dept.show()
df_emp.createOrReplaceTempView("emp")
df_dept.createOrReplaceTempView("dept")

df_res=spark.sql(""" select eid,ename,dname from emp inner join dept on dept.did=emp.did""")

df_res.show()

df_res.write.save("file:///D:/empdept0")
df_res.write.format("CSV").save("file:///D:/empdeptcsv0")