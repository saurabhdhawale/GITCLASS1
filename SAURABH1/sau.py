from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("Spark SQl Testing").getOrCreate()

df_emp1=spark.read.format("CSV").option("header","true") \
        .option("delimiter",",").option("inferschema","true").load("file:///D:/emp1.csv")
df_emp1.show()

df_dept1=spark.read.format("CSV").option("header","true")\
        .option("delimiter",",").option("inferschema","true").load("file:///D:/dept1.csv")
df_dept1.show()

df_emp1.createOrReplaceTempView("emp1")
df_dept1.createOrReplaceTempView("dept1")

df_res=spark.sql("""select eid,ename,dname from emp1 inner join dept1 on dept1.did=emp1.did""")
df_res.show()




