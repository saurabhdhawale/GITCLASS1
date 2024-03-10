from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("BROADCAST JOIN").getOrCreate()

df_emp1=spark.read.format("CSV").option("header","true") \
        .option("delimiter",",").option("inferschema","true").load("file:///D:/emp1.csv")
df_emp1.show()

df_dept1=spark.read.format("CSV").option("header","true")\
        .option("delimiter",",").option("inferschema","true").load("file:///D:/dept1.csv")
df_dept1.show()

df_emp1.createOrReplaceTempView("emp1")
df_dept1.createOrReplaceTempView("dept1")

df_res=spark.sql("select /*+ BROADCAST (dept) */ eid,ename,dname from emp1 inner join dept1 on emp1.did=dept1.did")
df_res.show()

