from pyspark.sql import  SparkSession
spark = SparkSession.builder.appName("SQL Testing").getOrCreate()

df_emp24=spark.read.format("CSV").option("header","true").option("delimiter","|").option("inferschema","true").load("file:///D:/emp24.csv")
df_emp24.show()

df_did=spark.read.format("CSV").option("header","true").option("delimiter","|").option("inferschema","true").load("file:///D:/did.csv")
df_did.show()

df_emp24.createOrReplaceTempView("emp24")
df_did.createOrReplaceTempView("did")

df_res=spark.sql("""select eid,ename,dname from emp24 inner join did on did.did = emp24.did""")

df_res.show()

df_res.write.save("file:///D:/emp24did")
df_res.write.format("CSV").save("file:///D:/empdidcsv")
