from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Testing").getOrCreate()

df_t1 = spark.read.format("csv").option("header", "true").load("file:///D:/t1.csv")
df_t2 = spark.read.format("csv").option("header", "true").load("file:///D:/t2.csv")

df_t1.show()
df_t2.show()

df_t1.createOrReplaceTempView("t1")
df_t2.createOrReplaceTempView("t2")

df_res=spark.sql("select * from t1 union all select * from t2")
df_res1=spark.sql("select * from t1 union select * from t2")
df_res2=spark.sql("select * from t1 intersect select * from t2")
df_res3=spark.sql("select * from t1 minus select * from t2")
df_res4=spark.sql("select * from t2 minus select * from t1")

df_res.show()
df_res1.show()
df_res2.show()
df_res3.show()
df_res4.show()

df_res = df_res.repartition(3)
df_res1 = df_res1.repartition(3)
df_res2 = df_res2.repartition(3)
df_res3 = df_res3.repartition(3)
df_res4 = df_res4.repartition(3)

df_res.write.format("csv").save("file:///D:/t1")
df_res1.write.format("csv").save("file:///D:/t2")
df_res2.write.format("csv").save("file:///D:/t3")
df_res3.write.format("csv").save("file:///D:/t4")
df_res4.write.format("csv").save("file:///D:/t5")