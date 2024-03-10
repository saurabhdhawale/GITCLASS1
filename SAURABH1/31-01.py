from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("Spark SQL Test").getOrCreate()

df_product=spark.read.format("CSV").option("header","true").option("delimiter",",").option("inferschema","true")\
    .load("file:///D:/product.csv")

df_product.show()

df_order=spark.read.format("CSV").option("header","true").option("delimiter",",").option("inferschema","true")\
    .load("file:///D:/order.csv")

df_order.show()

df_product.createOrReplaceTempView("product")
df_order.createOrReplaceTempView("order")


df_res=spark.sql("""select ProductName,Price,CustomerName,OrderID from product inner join order on Product.ProductID=order.ProductID""")

df_res.show()

df_res.write.save("file:///D:/saurabh")
df_res.write.format("CSV").save("file:///D:/saurabh1")