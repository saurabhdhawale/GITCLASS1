from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("Spark Testing").getOrCreate()

ls=[1,2,3,4,5,6,7,8,9]
rdd=spark.sparkContext.parallelize(ls)
print(rdd.collect())
print(rdd.getNumPartitions())
rdd1=rdd.repartition(10)
print(rdd1.getNumPartitions())
rdd2=rdd1.coalesce(5)
print(rdd2.getNumPartitions())