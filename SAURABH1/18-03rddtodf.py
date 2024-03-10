from pyspark.sql import SparkSession
from pyspark.sql.types import Row
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("Without Header").getOrCreate()

rdd=spark.sparkContext.textFile("D:/log.csv")

rdd1=rdd.map(lambda x:x.replace("     ","|")).map(lambda x:x.replace(",","|")).map(lambda x:x.replace(" ","|")).map(lambda x:x.replace("/","|"))

def sep(line):
	ls=line.split("|")
	return ls[1],ls[2],ls[3],ls[4]

schema=Row("ip","visited_site","visited_page","duration")

df=rdd1.map(sep).map(lambda x:schema(*x)).toDF()

df.show()