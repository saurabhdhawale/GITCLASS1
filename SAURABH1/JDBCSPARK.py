from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from functions import *


spark=SparkSession.builder.appName("SAmple").getOrCreate()

df=read_table(spark,"jdbc:mysql://database-1.cx82gqgyo4eg.ap-south-1.rds.amazonaws.com:3306/dev","puser","ppassword","emp","com.mysql.cj.jdbc.Driver")

#read_table.show()

#df1=df.filter(df["city"]=="New York")

df.show()

#writefile
df1.write.format("JDBC").option("url","jdbc:mysql://db1.cvtejpto9s8c.ap-south-1.rds.amazonaws.com:3306/prod").\
option("user","myuser").\
option("password","mypassword").\
option("dbtable","sau").\
option("driver","com.mysql.cj.jdbc.Driver").save()

df1.write.format("CSV").save("/sau")