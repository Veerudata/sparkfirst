import os
import urllib.request
import ssl

import projects
from sqlalchemy.testing.pickleable import Order

data_dir = "data"
os.makedirs(data_dir, exist_ok=True)

data_dir1 = "hadoop/bin"
os.makedirs(data_dir1, exist_ok=True)

hadoop_home = os.path.abspath("hadoop")   # <-- absolute path
os.makedirs(os.path.join(hadoop_home, "bin"), exist_ok=True)


# ======================================================================================

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys
import os
import urllib.request
import ssl

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['HADOOP_HOME'] = hadoop_home
os.environ['JAVA_HOME'] = r'C:\Users\veera\.jdks\corretto-1.8.0_472'        #  <----- 🔴JAVA PATH🔴
######################🔴🔴🔴################################

#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-avro_2.12:3.5.4 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 pyspark-shell'

#
conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)

spark = SparkSession.builder.getOrCreate()

##################🔴🔴🔴🔴🔴🔴 -> DONT TOUCH ABOVE CODE -- TYPE BELOW ####################################

# from pyspark.sql.functions import *
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.getOrCreate()
#
# foodDF = spark.sparkContext.parallelize([
#     (1, "Veg Biryani"),
#     (2, "Veg Fried Rice"),
#     (3, "Kaju Fried Rice"),
#     (4, "Chicken Biryani"),
#     (5, "Chicken Dum Biryani"),
#     (6, "Prawns Biryani"),
#     (7, "Fish Birayani")
# ]).toDF(["food_id", "food_item"])
#
# ratingDF = spark.sparkContext.parallelize([
#     (1, 5),
#     (2, 3),
#     (3, 4),
#     (4, 4),
#     (5, 5),
#     (6, 4),
#     (7, 4)
# ]).toDF(["food_id", "rating"])
#
# foodDF.show()
# ratingDF.show()
#
# finaloutDF = foodDF.join(ratingDF, "food_id") \
#     .withColumn("stars out of 5", expr("repeat('*', rating)"))
#
# finaloutDF.show()
#
#
# data = [
#     ("sai",  "chn", 40),
#     ("zeyo", "hyd", 10),
#     ("sai",  "hyd", 20),
#     ("zeyo", "chn", 20),
#     ("sai",  "chn", 10),
#     ("zeyo", "hyd", 10),
# ]
#
# df = spark.createDataFrame(data, ["name", "city", "amount"])
#
# df.show()
#
# aggdf = df.groupBy("name").agg(
#                             sum("amount").alias("total"),
#                             count("amount").alias("cnt"),
#                             collect_list("amount").alias("amt_list"),
#                             collect_set("amount").alias("amt_set")
# )
# aggdf.show()
#
#
# a = [1,2,3,4]
# total = 0
# for x in a:
#     total += x
#     a[0]=total
# print(a)
#
# dataSchema= ["ContractId","ContractValue", "ContractVersion", "Place"]
# data = ([123001,1232132,1,"Bangalore"],
#         [123001,1232132,1,"Bangalore"],
#         [123001,1232132,2,"Bangalore"],
#         [123001,1232132,2,"Mumbai"],
#         [123002,1232132,1,"Bangalore"],
#         [123002,1232132,2,"Bangalore"],
#         [123002,1232132,2,"Bangalore"],
#         [123002,0,2,"Mumbai"],
#         [123001,1232132,3,"Bangalore"],
#         [123001,1232132,4,"Bangalore"],
#         [123001,1232132,4,"Bangalore"],
#         [123001,1232131,4,"Bangalore"],
#         [123001,1232133,4,"Mumbai"],
#         [123003,1232132,1,"Bangalore"])
#
# dataframe  = spark.createDataFrame( data ,dataSchema)
#
# dataframe.show()
#
# data="""
#
# {
#     "id" : 1 ,
#     "trainer" : "sai",
#     "zeyoAddress" : {
#              "permanentAddress" : "Hyderabad",
#              "temporaryAddress" : "Chennai"
#     }
# }
#
# """
#
# df = spark.read.option("multiline","true").json(sc.parallelize([data]))
# df.show()
# df.printSchema()
#
#
#
#
# flatdf = df.selectExpr(
#     "id",
#     "trainer",
#     "zeyoAddress.permanentAddress",
#     "zeyoAddress.temporaryAddress"
#
# )
#
# flatdf.show()
# flatdf.printSchema()


# data = ['A', 'b', 'g', 'd', '&', '@', '#', '%', '1','2','3','4']
# df = spark.createDataFrame(data, 'string').toDF('char')
#
# df.show()
#
# finaldf = df.withColumn(
#     "category",
#     when(col("char").rlike("^[0-9]$"), "number")
#     .when(col("char").rlike("^[A-Za-z]$"), "alphabet")
#     .otherwise("special character")
# )
#
# finaldf.show()

# df1 = spark.read.parquet("s3a://snowpark-projects/USA-Sales-Order.snappy.parquet")
# df1.show()

rdd = spark.sparkContext.parallelize(["a b", "c d"])
result = rdd.flatMap(lambda x: x.split(" ").collect())
print(result)
