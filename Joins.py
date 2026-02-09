import os
import urllib.request
import ssl

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
#====================================================================================

# df = spark.createDataFrame([("Renuka1992@gmail.com", "9856765434"), ("anbuarasu@hotmail.com", "9844567788")], ["email", "mobile"])
# df.show()
# 
# 
# data = [
#     (1, "raj"),
#     (2, "ravi"),
#     (3, "sai"),
#     (5, "rani")
# ]
# 
# cust = spark.createDataFrame(data, ["id","name"])
# cust.show()
# 
# data = [
#     (1, "mouse"),
#     (3, "mobile"),
#     (7, "laptop")
# ]
# prod = spark.createDataFrame(data, ["id", "product"])
# prod.show()
# 
# 
# innerjoin = cust.join(prod, ["id"], "inner")
# innerjoin.show()
# 
# leftjoin = cust.join(prod, ["id"], "left")
# leftjoin.show()
# 
# rightjoin = cust.join(prod, ["id"],"right")
# rightjoin.show()
# 
# fulljoin = cust.join(prod, ["id"], "full")
# fulljoin.show()
# 
# prodids = prod.rdd.map(lambda x : x["id"]).collect()
# print(prodids)
# 
# filterdf = cust.filter(~cust.id.isin(prodids))
# print("=========LIST COMPREHENSION WAY FILTER===========")
# filterdf.show()
# 
# leftantijoin = cust.join(prod, ["id"], "leftanti")
# print("=========LEFT ANTI JOIN===========")
# leftantijoin.show()
# 
# difcoldf = prod.withColumnRenamed("id", "tid")#.select("*")
# print("=========COLUMN RENAME===========")
# difcoldf.show()
# 
# joindf = cust.join(difcoldf, cust.id == difcoldf.tid, "inner")#.drop("tid")
# print("=========DIFF COLUMN NAME JOIN===========")
# joindf.show()
#====================================================================================
# data = [
#     ("sai", "chennai", 10),
#     ("zeyo", "hyderabad", 15),
#     ("sai", "chennai", 15),
#     ("zeyo", "hyderabad", 10),
#     ("zeyo", "chennai", 5),
#     ("sai", "hyderabad", 10),
#     ("zeyo", "chennai", 20),
#     ("sai", "hyderabad", 10)
# ]
#
# df = spark.createDataFrame(data, ["name","city",'amount'])
# df.show()
#
# aggdf = df.groupBy('name').agg(
#                             sum('amount').alias('total'),
#                             count('amount').alias('count'),
#                             collect_list('amount').alias('amount_collect'),
#                             collect_set('amount').alias('amount_distinct')
# )
# aggdf.show()
#
# mulaggdf = df.groupBy("name","city").agg(
#                                         sum("amount").alias("total")
# )
# mulaggdf.show()
#====================================================================================


# data = [
#
#     ("DEPT1", 1000),
#     ("DEPT1", 500),
#     ("DEPT1", 700),
#     ("DEPT2", 400),
#     ("DEPT2", 200),
#     ("DEPT3", 200),
#     ("DEPT3", 500)
# ]
#
# df = spark.createDataFrame(data, ["department", "salary"])
# df.show()

# df.write.format("csv").mode("overwrite").save("file:///d:/salary_data")
# df.write.format("csv").mode("overwrite").save("d:/salary_data")
# df.write.csv("salary_data", header=True, mode="overwrite")

# from pyspark.sql.functions import *
# from pyspark.sql.window import Window
#
# createwindow = Window.partitionBy("department"). orderBy(col("salary").desc())
#
# denserankdf = df.withColumn("denserank", dense_rank().over(createwindow)).filter("denserank = 2").drop("denserank").withColumnRenamed('salary', 'second_highest_salary')
# denserankdf.show()



# fildf = denserankdf.filter("denserank = 2")#.drop("denserank")
# fildf.show()
#
# finaldf = fildf.drop("denserank")
# print("==========By Using Dense_Rank===========")
# finaldf.show()
#
# rankdf = df.withColumn("rank", rank().over(createwindow))
# # rankdf.show()
#
# filrnkdf = rankdf.filter("rank = 2")#.drop("denserank")
# filrnkdf.show()
#
# finaldfrnk = filrnkdf.drop("rank")
# print("==========By Using Rank===========")
# finaldfrnk.show()
#
# #====================================================================================
#

# def student(name='Unknown Name', age= 0, **marks):
#     print("Entered Name is:", name)
#     print("Entered Age is:", age)
#     # print("Entered Marks are:", marks)
#     for key, value in marks.items():
#         print(key, '', value)
# student('Veeramani', 44, Maths=90, Physics=98, Chemistry=100, Biology=95)
# #====================================================================================

# STRUCT DATA

# data = """
# {
#     "id": 1,
#     "institute": "zeyo",
#     "trainer": "Sai",
#     "zeyoAddress" :{
#     			"permanentAddress" : "Hyderabad",
#     			"temporaryAddress" : "chennai"
#     }
# }
# """
#
# jsonrdd = sc.parallelize([data])
#
#
# df = spark.read.option("multiline","true").json(jsonrdd)
#
#
# df.show()
# df.printSchema()
#
# flatdf = df.select(
#
#     "id",
#     "institute",
#     "trainer",
#     "zeyoAddress.permanentAddress",
#     "zeyoAddress.temporaryAddress"
#
# )
#
# flatdf.show()
# flatdf.printSchema()
#====================================================================================
# print()
# print("===============NESTED JSON FLATTENING================")
# print()
#====================================================================================

# data = """
# {
#   "first_name": "Rajeev",
#   "last_name": "Sharma",
#   "email_address": "rajeev@ezeelive.com",
#   "is_alive": true,
#   "age": 30,
#   "height_cm": 185.2,
#   "billing_address": {
#     "address": "502, Main Market, Evershine City, Evershine, Vasai East",
#     "city": "Vasai Raod, Palghar",
#     "state": "Maharashtra",
#     "postal_code": "401208"
#   },
#   "shipping_address": {
#     "address": "Ezeelive Technologies, A-4, Stattion Road, Oripada, Dahisar East",
#     "city": "Mumbai",
#     "state": "Maharashtra",
#     "postal_code": "400058"
#   },
#  "date_of_birth": null
# }
# """

# jsonrdd = spark.sparkContext.parallelize([data])
#
#
# df = spark.read.option("multiline","true").json(jsonrdd)
#
#
# df.show()
# df.printSchema()
#
#
# flatdf = df.selectExpr(
#
#     "age",
#     "billing_address.address   as   billing_address",
#     "billing_address.city   as   billing_city",
#     "billing_address.postal_code   as   billing_postal",
#     "billing_address.state   as   billing_state",
#     "date_of_birth",
#     "email_address",
#     "first_name",
#     "height_cm",
#     "is_alive",
#     "last_name",
#     "shipping_address.address   as   shipping_address",
#     "shipping_address.city  as   shipping_city",
#     "shipping_address.postal_code  as   shipping_postal",
#     "shipping_address.state  as   shipping_state"
#
# )
#
# flatdf.show()
# flatdf.printSchema()
#
#
# df1 = spark.read.format("json").load("file4.json")
# df1.show()


# data1 = [
#     (1, "Henry"),
#     (2, "Smith"),
#     (3, "Hall")
# ]
# columns1 = ["id", "name"]
# rdd1 = sc.parallelize(data1,1)
# df1 = rdd1.toDF(columns1)
# df1.show()
#
# data2 = [
#     (1, 100),
#     (2, 500),
#     (4, 1000)
# ]
# columns2 = ["id", "salary"]
# rdd2 = sc.parallelize(data2,1)
# df2 = rdd2.toDF(columns2)
# df2.show()
#
from pyspark.sql.functions import *
#
# leftjoindf = df1.join(df2, 'id', 'left')
# coaldf = leftjoindf.withColumn('salary', expr("coalesce(cast(salary as int), 0)"))
# print('========this is spark sql like coalesce method=========')
# coaldf.show()
#
#
# coaldf1 = leftjoindf.selectExpr(
#                     'id',
#                     'name',
#                     'coalesce(salary, 0) as salary'
# )
# print('========this is sql like coalesce method=========')
# coaldf1.show()


data1 = [
    (1, "abc", 31, "abc@gmail.com"),
    (2, "def", 23, "defyahoo.com"),
    (3, "xyz", 26, "xyz@gmail.com"),
    (4, "qwe", 34, "qwegmail.com"),
    (5, "iop", 24, "iop@gmail.com")
]
myschema1 = ["id", "name", "age", "email"]
df1 = spark.createDataFrame(data1, schema=myschema1)
print('======================= DF1 =======================')
print()
df1.show()

data2 = [
    (11, "jkl", 22, "abc@gmail.com", 1000),
    (12, "vbn", 33, "vbn@yahoo.com", 3000),
    (13, "wer", 27, "wer", 2000),
    (14, "zxc", 30, "zxc.com", 2000),
    (15, "lkj", 29, "lkj@outlook.com", 2000)
]
myschema2 = ["id", "name", "age", "email", "salary"]
df2 = spark.createDataFrame(data2, schema=myschema2)
print('======================= DF2 =======================')
print()
df2.show()

df1_valid = df1.filter(col("email").contains("@"))
df2_valid = df2.filter(col("email").contains("@"))

# df1_valid.show()
# df2_valid.show()
#
df1_sal = df1_valid.withColumn('salary', lit(1000).cast('int'))
# df1_sal.show()
#
finaldf = df1_sal.unionByName(df2_valid)
print('======================= FINAL EXPECTED OUTPUT =======================')
print()
finaldf.show()







