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
# print()
# df.show(truncate=False)
#
# splitdf =(
#
#     df.withColumn("user",expr("split(email,'@')[0]"))
#     .withColumn("emailc",expr("split(email,'@')[1]"))
#     .withColumn("usersize", expr("length(user)"))
#     .withColumn("firstchar",expr("substring(user,0,1)"))
#     .withColumn("lastchar",expr("substring(user,-2,2)"))
#     .withColumn("maskedemail",expr("concat(firstchar, repeat('*',usersize -3)   , lastchar , '@' , emailc)"))
# )
#
# splitdf.show(truncate=False)
#
#
# mobilemask = (
#     df
#     .withColumn("mobilesize", expr("length(mobile)"))
#     .withColumn("mobilefirstchar", expr("substring(mobile, 1, 1)"))
#     .withColumn("mobilelastchar", expr("substring(mobile, -2, 2)"))
#     .withColumn("maskedmobile", expr("concat(mobilefirstchar, repeat('*', mobilesize - 3), mobilelastchar)"))
# )
#
# mobilemask.show(truncate=False)
#
#
# data="""{
#     "id": 1,
#     "institute": "zeyo",
#     "trainer": "Sai",
#     "zeyoAddress" :{
#     			"permanentAddress" : "Hyderabad",
#     			"temporaryAddress" : "chennai"
#     }
# }"""
#
# df = spark.read.json(sc.parallelize([data]))
#
#
# df.show()
#
# df.printSchema()
#
#
#
#
# flatdf = df.select(
#
#     "id",
#     "institute",
#     "trainer",
#     "zeyoAddress.permanentAddress",
#     "zeyoAddress.temporaryAddress"
#
#
# )
#
# flatdf.show()
# flatdf.printSchema()
#
#
#
# withflatdf =(
#
#     df.withColumn("permanentAddress",expr("zeyoAddress.permanentAddress"))
#     .withColumn("temporaryAddress", expr("zeyoAddress.temporaryAddress"))
#     .drop("zeyoAddress")
# )
#
#
# withflatdf.show()
# withflatdf.printSchema()
#
# #######################################################################################
#
# # COMPLEX ARRAY EXAMPLE 1
#
# #######################################################################################
#
# data="""{
#   "country" : "US",
#   "version" : "0.6",
#   "Actors": [
#     {
#       "name": "Tom Cruise",
#       "age": 56,
#       "BornAt": "Syracuse, NY",
#       "Birthdate": "July 3, 1962",
#       "photo": "https://jsonformatter.org/img/tom-cruise.jpg",
#       "wife": null,
#       "weight": 67.5,
#       "hasChildren": true,
#       "hasGreyHair": false,
#       "picture": {
#                     "large": "https://randomuser.me/api/portraits/men/73.jpg",
#                     "medium": "https://randomuser.me/api/portraits/med/men/73.jpg",
#                     "thumbnail": "https://randomuser.me/api/portraits/thumb/men/73.jpg"
#                 }
#     },
#     {
#       "name": "Robert Downey Jr.",
#       "age": 53,
#       "BornAt": "New York City, NY",
#       "Birthdate": "April 4, 1965",
#       "photo": "https://jsonformatter.org/img/Robert-Downey-Jr.jpg",
#       "wife": "Susan Downey",
#       "weight": 77.1,
#       "hasChildren": true,
#       "hasGreyHair": false,
#       "picture": {
#                     "large": "https://randomuser.me/api/portraits/men/78.jpg",
#                     "medium": "https://randomuser.me/api/portraits/med/men/78.jpg",
#                     "thumbnail": "https://randomuser.me/api/portraits/thumb/men/78.jpg"
#                 }
#     }
#   ]
# }"""
#
# df = spark.read.json(sc.parallelize([data]))
#
#
# df.show()
# df.printSchema()
#
#
#
# flat1 = df.selectExpr(
#
#     "explode(Actors) as Actors",
#     "country",
#     "version"
#
#
# )
#
# flat1.show()
# flat1.printSchema()
#
#
#
#
# flat2 = flat1.selectExpr(
#
#     "Actors.Birthdate",
#     "Actors.BornAt",
#     "Actors.age",
#     "Actors.hasChildren",
#     "Actors.hasGreyHair",
#     "Actors.name",
#     "Actors.photo",
#     "Actors.picture.large",
#     "Actors.picture.medium",
#     "Actors.picture.thumbnail",
#     "Actors.weight",
#     "Actors.wife",
#     "country",
#     "version"
#
#
# )
#
#
# flat2.show()
# flat2.printSchema()
#
# ########################################################################################
# ## COMPLEX ARRAY EXAMPLE 2
# ########################################################################################
#
# data="""{
#    "name":"John",
#    "age":30,
#    "cars":[
#       {
#          "name":"Ford",
#          "models":[
#             "Fiesta",
#             "Focus",
#             "Mustang"
#          ]
#       },
#       {
#          "name":"BMW",
#          "models":[
#             "320",
#             "X3",
#             "X5"
#          ]
#       },
#       {
#          "name":"Fiat",
#          "models":[
#             "500",
#             "Panda"
#          ]
#       }
#    ]
# }"""
#
# df = spark.read.json(sc.parallelize([data]))
#
#
# df.show()
# df.printSchema()
#
#
#
# flat1 = df.selectExpr(
#
#     "age",
#     "explode(cars) as cars",
#     "name"
#
# )
#
# flat1.show()
#
# flat1.printSchema()
#
#
#
#
# flat2 = flat1.selectExpr(
#
#     "age",
#     "cars.models",
#     "cars.name as cars_name",
#     "name"
#
# )
#
# flat2.show()
# flat2.printSchema()
#
#
# finalflat = flat2.selectExpr(
#
#     "age",
#     "explode(models) as models",
#     "cars_name",
#     "name"
#
# )
#
# finalflat.show()
# finalflat.printSchema()

#######################################################################################
# URL DATA (API) READ
#######################################################################################

import urllib
import ssl

urldata = urllib.request.urlopen("https://randomuser.me/api/0.8/?results=10",context=ssl._create_unverified_context()).read().decode('utf-8')


print(urldata)


df   = spark.read.json(sc.parallelize([urldata]))

df.show()
df.printSchema()



explodedf = df.withColumn("results" , expr("explode(results)"))

explodedf.show()
explodedf.printSchema()


finalflatten = explodedf.select(

    "nationality",
    "results.user.cell",
    "results.user.dob",
    "results.user.email",
    "results.user.gender",
    "results.user.location.city",
    "results.user.location.state",
    "results.user.location.street",
    "results.user.location.zip",
    "results.user.md5",
    "results.user.name.first",
    "results.user.name.last",
    "results.user.name.title",
    "results.user.password",
    "results.user.phone",
    "results.user.picture.large",
    "results.user.picture.medium",
    "results.user.picture.thumbnail",
    "results.user.registered",
    "results.user.salt",
    "results.user.sha1",
    "results.user.sha256",
    "results.user.username",
    "seed",
    "version"
)

finalflatten.show()
finalflatten.printSchema()


