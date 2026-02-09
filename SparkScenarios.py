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

source_rdd = spark.sparkContext.parallelize([
    (1, "A"),
    (2, "B"),
    (3, "C"),
    (4, "D")
],1)

target_rdd = spark.sparkContext.parallelize([
    (1, "A"),
    (2, "B"),
    (4, "X"),
    (5, "F")
],2)

# Convert RDDs to DataFrames using toDF()
df1 = source_rdd.toDF(["id", "name"])
df2 = target_rdd.toDF(["id", "name1"])

df1.show()
df2.show()


joindf = df1.join(df2, ["id"], "full" )
joindf.show()



comdf = joindf.withColumn("comment" , expr("case when name=name1 then 'match'  else 'mismatch' end"))
comdf.show()


fildf = comdf.filter(" comment !=  'match'   ")
fildf.show()


exprdf = fildf.withColumn("comment", expr("""
                                                    case
                                                    when  name1  is null then 'New in Source'
                                                    when  name   is null then 'New in Target'
                                                    else comment
                                                    end

                                                """))
exprdf.show()


finaldf =  exprdf.drop("name","name1")
finaldf.show()

#Cross JOIN



# data4 = [
#     (1, "raj"),
#     (2, "ravi"),
#     (3, "sai"),
#     (5, "rani")
# ]
#
# cust = spark.createDataFrame(data4, ["id", "name"])
# cust.show()
# data3 = [
#     (1, "mouse"),
#     (3, "mobile"),
#     (7, "laptop")
# ]
# prod = spark.createDataFrame(data3, ["id", "product"])
# prod.show()
#
#
# crossjoin = cust.crossJoin(prod)
# crossjoin.show()

# data4 = [
#     (1, "sai"),
#     (2, "zeyo"),
#     ]
#
# cust = spark.createDataFrame(data4, ["id", "name"])
# cust.show()
# data3 = [
#     (1, "mouse"),
#     (2, "mobile"),
#
# ]
# prod = spark.createDataFrame(data3, ["id", "product"])
# prod.show()
#
#
# crossjoin = cust.crossJoin(prod)
# crossjoin.show()