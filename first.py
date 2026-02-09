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


conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)

spark = SparkSession.builder.getOrCreate()

spark.read.format("csv").load("data/test.txt").toDF("Success").show(20, False)


##################🔴🔴🔴🔴🔴🔴 -> DONT TOUCH ABOVE CODE -- TYPE BELOW ####################################



print("====Spark Started====")
#
# a = 2
# print(a)
#
# b = a + 2
# print(b)

# c = "zeyobron"
# print(c)
#
# d = c + " analytics"
# print(d)

ls = [1,2,3,4]
print("====== rawList ======")
print(ls)

rddls = sc.parallelize(ls)
print("====== rddlist ======")
print(rddls.collect())

addls=rddls.map (lambda x: x + 2)
print("========= addlist ========")
print(addls.collect())

multils=rddls.map (lambda x: x * 2)
print("========= multilist ========")
print(multils.collect())

e=  [  1  ,   2   ,   3   , 4]
print()
print("=============RawList============")
print(e)


rddin = sc.parallelize(e)
print()
print("=============RDD============")
print(rddin.collect())


addin = rddin.map(   lambda   x   :   x +  2  )
print()
print("=============addin============")
print(addin.collect())




mulin = rddin.map(   lambda   x  :  x  *  10)
print()
print("=============mulin============")
print(mulin.collect())


# FILTER CODE

print("=======STARTED======")


e=  [  1  ,   2   ,   3   , 4]
print()
print("=============RawList============")
print(e)


rddin = sc.parallelize(e)
print()
print("=============RDD============")
print(rddin.collect())


filin = rddin.filter( lambda x :  x  >  2)
print()
print("=============filin============")
print(filin.collect())



print("=======STARTED======")


listr=  [  "zeyobron"  ,  "zeyo"   ,   "tera"   ]
print()
print("=============listr============")
print(listr)


rddstr = sc.parallelize(listr)
print()
print("=============rddstr============")
print(rddstr.collect())


#FULL STRING CODE

listr=  [  "zeyobron"  ,  "zeyo"   ,   "tera"   ]
print()
print("=============listr============")
print(listr)


rddstr = sc.parallelize(listr)
print()
print("=============rddstr============")
print(rddstr.collect())

addstr = rddstr.map(lambda   x  :  x  +  " Analytics")
print()
print("=============addstr============")
print(addstr.collect())


repstr = rddstr.map( lambda x : x.replace( "zeyo" , "" ))
print()
print("=============repstr============")
print(repstr.collect())

# sc = SparkContext(conf=conf)
# ss = SparkSession.builder.getOrCreate()

sc.textFile(r"file:///C:\Users\veera\IdeaProjects\sparkfirst\dt.txt").foreach(print)

spark.read.csv(r"file:///C:\Users\veera\IdeaProjects\sparkfirst\dt.txt").show()

spark.read.csv(r"file:///C:\Users\veera\IdeaProjects\sparkfirst\file1.txt").show(truncate=False)

# spark.read.csv(r"file:///C:\Users\veera\IdeaProjects\sparkfirst\df1.csv").options("Header", "True").show(truncate=False)



spark.read.format("csv").option("header", "true").load(r"file:///C:\Users\veera\IdeaProjects\sparkfirst\df1.csv").show()