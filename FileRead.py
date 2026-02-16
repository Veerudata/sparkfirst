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

spark = (
    SparkSession.builder
    .appName("pyspark")
    .master("local[*]")
    .config("spark.driver.host", "localhost")
    .config("spark.ui.port", "4040")
    .config("spark.default.parallelism", "1")
    .getOrCreate()
)


df = spark.read.option("multiline", "true") \
    .json("file:///C:/Users/veera/instance_types.json")

# df.show(truncate=False)

df.printSchema()


# data = spark.read.format("csv") \
#     .option("header", "true") \
#     .load("file:///E:/Downloads/instancetypes.csv")
#
# data = data.withColumnRenamed("Instance type", "instance_type") \
#     .withColumnRenamed("Instance family", "instance_family") \
#     .withColumnRenamed("vCPUs", "vcpu") \
#     .withColumnRenamed("Memory (GiB)", "memory_gib") \
#     .withColumnRenamed("Network performance", "network_perf")
#
# data.createOrReplaceTempView("data")
#
# # data.printSchema()
# selectCols = spark.sql("""
# SELECT
#     instance_type,
#     instance_family,
#     vcpu,
#     memory_gib,
#     network_perf
#     FROM data
# """)
#
# selectCols.show(10000)
# from pyspark.sql import Row

# spark.createDataFrame([Row(total_rows=data.count())]).show()
# countentry = data.count()
# print(countentry)


from pyspark.sql.functions import split

# from pyspark.sql.functions import split, col
#
# df2 = data.withColumn("cols", split(col("value"), "\t"))
#
# df2.select(
#     col("cols").getItem(0).alias("S_No"),
#     col("cols").getItem(1).alias("Date_Time"),
#     col("cols").getItem(2).alias("HR_Name"),
#     col("cols").getItem(3).alias("Contact_No"),
#     col("cols").getItem(4).alias("Email"),
#     col("cols").getItem(5).alias("Company"),
#     col("cols").getItem(6).alias("Client"),
#     col("cols").getItem(7).alias("Mode"),
#     col("cols").getItem(8).alias("Place")
# ).show()
#
# df2.persist()
#
# print('Persisted DF2')
#
#
# df2.rdd.partitions.size





#
# from pyspark.sql import SparkSession
# import time
#
# spark = SparkSession.builder \
#     .appName("pyspark") \
#     .master("local[*]") \
#     .config("spark.driver.host", "localhost") \
#     .getOrCreate()
#
# print("Spark UI is available at http://localhost:4040")
# time.sleep(300)   # keep app alive for 5 minutes


##################🔴🔴🔴🔴🔴🔴 -> DONT TOUCH ABOVE CODE -- TYPE BELOW ####################################

# a=2
# print(a)
#
# b=a+2
# print(b)
#
# c='zeyobron'
# print(c)
#
# d=c+'Analytics'
# print(d)

# lis=[1,2,3,4]
# print()
# print("=====RAW LIST=====")
# print(lis)
#
# rddlis = sc.parallelize(lis)
# print()
# print("=====RDD LIST=====")
# print(rddlis.collect())
#
# addlis=rddlis.map(lambda x: x + 2)
# print()
# print("=====ADD LIST=====")
# print(addlis.collect())
#
# mullis=rddlis.map(lambda x: x * 2)
# print()
# print("=====MULTIPLY LIST=====")
# print(mullis.collect())
#
# fillis=rddlis.filter(lambda x: x > 2)
# print()
# print("=====FILTER LIST=====")
# print(fillis.collect())

# listr = ["zeyobron" , "zeyo" , "tera"]
# print()
# print("=====RAW STRING LIST=====")
# print(listr)
#
# rddstr=sc.parallelize(listr)
# print()
# print("=====RDD STRING LIST=====")
# print(rddstr.collect())
#
# conrdd = rddstr.map( lambda x : x + "Analytics")
# print()
# print("=====CONCAT STRING RDD=====")
# print(conrdd.collect())
#
# reprdd=rddstr.map( lambda x : x.replace("zeyo", "exa"))
# print()
# print("=====REPLACE STRING RDD=====")
# print(reprdd.collect())
#
# repemp=rddstr.map( lambda x : x.replace("zeyo", ""))
# print()
# print("=====REMOVE STRING RDD=====")
# print(repemp.collect())
#
# filsrdd = rddstr.filter( lambda x: "zeyo" in x)
# print()
# print("=====CONTAINS STRING RDD=====")
# print(filsrdd.collect())
#
# flatlis = ["A~B", "C~B"]
# print()
# print("====RAW FLAT LIST====")
# print(flatlis)
#
# flatrdd = sc.parallelize(flatlis)
# print()
# print("=====RDD LIST FLAT ======")
# print(flatrdd.collect())
#
# flatrdds = flatrdd.flatMap(lambda x : x.split("~"))
# print()
# print("=====FLATTENED LIST RDD=====")
# print(flatrdds.collect())

# lstates = [
#     "State->TN~City->Chennai"  , "State->Kerala~City->Trivandrum"
# ]
# print()
# print("=====RAW LIST=====")
# print(lstates)
#
# rddstates = sc.parallelize(lstates)
# print()
# print("====RDD STATES=====")
# print(rddstates.collect())
#
# flatrdd = rddstates.flatMap(lambda x : x.split("~"))
# print()
# print("=====flatrdd======")
# print(flatrdd.collect())
#
# filstates = flatrdd.filter(lambda x : "State" in x)
# print()
# print("=====filstates======")
# print(filstates.collect())
#
# filcities = flatrdd.filter(lambda x : "City" in x)
# print()
# print("=====filcities======")
# print(filcities.collect())
#
# states = filstates.map(lambda x : x.replace("State->",""))
# print()
# print("=====states======")
# print(states.collect())
#
# cities = filcities.map(lambda x : x.replace("City->",""))
# print()
# print("=====cities======")
# # print(cities.collect())
# cities.foreach(print)
#
#
# data = ['State->TN~City->Chennai', 'State->Kerala~City->Trivandrum']
# #
# rdd = sc.parallelize(data)
# #
# states = rdd.map(lambda x: x.split("~")[0].split("->")[1]).collect()
# cities = rdd.map(lambda x: x.split("~")[1].split("->")[1]).collect()
#
# print(states)
# print(cities)

# result = rdd.map(lambda x: tuple(part.split("->")[1] for part in x.split("~"))).collect()
#
# states  = [s for s, c in result]
# cities  = [c for s, c in result]
# print()
# print(states)
# print(cities)

# data = sc.textFile("state.txt")
# print()
# print("=====RAW RDD=====")
# data.foreach(print)
#
# flatdata = data.flatMap(lambda x : x.split("~"))
# print()
# print("======flatdata Print Using foreach Method()=======")
# flatdata.foreach(print)
# print()
# print("======flatdata Print Using Collect() Method========")
# print(flatdata.collect())
#
# filstate = flatdata.filter(lambda x : "State" in x)
# print()
# print("======filstate========")
# filstate.foreach(print)
# # print(filstate.collect())
#
# filcity = flatdata.filter(lambda x : "City" in x)
# print()
# print("=======filcity=======")
# filcity.foreach(print)
# # print(filcity.collect())
#
# states = filstate.map(lambda x : x.replace("State->"," "))
# print()
# print("=======states========")
# states.foreach(print)
# # print(states.collect())
#
# cities = filcity.map(lambda x : x.replace("City->", " "))
# print()
# print("======cities=======")
# cities.foreach(print)
# # print(cities.collect())


# data = sc.textFile("usdata.csv")
# print()
# print("======RAW RDD=======")
# data.foreach(print)
#
# lendata = data.filter(lambda x : len(x) > 200)
# print()
# print("======lendata=======")
# lendata.foreach(print)
#
# flatdata = lendata.flatMap(lambda x : x.split(","))
# print()
# print("======flatdata=======")
# flatdata.foreach(print)
#
# remdata = flatdata.map(lambda x : x.replace("-", ""))
# print()
# print("======remdata=======")
# remdata.foreach(print)
#
# condata = flatdata.map(lambda x : x + ",zeyo")
# print()
# print("======condata=======")
# condata.foreach(print)
#
# condata.saveAsTextFile("file:///E:/procusdata")

# data = sc.textFile("dt.txt")
# print()
# print("=======RAW RDD=======")
# data.foreach(print)
#
# gymdata = data.filter( lambda x : "Gymnastics" in x)
# print()
# print("=========gymdata==========")
# gymdata.foreach(print)
#
# datasplit = data.map( lambda x : x.split(","))
# print()
# print("==========datasplit=============")
# datasplit.foreach(print)
#
# filrdd = datasplit.filter( lambda x : "Gymnastics" in x[3])
# print()
# print("==========filrdd=============")
# filrdd.foreach(print)

# data = sc.textFile("dt.txt")
# print()
# print("======RAW DATA=======")
# data.foreach(print)
#
# mapsplit = data.map( lambda x : x.split(","))
# print()
# print("=====mapsplit======")
# mapsplit.foreach(print)
#
# #  LETS DEFINE THE COLUMNS
#
# from collections import namedtuple
# columns = namedtuple('columns',['id','tdate','amt','category','product','mode'])
#
#
# #  Lets Assign columns to split
#
#
# schemardd = mapsplit.map( lambda   x  :  columns( x[0],    x[1],  x[2]  ,  x[3]  ,x[4]   ,x[5]))
#
#
# # Apply filter on Product Column
#
# prodfilter = schemardd.filter( lambda   x   :  'Gymnastics'  in   x.product)
# print()
# print("=====prodfilter======")
# prodfilter.foreach(print)
#
#
# #  Convert RDD to Dataframe
#
# df   =   prodfilter.toDF()
# df.show()

# CSV Read

# csvdf = spark.read.format("csv").option("header", "true").load("usdata.csv")
# print()
# print("==========CSV Read==========")
# csvdf.show(1)
#
# # JSON Read
#
# jsondf = spark.read.format("json").load("file4.json")
# print()
# print("==========JSON Read==========")
# jsondf.show(1)
#
# # Parquet Read
#
# parquetdf = spark.read.format("parquet").load("file5.parquet")
# print()
# print("==========PARQUET Read==========")
# parquetdf.show(1)

############  GET READY SQL DATA  #############

# data = [
#     (0, "06-26-2011", 300.4, "Exercise", "GymnasticsPro", "cash"),
#     (1, "05-26-2011", 200.0, "Exercise Band", "Weightlifting", "credit"),
#     (2, "06-01-2011", 300.4, "Exercise", "Gymnastics Pro", "cash"),
#     (3, "06-05-2011", 100.0, "Gymnastics", "Rings", "credit"),
#     (4, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
#     (5, "02-14-2011", 200.0, "Gymnastics", None, "cash"),
#     (6, "06-05-2011", 100.0, "Exercise", "Rings", "credit"),
#     (7, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
#     (8, "02-14-2011", 200.0, "Gymnastics", None, "cash")
# ]
#
# df = spark.createDataFrame(data, ["id", "tdate", "amount", "category", "product", "spendby"])
# df.show()
#
# data2 = [
#     (4, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
#     (5, "02-14-2011", 200.0, "Gymnastics", None, "cash"),
#     (6, "02-14-2011", 200.0, "Winter", None, "cash"),
#     (7, "02-14-2011", 200.0, "Winter", None, "cash")
# ]
#
# df1 = spark.createDataFrame(data2, ["id", "tdate", "amount", "category", "product", "spendby"])
# df1.show()
#
# data4 = [
#     (1, "raj"),
#     (2, "ravi"),
#     (3, "sai"),
#     (5, "rani")
# ]
#
# cust = spark.createDataFrame(data4, ["id", "name"])
# cust.show()
#
# data3 = [
#     (1, "mouse"),
#     (3, "mobile"),
#     (7, "laptop")
# ]
#
# prod = spark.createDataFrame(data3, ["id", "product"])
# prod.show()
#
# # Register DataFrames as temporary views
# df.createOrReplaceTempView("df")
# df1.createOrReplaceTempView("df1")
# cust.createOrReplaceTempView("cust")
# prod.createOrReplaceTempView("prod")
#
#
# spark.sql("select * from df order by id desc").show()
# spark.sql("select * from df1 order by id desc").show()
# spark.sql("select * from df").show()
# spark.sql("select id,tdate from df order by id").show()
# spark.sql("select id,tdate,category from df where category='Exercise'   order by id").show()
# spark.sql("select id, tdate, category, spendby from df where category='Exercise' and spendby='cash' ").show()
# spark.sql("select * from df   where category in ('Exercise','Gymnastics') order by category, id").show()
# spark.sql("select * from df   where product like ('%Gymnastics%')").show()
# spark.sql("select * from df   where category != 'Exercise'").show()
# spark.sql("select * from df   where category not in ('Exercise','Gymnastics')").show()
# spark.sql("select * from df   where product is null").show()
# spark.sql("select * from df   where product is not null").show()
# spark.sql("select max(id) from df").show()
# spark.sql("select min(id) from df").show()
# spark.sql("select count(1) from df").show()
# spark.sql("select *,case when spendby='cash' then 1 else 0 end  as status from df").show()
# spark.sql("select id,category,concat(id,'-',category) as condata from df").show()
# spark.sql("select id,category,product,concat_ws('-',id,category,product) as condata from df").show()
# spark.sql("select category,lower(category) as lower from df").show()
# spark.sql("select category,upper(category) as upper from df").show()
# spark.sql("select amount,ceil(amount) as ceil from df").show()
# spark.sql("select amount,round(amount) as round from df").show()
# spark.sql("select product,coalesce(product,'NA') as nullrep from df").show()
# spark.sql("select trim(product) from df").show()
# spark.sql("select distinct category,spendby from df").show()
# spark.sql("select substring(product,1,10) as sub from df").show()
# spark.sql("select SUBSTRING_INDEX(category,' ',1) as spl from df").show()
# spark.sql("select * from df union all select * from df1").show()
# spark.sql("select * from df union select * from df1 order by id").show()
# spark.sql("select category, sum(amount) as total from df group by category").show()
# spark.sql("select category,spendby,sum(amount)  as total from df group by category,spendby").show()
# spark.sql("select category,spendby,sum(amount) As total,count(amount)  as cnt from df group by category,spendby").show()
# spark.sql("select category, max(amount) as max from df group by category").show()
# spark.sql("select category, max(amount) as max from df group by category order by category desc").show()
# spark.sql("SELECT category,amount, row_number() OVER ( partition by category order by amount desc ) AS row_number FROM df").show()
# spark.sql("SELECT category,amount, dense_rank() OVER ( partition by category order by amount desc ) AS dense_rank FROM df").show()
# spark.sql("SELECT category,amount, rank() OVER ( partition by category order by amount desc ) AS rank FROM df").show()
# spark.sql("SELECT category,amount, lead(amount) OVER ( partition by category order by amount desc ) AS lead FROM df").show()
# spark.sql("SELECT category,amount, lag(amount) OVER ( partition by category order by amount desc ) AS lag FROM df").show()
# spark.sql("select category,count(category) as cnt from df group by category having count(category)>1").show()
# spark.sql("select a.id,a.name,b.product from cust a join prod b on a.id=b.id").show()
# spark.sql("select a.id,a.name,b.product from cust a left join prod b on a.id=b.id").show()
# spark.sql("select a.id,a.name,b.product from cust a right join prod b on a.id=b.id").show()
# spark.sql("select a.id,a.name,b.product from cust a full join prod b on a.id=b.id").show()
# spark.sql("select a.id,a.name from cust a LEFT ANTI JOIN  prod b on a.id=b.id").show()
# spark.sql("select id,tdate,from_unixtime(unix_timestamp(tdate,'MM-dd-yyyy'),'yyyy-MM-dd') as con_date from df").show()
# spark.sql("""select sum(amount) as total , con_date from (select id,tdate,from_unixtime(unix_timestamp(tdate,'MM-dd-yyyy'),'yyyy-MM-dd') as con_date,amount,category,product,spendby from df) group by con_date
# """).show()
# spark.sql("select category,collect_list(spendby) as col_spend from df group by category").show()
# spark.sql("select category,collect_set(spendby) as col_spend from df group by category ").show()
# spark.sql("select category,explode(col_spend) as ex_spend from (select category,collect_set(spendby) as col_spend from df group by category)").show()
# spark.sql("select category,explode_outer(col_spend) as ex_spend from (select category,collect_set(spendby) as col_spend from df group by category)").show()



# data = [
#
#     ("00000", "06-26-2011", 200, "Exercise", "GymnasticsPro", "cash"),
#     ("00001", "05-26-2011", 300, "Exercise", "Weightlifting", "credit"),
#     ("00002", "06-01-2011", 100, "Exercise", "GymnasticsPro", "cash"),
#     ("00003", "06-05-2011", 100, "Gymnastics", "Rings", "credit"),
#     ("00004", "12-17-2011", 300, "Team Sports", "Field", "paytm"),
#     ("00005", "02-14-2011", 200, "Gymnastics", None, "cash")
#
# ]
#
# df = spark.createDataFrame(data, ["id", "tdate", "amount", "category", "product", "spendby"])
# df.show()
#

# procdf  =  df.select("tdate" ,"amount")
# procdf.show()
#
# dropdf = df.drop("tdate","amount")
# dropdf.show()
#
# sincol = df.filter("   category ='Exercise'    ")
# sincol.show()
#
#
# mulcoland = df.filter("    category='Exercise'  and  spendby = 'cash'          ")
# mulcoland.show()
#
#
# mulcolor =  df.filter("    category ='Exercise' or spendby = 'cash'   ")
# mulcolor.show()


# sincolin = df.filter(" category in ( 'Exercise', 'Gymnastics' ) ")
# sincolin.show()
#
# print(sc.parallelize(["first","second"]).collect())
#
# import time
# time.sleep(360)     #  in browserhit this url ---->   localhost:4040

#        localhost:4040

# procdf = df.selectExpr(
#     "id",
#     "split(tdate, '-')[2] as year",
#     "amount + 1000 as amount",
#     "upper(category) as category",
#     "concat(product, '~zeyo') as product",
#     "spendby"
# )
# procdf.show()
# procdf.printSchema()
#
# procdf = df.selectExpr(
#     " cast(id as int) as id",
#     "split(tdate, '-')[2] as year",
#     "amount + 1000 as amount",
#     "upper(category) as category",
#     "concat(product, '~zeyo') as product",
#     "spendby",
#     "case when spendby='cash' then 0 else 1 end as status"
# )
# procdf.show()
# procdf.printSchema()

# data = spark.read.format('parquet').load(r 'file:///C:/Users/veera/localfolder/veerafile/projfinal.parquet')
# data = spark.read.parquet("file:///C:/Users/veera/localfolder/projfinal.parquet")
# data.show()

#
# a = [1,2,3,4]
# b = [10, 20, 30]
# c = []
#
# for x, y in zip(a[1:], b):
#     c.append( x -y)
# print(c)


# data = spark.read.textFile("file:///C:\Users\veera\OneDrive\Desktop\Sai's big data course\Resume\CALLS.txt")
# data.show()

# data = spark.read.text(
#     r"file:///E:/Users/veera/OneDrive/Desktop/Sai's big data course/Resume/CALLS.txt"
# )
# # data.show(truncate=False)
#
# from pyspark.sql.functions import split
#
# from pyspark.sql.functions import split, col
#
# df2 = data.withColumn("cols", split(col("value"), "\t"))
#
# df2.select(
#     col("cols").getItem(0).alias("S_No"),
#     col("cols").getItem(1).alias("Date_Time"),
#     col("cols").getItem(2).alias("HR_Name"),
#     col("cols").getItem(3).alias("Contact_No"),
#     col("cols").getItem(4).alias("Email"),
#     col("cols").getItem(5).alias("Company"),
#     col("cols").getItem(6).alias("Client"),
#     col("cols").getItem(7).alias("Mode"),
#     col("cols").getItem(8).alias("Place")
# ).show()
#
# df2.persist()
#
# print('Persisted DF2')
#
#
# df2.rdd.partitions.size


from pyspark import StorageLevel













































