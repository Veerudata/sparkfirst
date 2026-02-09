import os
os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("MapVsFlatMap") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext

orders = [
    "pen pencil",
    "book notebook",
    "eraser"
]

rdd = sc.parallelize(orders)
print('\n'*2)
mapped = rdd.map(lambda x: x.split(" "))
print("This is map output:", '\n'*2, mapped.collect())
print('\n'*2)

flatmapped = rdd.flatMap(lambda x: x.split(" "))
print("This is flatmap output:", '\n'*2, flatmapped.collect())


logs = [
    "user login success",
    "user logout",
    "login failed"
]

rdd = sc.parallelize(logs)

# map
map_out = rdd.map(lambda x: x.split(" "))
print("MAP OUTPUT:\n", map_out.collect())

# flatMap
flatmap_out = rdd.flatMap(lambda x: x.split(" "))
print("FLATMAP OUTPUT:\n", flatmap_out.collect())

data = [
    "101,pen|pencil|eraser",
    "102,book|notebook",
    "103,marker"
]

rdd = sc.parallelize(data)

# map
map_out = rdd.map(lambda x: x.split(",")[0].split(" "))
print("MAP OUTPUT:\n", map_out.collect())

# flatMap
flatmap_out = rdd.flatMap(lambda x: x.split(",")[0].split(" "))
print("FLATMAP OUTPUT:\n", flatmap_out.collect())

map_out = rdd.map(lambda x: x.split(",")[1].split("|"))
print("MAP OUTPUT:\n", map_out.collect())

# flatMap
flatmap_out = rdd.flatMap(lambda x: x.split(",")[1].split("|"))
print("FLATMAP OUTPUT:\n", flatmap_out.collect())


api_response = [
    {"ids": [1, 2, 3]},
    {"ids": [4, 5]},
    {"ids": [6]}
]

rdd = sc.parallelize(api_response)

# map
map_out = rdd.map(lambda x: x["ids"])
print("MAP OUTPUT:\n", map_out.collect())

# flatMap
flatmap_out = rdd.flatMap(lambda x: x["ids"])
print("FLATMAP OUTPUT:\n", flatmap_out.collect())


# from pyspark.sql import SparkSession

# spark = SparkSession.builder.appName("Validation").getOrCreate()

data = [
    (1, "25"),
    (2, "30"),
    (3, "abc"),
    (4, "40x")
]

df = spark.createDataFrame(data, ["id", "age"])
df.show()

valid_df = df.filter(col("age").rlike("^[0-9]+$"))
valid_df.show()

invalid_df = df.filter(~col("age").rlike("^[0-9]+$"))
invalid_df.show()



data = [
    (1, "25", "50000"),
    (2, "abc", "40000"),
    (3, "30", "xyz"),
    (4, "40x", "60000")
]

df = spark.createDataFrame(data, ["id", "age", "salary"])
df.show()

valid_df = df.filter(
    col("age").rlike("^[0-9]+$") &
    col("salary").rlike("^[0-9]+$")
)

valid_df.show()


invalid_df = df.filter(
    ~(
            col("age").rlike("^[0-9]+$") &
            col("salary").rlike("^[0-9]+$")
    )
)

invalid_df.show()

df.withColumn(
    "error_reason",
    when(~col("age").rlike("^[0-9]+$"), "INVALID_AGE")
    .when(~col("salary").rlike("^[0-9]+$"), "INVALID_SALARY")
    .otherwise("VALID")
).show()





