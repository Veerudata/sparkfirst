from pyspark.sql import SparkSession

spark = (
    SparkSession.builder \
        .appName("S3Read") \
        # .master("spark://localhost:7077") \
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.s3a.impl","org.apache.hadoop.fs.s3a.S3A") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.profile.ProfileCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.profile","default") \
        .config("spark.hadoop.fs.s3a.endpoint","s3.us-east-1.amazonaws.com") \
        .getOrCreate()
)

print('\n'*3)
print('==================SPARK STARTED SUCCESSFULLY==================')
print('\n'*3)

# sc = spark.sparkContext
#
# uri = spark._jvm.java.net.URI("s3a://snowpark-projects/")
# fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(uri, sc._jsc.hadoopConfiguration())
#
# path = spark._jvm.org.apache.hadoop.fs.Path("s3a://snowpark-projects/")
# files = fs.listStatus(path)
#
# print("\n====== S3 CONTENT ======")
# for f in files:
#     print(f.getPath().toString())


# df = spark.read.parquet("s3a://snowpark-projects/USA-Sales-Order.snappy.parquet")
# df1=df.select('Order ID', 'Customer Name','Payment Status','Delivery Address')
# df1.show()

df = spark.read.parquet("s3a://veeran46/src/projfinal.parquet")
df.show()
d_count=df.count()
print(d_count)
df.printSchema()
# df1=df.select('Order ID', 'Customer Name','Payment Status','Deliparquet")






