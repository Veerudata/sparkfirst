from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
import sys
import os

# Ensure PySpark uses current Python
python_path = sys.executable
os.environ["PYSPARK_PYTHON"] = python_path

# Delta package (adjust version if needed)
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages io.delta:delta-spark_2.12:3.2.0 pyspark-shell"
)

# Java Home (update path if different)
# os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk1.8.0_202"

# Spark Configuration
conf = SparkConf().setAppName("pyspark").setMaster("local[*]")

sc = SparkContext.getOrCreate(conf=conf)
sc.setLogLevel("ERROR")

# Create SparkSession with Delta configs
spark = (
    SparkSession.builder
    .appName("DeltalakeExample")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

print("✅ Spark + Delta initialized successfully")
