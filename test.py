# save as test_spark.py

from pyspark.sql import SparkSession

def main():
    # create SparkSession only (don't manually create SparkContext earlier)
    spark = SparkSession.builder \
        .appName("test_pyspark") \
        .master("local[*]") \
        .getOrCreate()

    data = [
        ("Renuka1992@gmail.com", "9856765434"),
        ("anbuarasu@hotmail.com", "9844567788")
    ]
    cols = ["email", "mobile"]

    df = spark.createDataFrame(data, cols)
    df.show(truncate=False)

    # stop spark
    spark.stop()

if __name__ == "__main__":
    main()
