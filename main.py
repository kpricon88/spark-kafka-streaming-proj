from pyspark.sql import SparkSession


if __name__ == "__main__":
    print("PySpark Structured Streaming with Kafka Demo Application Started ...")

spark = SparkSession \
    .builder \
    .appName("PySpark Structured Streaming with Kafka Demo") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


plist = '[{"Product":"P1","Qty":"10"},{"Product":"P2","Qty":"5"}]'

spark_df = spark.read.json(spark.parallelize([plist]))
spark_df.show()
spark.stop()
print("spark application done")

