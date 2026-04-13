from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test-submit").getOrCreate()

print("=== SPARK SUBMIT TEST: SCRIPT RAN SUCCESSFULLY ===")

spark.stop()