from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, col, rank, sum
from pyspark.sql.window import Window

AGGREGATES_PATH = "file:///data/traffic_aggregates/parquet"
REPORT_PATH = "file:///data/traffic_reports/daily_peaks"

def find_peak_traffic(spark):
    df = spark.read.parquet(AGGREGATES_PATH)

    hourly_df = df.withColumn(
        "hour", hour(col("window_start").cast("timestamp"))
    ).groupBy("sensor_id", "hour").agg(
        sum("total_vehicles").alias("hourly_volume")
    )

    window_spec = Window.partitionBy("sensor_id").orderBy(col("hourly_volume").desc())
    
    peak_df = hourly_df.withColumn(
        "rank", rank().over(window_spec)
    ).filter(col("rank") == 1).select(
        "sensor_id",
        col("hour").alias("peak_hour"),
        "hourly_volume"
    )

    peak_df.write.mode("overwrite").csv(REPORT_PATH, header=True)
    
    print(f"Daily peak report written to {REPORT_PATH}")

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    find_peak_traffic(spark)
    spark.stop()