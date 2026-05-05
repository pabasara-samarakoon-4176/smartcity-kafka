from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, col, rank, sum, when, avg, current_date, date_sub
from pyspark.sql.window import Window

AGGREGATES_PATH = "file:///data/traffic_aggregates/parquet"
REPORT_PATH = "file:///data/traffic_reports/daily_peaks"

CONGESTION_THRESHOLD = 5.0

def find_peak_traffic(spark):
    df = spark.read.parquet(AGGREGATES_PATH).filter(
        col("traffic_date") == current_date()
    )

    hourly_df = df.withColumn(
        "hour",
        hour(col("window_start").cast("timestamp"))
    ).groupBy(
        "sensor_id",
        "hour"
    ).agg(
        sum("total_vehicles").alias("hourly_volume"),
        avg("congestion_index").alias("avg_congestion_index")
    )

    window_spec = Window.partitionBy("sensor_id").orderBy(col("hourly_volume").desc())
    
    peak_df = hourly_df.withColumn(
        "rank",
        rank().over(window_spec)
    ).filter(
        col("rank") == 1
    ).select(
        "sensor_id",
        col("hour").alias("peak_hour"),
        "hourly_volume",
        "avg_congestion_index"
    )

    max_volume = peak_df.agg(
        {"hourly_volume": "max"}
    ).collect()[0][0]

    dynamic_volume_threshold = max_volume * 0.95

    report_df = peak_df.withColumn(
        "intervention_required",
        when(
            (col("hourly_volume") >= dynamic_volume_threshold) &
            (col("avg_congestion_index") >= CONGESTION_THRESHOLD),
            "YES"
        ).otherwise("NO")
    ).withColumn(
        "recommended_action",
        when(
            col("intervention_required") == "YES",
            "Deploy traffic police"
        ).otherwise("Monitor only")
    )

    report_df.write \
        .mode("overwrite") \
        .option("header", True) \
        .csv(REPORT_PATH)

    print(
        f"Daily traffic intervention report written to {REPORT_PATH}"
    )

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    find_peak_traffic(spark)
    spark.stop()