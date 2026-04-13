import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window, avg, count, to_json, struct, sum, min, round
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

KAFKA_BROKERS = "kafka:29092"
RAW_TOPIC = "raw-traffic"
CRITICAL_TOPIC = "critical-traffic"
AGGREGATES_PATH = "file:///data/traffic_aggregates/parquet"

TRAFFIC_SCHEMA = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("vehicle_count", IntegerType(), True),
    StructField("avg_speed", DoubleType(), True),
])

def create_spark_session():
    """Initializes and returns a Spark Session."""
    return SparkSession.builder \
        .appName("SmartCityTrafficProcessor") \
        .master("spark://spark-master:7077") \
        .config("spark.executor.cores", "1") \
        .config("spark.cores.max", "1") \
        .getOrCreate()

def read_from_kafka(spark):
    """Reads the raw-traffic stream from Kafka."""
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
        .option("subscribe", RAW_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    parsed_df = raw_df.select(
        col("key").cast(StringType()),
        from_json(col("value").cast("string"), TRAFFIC_SCHEMA).alias("data"),
        col("timestamp").alias("kafka_ingest_time")
    ).select(
        col("data.*"),
        col("kafka_ingest_time")
    ).withColumn(
        "timestamp", to_timestamp(col("timestamp"))
    ).withColumn(
        "vehicle_count", col("vehicle_count").cast(IntegerType())
    ).withColumn(
        "avg_speed", col("avg_speed").cast(DoubleType())
    )
    
    return parsed_df

def process_and_split_stream(df):
    """
    Splits the stream into two logical streams:
    1. Aggregates (5-minute windowing).
    2. Critical Alerts (immediate trigger).
    """
    aggregated_df = df.withWatermark("timestamp", "2 minutes") \
        .groupBy(
            window(col("timestamp"), "1 minutes"),
            col("sensor_id")
        ) \
        .agg(
            sum("vehicle_count").alias("total_vehicles"),
            avg("avg_speed").alias("window_avg_speed"),
            min("avg_speed").alias("min_speed_in_window"),
            count("*").alias("total_readings")
        ) \
        .withColumn(
            # Congestion Index: A simple example (e.g., total vehicles / avg speed)
            "congestion_index", 
            round(col("total_vehicles") / (col("window_avg_speed") + 0.01), 2)
        ) \
        .select(
            col("sensor_id"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "total_vehicles",
            "window_avg_speed",
            "congestion_index"
        )
    alert_df = df.filter(col("avg_speed") < 10.0) \
        .select(
            to_json(struct(
                col("sensor_id"),
                col("timestamp").cast(StringType()).alias("alert_timestamp"),
                col("avg_speed").alias("critical_speed")
            )).alias("value"),
            col("sensor_id").alias("key")
        )
    
    return aggregated_df, alert_df

def write_parquet_stream(df):
    """Writes the aggregated stream to Parquet files, partitioned by date/time."""
    print("Starting Parquet Write Stream...")
    parquet_query = df.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("path", AGGREGATES_PATH) \
        .option("checkpointLocation", f"{AGGREGATES_PATH}_checkpoint") \
        .partitionBy("sensor_id", "window_start") \
        .trigger(processingTime='1 minute') \
        .start()
    return parquet_query

def write_kafka_stream(df):
    """Writes the alert stream to the critical-traffic Kafka topic."""
    print("Starting Kafka Alert Write Stream...")
    kafka_query = df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
        .option("topic", CRITICAL_TOPIC) \
        .option("checkpointLocation", f"/tmp/critical_alerts_checkpoint") \
        .outputMode("append") \
        .start()
    return kafka_query

if __name__ == "__main__":
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR") 

    raw_traffic_df = read_from_kafka(spark)
    
    aggregated_df, alert_df = process_and_split_stream(raw_traffic_df)

    parquet_query = write_parquet_stream(aggregated_df)
    kafka_query = write_kafka_stream(alert_df)

    print("Spark Streaming is running. Press Ctrl+C to stop.")
    try:
        parquet_query.awaitTermination()
        kafka_query.awaitTermination()
    except KeyboardInterrupt:
        print("Stopping Spark queries...")
    finally:
        spark.stop()