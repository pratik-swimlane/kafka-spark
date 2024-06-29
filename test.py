from pyspark.sql import SparkSession

# Initialize the Spark session
spark = SparkSession.builder \
    .appName("KafkaSparkIntegrationTest") \
    .getOrCreate()

# Attempt to read from Kafka
try:
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "test_topic") \
        .load()

    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").writeStream \
        .format("console") \
        .start() \
        .awaitTermination()

except Exception as e:
    print(f"An error occurred: {e}")
finally:
    spark.stop()
