import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

    # It's a good practice to set the Spark version and Kafka package versions
SPARK_VERSION = '3.3.0'
KAFKA_PACKAGE = f'org.apache.spark:spark-sql-kafka-0-10_2.12:{SPARK_VERSION}'

def main():
        print("Starting Spark processing...")
        spark = SparkSession.builder \
            .appName("LogAnalytics") \
            .config("spark.jars.packages", KAFKA_PACKAGE) \
            .getOrCreate()

        # Set log level to WARN to reduce verbosity
        spark.sparkContext.setLogLevel("WARN")

        # Define the schema for the incoming log data
        log_schema = StructType([
            StructField("timestamp", StringType(), True),
            StructField("level", StringType(), True),
            StructField("message", StringType(), True),
            StructField("user_id", IntegerType(), True)
        ])

        # Read from Kafka
        kafka_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:29092") \
            .option("subscribe", "logs") \
            .option("startingOffsets", "earliest") \
            .load()

        # Parse the JSON data from the 'value' column
        parsed_df = kafka_df.select(from_json(col("value").cast("string"), log_schema).alias("log")) \
            .select("log.*")

        # Perform a simple aggregation: count logs by level
        log_counts = parsed_df.groupBy("level").count()

        # Write the aggregated data to the console
        query = log_counts.writeStream \
            .outputMode("complete") \
            .format("console") \
            .start()

        print("Streaming query started. Waiting for termination...")
        query.awaitTermination()
        print("Streaming query terminated.")

if __name__ == "__main__":
        main()
