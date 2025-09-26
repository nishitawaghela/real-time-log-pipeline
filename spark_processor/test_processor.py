import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
import json

    # This is a simplified version of the logic in your processor.py
    # We are testing the core transformation logic in isolation.
def extract_log_level(df, level_col_name="level"):
        """Extracts the 'level' field from a JSON string column."""
        json_schema = "level STRING"
        return df.withColumn(level_col_name, F.from_json(col("value"), json_schema).getItem("level"))


@pytest.fixture(scope="session")
def spark_session():
        """Creates a Spark session for testing."""
        spark = SparkSession.builder \
            .master("local[2]") \
            .appName("TestLogLevelExtraction") \
            .getOrCreate()
        yield spark
        spark.stop()


def test_log_level_extraction(spark_session):
        """Tests that the log level is correctly extracted from a JSON string."""
        # 1. Create a sample DataFrame
        log_data = [
            (json.dumps({"level": "INFO", "message": "User logged in"}),),
            (json.dumps({"level": "ERROR", "message": "DB connection failed"}),)
        ]
        df = spark_session.createDataFrame(log_data, ["value"])

        # 2. Apply the transformation
        result_df = extract_log_level(df)

        # 3. Collect the results and check them
        results = result_df.select("level").rdd.flatMap(lambda x: x).collect()

        assert "INFO" in results
        assert "ERROR" in results
        assert len(results) == 2