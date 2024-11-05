from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DateType

# Set up a Spark session to stream data from a Kafka topic named 'stedi-events'
spark = SparkSession.builder.appName("KafkaEventStream").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

# Read from the Kafka topic with "earliest" offset option to capture all prior events as well as new events
kafka_stream_df = (
    spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "stedi-events")
    .option("startingOffsets", "earliest")
    .load()
)

# Transform the 'value' column from binary to string format for JSON parsing
kafka_stream_df = kafka_stream_df.selectExpr("CAST(value AS STRING) AS raw_value")

# Define the schema for the JSON data in the 'raw_value' column
event_schema = StructType([
    StructField("customer", StringType(), True),
    StructField("score", FloatType(), True),
    StructField("riskDate", DateType(), True)
])

# Parse JSON from 'raw_value' into separate columns based on schema, creating a structured DataFrame
kafka_stream_df = kafka_stream_df.withColumn("parsed_data", from_json("raw_value", event_schema)) \
                                 .select("parsed_data.*")

# Register the structured DataFrame as a temporary view to enable SQL queries
kafka_stream_df.createOrReplaceTempView("CustomerRisk")

# Run a SQL query to select 'customer' and 'score' columns from the temporary view
customer_risk_df = spark.sql("SELECT customer, score FROM CustomerRisk")

# Stream the query results to the console, showing new records in real-time as they arrive
query = (
    customer_risk_df
    .writeStream
    .outputMode("append")
    .format("console")
    .start()
)
query.awaitTermination()