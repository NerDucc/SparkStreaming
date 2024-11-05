from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, unbase64, split
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, ArrayType, FloatType, DateType

# Define the schema for parsing JSON from the Kafka 'redis-server' topic
# This schema outlines the structure of data changes in Redis
redis_schema = StructType([
    StructField("key", StringType(), True),
    StructField("existType", StringType(), True),
    StructField("Ch", BooleanType(), True),
    StructField("Incr", BooleanType(), True),
    StructField("zSetEntries", ArrayType(
        StructType([
            StructField("element", StringType(), True),
            StructField("Score", StringType(), True)
        ])
    ), True)
])

# Define the schema for the Customer JSON data from Redis
customer_json_schema = StructType([
    StructField("customer", StringType(), True),
    StructField("score", StringType(), True),
    StructField("email", StringType(), True),
    StructField("birthYear", StringType(), True)
])

# Define the schema for the 'stedi-events' topic, which contains customer risk information
event_schema = StructType([
    StructField("customer", StringType(), True),
    StructField("score", FloatType(), True),
    StructField("riskDate", DateType(), True)
])

# Initialize a Spark session for streaming
spark = SparkSession.builder.appName("KafkaRedisStreamingApp").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

# Load streaming data from Kafka topic 'redis-server' with startingOffsets to capture all data from the beginning
redis_stream_df = (
    spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "redis-server")
    .option("startingOffsets", "earliest")
    .load()
)

# Convert Kafka's 'value' column to a string format to facilitate JSON parsing
redis_stream_df = redis_stream_df.selectExpr("CAST(value AS STRING) AS json_value")

# Parse the JSON data based on the defined Redis schema, and create a temporary view for further SQL processing
redis_stream_df = redis_stream_df.withColumn("parsed_data", from_json("json_value", redis_schema)) \
                                 .select("parsed_data.*")
redis_stream_df.createOrReplaceTempView("RedisSortedSet")

# Extract the base64 encoded customer information from the first entry in the 'zSetEntries' array
encoded_customer_df = spark.sql("SELECT zSetEntries[0].element AS encodedCustomer FROM RedisSortedSet")

# Decode the base64 encoded data to reveal the actual customer information as JSON
decoded_customer_df = encoded_customer_df.withColumn("customer", unbase64(col("encodedCustomer")).cast("string"))

# Define schema for detailed customer data after decoding
customer_detail_schema = StructType([
    StructField("customerName", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("birthDay", StringType(), True)
])

# Parse the JSON in the decoded customer data and create a temporary view for querying
decoded_customer_df = decoded_customer_df.withColumn("customer_data", from_json("customer", customer_detail_schema)) \
                                         .select("customer_data.*")
decoded_customer_df.createOrReplaceTempView("CustomerRecords")

# Filter out records with non-null email and birthDay fields, and create a DataFrame from the result
email_and_birthday_df = spark.sql("""
    SELECT email, birthDay 
    FROM CustomerRecords 
    WHERE email IS NOT NULL AND birthDay IS NOT NULL
""")

# Extract the year from the 'birthDay' field and create a new DataFrame with just 'email' and 'birthYear'
email_and_birth_year_df = email_and_birthday_df.withColumn("birthYear", split(col("birthDay"), "-").getItem(0)) \
                                               .select("email", "birthYear")

# Stream the result to the console in append mode for real-time monitoring
query = email_and_birth_year_df.writeStream.outputMode("append").format("console").start()
query.awaitTermination()