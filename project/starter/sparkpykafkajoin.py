from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType, FloatType

# Define schema for the "redis-server" Kafka topic which includes all changes in Redis - schema inference isn't automatic in Spark pre-3.0.0
redisSchema = StructType([
    StructField("key", StringType()),
    StructField("existType", StringType()),
    StructField("Ch", BooleanType()),
    StructField("Incr", BooleanType()),
    StructField("zSetEntries", ArrayType(
        StructType([
            StructField("element", StringType()),
            StructField("Score", StringType())
        ])
    ))
])

# Define schema for JSON data from Redis containing customer information - schema inference isn't automatic in Spark pre-3.0.0
customerJSONSchema = StructType([
    StructField("customer", StringType()),
    StructField("score", StringType()),
    StructField("email", StringType()),
    StructField("birthYear", StringType())
])

# Define schema for "stedi-events" Kafka topic containing customer risk information from Redis - schema inference isn't automatic in Spark pre-3.0.0
stediEventSchema = StructType([
    StructField("customer", StringType()),
    StructField("score", FloatType()),
    StructField("riskDate", DateType())
])

# Initialize Spark session
spark = SparkSession.builder.appName("STEDI_Application").getOrCreate()

# Set Spark log level to only show warnings
spark.sparkContext.setLogLevel('WARN')

# Read a streaming DataFrame from the Kafka topic "redis-server" as the source, configuring to read all previous records
redisStreamDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "redis-server") \
    .option("startingOffsets", "earliest") \
    .load() 

# Convert the "value" column in the streaming DataFrame to STRING type
redisStreamDF = redisStreamDF.selectExpr("CAST(value AS string) value")

# Parse JSON objects in the "value" column to separate fields
redisStreamDF.withColumn("value", from_json("value", redisSchema)) \
    .select(col('value.existType'), col('value.Ch'), col('value.Incr'), col('value.zSetEntries')) \
    .createOrReplaceTempView("RedisSortedSet")

# Select the "element" field from the first element in the array of structures, creating a column called "encodedCustomer"
encodedCustomerDF = spark.sql("SELECT zSetEntries[0].element AS encodedCustomer FROM RedisSortedSet")

# Decode the base64-encoded "encodedCustomer" column to readable JSON format
decodedCustomerDF = encodedCustomerDF.withColumn("customer", unbase64(encodedCustomerDF.encodedCustomer).cast("string"))

# Define schema for customer data
customerSchema = StructType([
    StructField("customerName", StringType()),
    StructField("email", StringType()),
    StructField("phone", StringType()),
    StructField("birthDay", StringType())
])

# Parse JSON in the customer field and create a temporary view
decodedCustomerDF.withColumn("customer", from_json("customer", customerSchema)) \
    .select(col('customer.*')) \
    .createOrReplaceTempView("CustomerRecords")

# Filter records with non-null "email" and "birthDay" columns
filteredCustomerDF = spark.sql("""
    SELECT *
    FROM CustomerRecords
    WHERE email IS NOT NULL AND birthDay IS NOT NULL
""")

# Extract the birth year from the "birthDay" column
filteredCustomerDF = filteredCustomerDF.withColumn('birthYear', split(filteredCustomerDF.birthDay, "-").getItem(0))

# Select only the "email" and "birthYear" columns for further use
emailBirthYearDF = filteredCustomerDF.select(col('email'), col('birthYear'))

# Read a streaming DataFrame from the Kafka topic "stedi-events" as the source, configuring to read all previous records
eventsStreamDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "stedi-events") \
    .option("startingOffsets", "earliest") \
    .load()

# Convert the "value" column in the streaming DataFrame to STRING type
eventsStreamDF = eventsStreamDF.selectExpr("CAST(value AS string) value")

# Parse JSON in the "value" column and create a temporary view
eventsStreamDF.withColumn("value", from_json("value", stediEventSchema)) \
    .select(col('value.customer'), col('value.score'), col('value.riskDate')) \
    .createOrReplaceTempView("CustomerRisk")

# Select the customer and score from the temporary view
customerRiskDF = spark.sql("SELECT customer, score FROM CustomerRisk")

# Join the streaming DataFrames on the email field to combine risk score and birth year in one DataFrame
combinedScoreDF = customerRiskDF.join(emailBirthYearDF, expr("customer = email"))

# Write the joined data to a new Kafka topic in JSON format
outputQuery = combinedScoreDF.selectExpr("TO_JSON(struct(*)) AS value").writeStream \
    .outputMode('append') \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("checkpointLocation", "checkpoint") \
    .option("topic", "stedi-customer-risk-score-topic") \
    .start()

# Also write the joined data to the console for viewing
consoleOutputQuery = combinedScoreDF.selectExpr("TO_JSON(struct(*)) AS value").writeStream \
    .outputMode('append') \
    .format('console') \
    .option('truncate', False) \
    .start()

outputQuery.awaitTermination()
consoleOutputQuery.awaitTermination()