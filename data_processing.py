from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

# create a SparkSession
spark = SparkSession.builder.appName("MobileMoneyAnalysis").getOrCreate()

# create a schema for the mobile money data
schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("sender_phone_number", StringType(), True),
    StructField("receiver_phone_number", StringType(), True),
    StructField("transaction_amount", IntegerType(), True),
    StructField("transaction_time", StringType(), True)
])

# read the mobile money data from Kafka
mobile_money_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "mobile_money") \
    .option("startingOffsets", "earliest") \
    .load() \
    .select(from_json(col("value").cast("string"), schema).alias("data"))

# select relevant columns and cast transaction time to timestamp
mobile_money_processed_df = mobile_money_df.select(
    col("data.transaction_id").alias("transaction_id"),
    col("data.sender_phone_number").alias("sender_phone_number"),
    col("data.receiver_phone_number").alias("receiver_phone_number"),
    col("data.transaction_amount").alias("transaction_amount"),
    to_timestamp(col("data.transaction_time"), "yyyy-MM-dd HH:mm:ss").alias("transaction_time")
)

# group by sender phone number and aggregate total transaction amount
total_transactions_df = mobile_money_processed_df \
    .groupBy("sender_phone_number") \
    .agg(sum("transaction_amount").alias("total_transaction_amount"))

# add a rank column to the total transactions dataframe
window_spec = Window.orderBy(col("total_transaction_amount").desc())
ranked_transactions_df = total_transactions_df.withColumn("rank", rank().over(window_spec))

# write the processed data to a parquet file
mobile_money_processed_df \
    .writeStream \
    .format("parquet") \
    .option("path", "/path/to/processed/data") \
    .option("checkpointLocation", "/path/to/checkpoint/dir") \
    .start()

# write the total transactions dataframe to a console for testing purposes
total_transactions_query = total_transactions_df \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

# write the ranked transactions dataframe to a console for testing purposes
ranked_transactions_query = ranked_transactions_df \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

# wait for the streaming queries to finish
spark.streams.awaitAnyTermination()
