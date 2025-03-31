from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType, BooleanType

KAFKA_BROKER = "kafka-broker-1:19092,kafka-broker-2:19092,kafka-broker-3:19092"
SOURCE_TOPIC = "financial_transactions"
AGGREGATED_TOPIC = "transactions_aggregated"
ANOMALIES_TOPIC = "transactions_anomalies"
CHECKPOINT_DIR = "./mnt/checkpoints"
STATE_DIR = "./mnt/spark-state"

spark = SparkSession.builder \
        .appName("Financial Transaction Processor") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR) \
        .config("spark.sql.streaming.stateStore.stateStoreDir", STATE_DIR) \
        .config("spark.sql.shuffle.partitions", 20) \
        .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

transaction_schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("transaction_time", LongType(), False),
    StructField("merchant_id", StringType(), False),
    StructField("transaction_type", StringType(), False),
    StructField("location", StringType(), False),
    StructField("payment_method", StringType(), False),
    StructField("is_international", BooleanType(), False),
    StructField("currency", StringType(), False)
])

kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", SOURCE_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

transactions_df = kafka_stream.selectExpr("CAST(value AS string)") \
    .select(from_json(col('value'), transaction_schema).alias("data")) \
    .select('data.*')

transactions_df = transactions_df.withColumn('transaction_time',
                                            (col('transaction_time') / 1000).cast('timestamp'))

aggregated_df = transactions_df.groupBy('merchant_id') \
    .agg(
        sum('amount').alias('total_amount'),
        count('*').alias('transactions_count')
    )

aggregated_query = aggregated_df \
    .withColumn("key", col('merchant_id').cast('string')) \
    .withColumn("value", to_json(struct( \
        col('merchant_id'),
        col('total_amount'),
        col('transactions_count')
    ))) \
    .selectExpr("key", "value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("topic", AGGREGATED_TOPIC) \
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/aggregates") \
    .outputMode("update") \
    .start().awaitTermination()







