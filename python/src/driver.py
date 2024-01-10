from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from db import dao
from rules import verify_rules 
from copy_to_hbase import batch_insert_data
import subprocess

hbase_instance = dao.HBaseDao.get_instance()
rules_instance = verify_rules.verifyRules.get_instance()

spark = SparkSession \
    .builder \
    .appName("KafkaRead") \
    .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

# Read from Kafka
lines = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "18.211.252.152:9092") \
    .option("subscribe", "transactions-topic-verified") \
    .option("startingOffsets", "earliest") \
    .load()

# set schema for stream
tran_schema = StructType([
    StructField("card_id", StringType(), True),
    StructField("member_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("pos_id", StringType(), True),
    StructField("postcode", StringType(), True),
    StructField("transaction_dt", StringType(), True),
])

# Parse JSON using from_json
parsed_trans = lines.selectExpr("cast(value as string) as json_value") \
    .select(from_json("json_value", tran_schema).alias("tran_data")) \
    .selectExpr("tran_data.card_id", "tran_data.member_id", "tran_data.amount", "tran_data.postcode", "tran_data.pos_id", 
            "from_unixtime(unix_timestamp(tran_data.transaction_dt, 'dd-MM-yyyy HH:mm:ss')) as transaction_dt")

# create a udf with rule_check method from verify_rules.py
status_udf = udf(rules_instance.rule_check, StringType())

# apply the udf on each row in the stream, and create a new column status
status_df = parsed_trans.filter("card_id is not null") \
    .withColumn("status", status_udf("card_id", "amount", "postcode", "transaction_dt"))


# Write to Console
query1 = status_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start() 


# Write to CSV
query2 = status_df \
    .writeStream \
    .format("csv") \
    .option("path", "/user/hadoop/new_trans") \
    .option("header", True) \
    .option("checkpointLocation", "/user/hadoop/checkpoint") \
    .outputMode("append") \
    .start()
query2.awaitTermination()

