from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import dao
import verify_rules 

hbase_instance = dao.HBaseDao.get_instance()
rules_instance = verify_rules.verifyRules.get_instance()

spark = SparkSession \
    .builder \
    .appName("KafkaRead") \
    .enableHiveSupport() \
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
status_df = parsed_trans.withColumn("status", status_udf("card_id", "amount", "postcode", "transaction_dt"))

# # write the transactions to card_transcations table in HBase
# hbase_instance.write_data(f'{status_df.card_id}_{status_df.amount}_{status_df.transaction_dt}', 
#                           {'cf1:member_id': status_df.member_id, 
#                            'cf1:postcode': status_df.postcode ,
#                            'cf1:pos_id': status_df.pos_id, 
#                            'cf1:status': status_df.status}, 
#                         'card_transcations')

#Write to Console
query = status_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start() 

query.awaitTermination()


