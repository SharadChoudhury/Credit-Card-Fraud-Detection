from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


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
    .option("maxOffsetsPerTrigger", 500) \
    .load()



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


#Write to Console
query = parsed_trans \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start() 



query.awaitTermination()