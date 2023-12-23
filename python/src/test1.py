from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession \
    .builder \
    .appName("KafkaRead2") \
    .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

# Read from Kafka
lines = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "18.211.252.152:9092") \
    .option("subscribe", "real-time-project") \
    .load()

# Define schema of a single order
order_schema = StructType([
    StructField("items", ArrayType(
        StructType([
            StructField("SKU", StringType(), True),
            StructField("title", StringType(), True),
            StructField("unit_price", DoubleType(), True),
            StructField("quantity", IntegerType(), True),
        ])
    ), True),
    StructField("type", StringType(), True),
    StructField("country", StringType(), True),
    StructField("invoice_no", LongType(), True),
    StructField("timestamp", StringType(), True),
])

# Parse JSON using from_json
parsed_orders = lines.selectExpr("cast(value as string) as json_value") \
    .select(from_json("json_value", order_schema).alias("order_data")) \
    .select("order_data.*")

#Write to Console
query = parsed_orders \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="1 minute") \
    .start() 

query.awaitTermination()