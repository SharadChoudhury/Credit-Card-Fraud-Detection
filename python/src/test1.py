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

# create udfs
def calculate_total_cost(items, order_type):
    if items is not None:
        total_cost=0
        for item in items:
            total_cost += item[2]*item[3]
        # -ve indexing not supported in spark
        return -total_cost if order_type == 'RETURN' else total_cost
    else:
        return 0

total_costs_udf = udf(calculate_total_cost, DoubleType())
total_items_udf = udf(lambda item_list: len(item_list), IntegerType())
is_order_udf = udf(lambda order_type: 1 if order_type == "ORDER" else 0, IntegerType())
is_return_udf = udf(lambda order_type: 1 if order_type == "RETURN" else 0, IntegerType())


# Calculate metrics
calculated_metrics = parsed_orders \
    .withColumn("total_cost", total_costs_udf("items", "type")) \
    .withColumn("total_items", total_items_udf(parsed_orders.items)) \
    .withColumn("is_order", is_order_udf(parsed_orders.type)) \
    .withColumn("is_return", is_return_udf(parsed_orders.type)) \
    .withColumn("Timestamp", to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss")) \
    .select("invoice_no","country","Timestamp",
            format_number("total_cost", 2).alias("total_cost"),
            "total_items","is_order","is_return")


#Write to Console
query = calculated_metrics \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="1 minute") \
    .start() 

query.awaitTermination()