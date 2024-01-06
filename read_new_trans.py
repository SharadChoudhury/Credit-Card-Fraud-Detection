from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from copy_to_hbase import batch_insert_data


spark = SparkSession \
    .builder \
    .appName("csvread") \
    .getOrCreate()

files_df = spark.read.csv("/user/hadoop/new_trans")
files_df.coalesce(1).write.format("csv").save("/user/hadoop/all_new_tran.csv")

batch_insert_data("all_new_tran.csv")


# spark-submit --py-files copy_to_hbase.py read_new_trans.py