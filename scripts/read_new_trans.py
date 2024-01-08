# this script reads all the new transactions from the csv files in HDFS and writes them to HBase
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from copy_to_hbase import batch_insert_data
import subprocess


spark = SparkSession \
    .builder \
    .appName("csvread") \
    .getOrCreate()

files_df = spark.read.csv("/user/hadoop/new_trans")
files_df.coalesce(1).write.format("csv").save("/user/hadoop/all_new_trans")


# moving the created csv to /home/hadoop
subprocess.run('hdfs dfs -get /user/hadoop/all_new_trans/part*', shell=True)
subprocess.run('mv part* all_new_trans.csv', shell=True)

# run the batch_insert_data method on the imported csv file
batch_insert_data('all_new_trans.csv')

