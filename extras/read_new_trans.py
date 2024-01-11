# this script reads all the new transactions from the csv file and writes them to HBase
from copy_to_hbase import batch_insert_data
import subprocess


# moving the created csv to /home/hadoop
subprocess.run('hdfs dfs -get /user/hadoop/new_trans/part*', shell=True)
# rename it to all_new_trans.csv
subprocess.run('mv part* all_new_trans.csv', shell=True)

# run the batch_insert_data method on the imported csv file
batch_insert_data('all_new_trans.csv')

