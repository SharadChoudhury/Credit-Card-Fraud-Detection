# Credit-Card-Fraud-Detection

Credit card fraud detection of real-time transaction data

 ![image](https://github.com/SharadChoudhury/Credit-Card-Fraud-Detection/assets/65325622/761ba2db-8955-47ea-92e7-2d2c0e9ed190)


**Dataset for Past cards transactions:** `card_transactions.csv`

Create an AWS EMR cluster with the following services:
1. HBase
2. Hive
3. Spark
4. Hue

**Install Happybase:**
```bash
sudo su
sudo yum update
yum install gcc
sudo yum install python3-devel
pip install happybase
pip install pandas
```

**To enable sqoop connection to RDS:**
```bash
sudo su
wget https://de-mysql-connector.s3.amazonaws.com/mysql-connector-java-8.0.25.tar.gz
tar -xvf mysql-connector-java-8.0.25.tar.gz
cd mysql-connector-java-8.0.25/
sudo cp mysql-connector-java-8.0.25.jar /usr/lib/sqoop/lib/
```

## Batch layer tasks

**Create card_transactions table in Hbase and load data in it using the csv file and happybase.**
```bash
create 'card_transactions', 'cf1'
```
Run the copy_to_hbase.py in the same directory as your csv file to insert data in HBase table in batches.


**Import the card_member and member_score table into Hive using Sqoop**
```bash
sqoop import \
--connect jdbc:mysql://upgradawsrds1.cyaielc9bmnf.us-east-1.rds.amazonaws.com/cred_financials_data \
--table card_member \
--username upgraduser --password upgraduser \
--hive-import \
--hive-table card_member \
--create-hive-table \
--fields-terminated-by ',' \
-m 1
```

```bash
sqoop import \
--connect jdbc:mysql://upgradawsrds1.cyaielc9bmnf.us-east-1.rds.amazonaws.com/cred_financials_data \
--table member_score \
--username upgraduser --password upgraduser \
--hive-import \
--hive-table member_score \
--create-hive-table \
--fields-terminated-by ',' \
-m 1
```

**Creating Lookup table**

Copy csv file to hadoop:
```bash
hadoop fs -put /home/hadoop/card_transactions.csv /user/hadoop/
```

We'll create the lookup table which stores the following details for each unique card_id
- Card id 
- Upper control limit (UCL) 
- Postcode of the last transaction 
- Transaction date of the last transaction
- The credit score of the member

We'll create this table using the member_score and card_transactions table in Hive.

Run commands in Hive_commands.sql in Hive shell to create the lookup table using Hive-Hbase integration
Now, the lookup table in Hbase is hbase_lookup_table.


## Streaming layer tasks
**Ingesting new records from Kafka**

Now, that we have the card_transactions table and the hbase_lookup_table in HBase:

- We can read the incoming records from Kafka and apply the set of rules on each record
and decide the ‘status’ of the transaction based on the rules as 'GENUINE' or 'FRAUD'.
- Then we update the postcode and last transaction date for the card_id for the Genuine
transactions.
- Finally, we write all the transactions with their status to card_transactions Hbase table.
- Also, write the stream to the console.

Copy the python folder to EMR with the same file structure. Then run :
```bash
cd python/src
zip src.zip __init__.py rules/* db/*
export SPARK_KAFKA_VERSION=0.10
```

Now, run the driver.py file using the spark-submit command:
```bash
spark-submit --py-files src.zip --files uszipsv.csv --packages org.apache.spark:spark-sql-kafka-0- 10_2.11:2.4.5 driver.py
```
