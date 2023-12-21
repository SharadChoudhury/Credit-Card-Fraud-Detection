-- Hive commands to create the tables:

-- card_member and member_score are internal tables created by sqoop

-- transactions Data Table schema creation
CREATE TABLE card_transactions (
    card_id STRING,
    member_id STRING,
    amount DOUBLE,
    postcode INT,
    pos_id STRING,
    transaction_dt STRING,
    status STRING
) 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
TBLPROPERTIES ("skip.header.line.count"="1");

--LOCATION '/user/hive/warehouse/card_transactions'


-- load data into it from the csv file
LOAD DATA INPATH '/user/hadoop/card_transactions.csv' INTO TABLE card_transactions;

select * from card_transactions limit 5;



-- Creating the lookup table in Hive:
-- Create a temporary table with moving average and standard deviation:
CREATE TEMPORARY TABLE temp_transactions AS
SELECT b.*, 
    AVG(amount) OVER (PARTITION BY card_id ORDER BY rn ROWS BETWEEN CURRENT ROW AND 9 FOLLOWING) AS moving_avg,
    STDDEV(amount) OVER (PARTITION BY card_id ORDER BY rn ROWS BETWEEN CURRENT ROW AND 9 FOLLOWING) AS std_dev
FROM
   ( SELECT *, 
        row_number() over(PARTITION BY card_id ORDER BY transaction_dt desc) as rn
    FROM
        (SELECT
            card_id ,
            member_id ,
            amount ,
            postcode ,
            pos_id ,
            from_unixtime(unix_timestamp(transaction_dt , 'dd-MM-yyyy HH:mm:ss')) as transaction_dt,
            status 
        FROM card_transactions t
        WHERE status = 'GENUINE'
        ) AS a
    ) as b
WHERE rn <= 10;


-- Create the final lookup table
CREATE TABLE genuine_transactions AS
SELECT
    card_id,
    postcode AS last_postcode,
    transaction_dt AS last_transaction_dt,
    score AS credit_score,
    ROUND(moving_avg + 3*std_dev, 2) as ucl
FROM temp_transactions t 
left join 
member_score m
ON t.member_id = m.member_id
WHERE rn = 1;


-- mapping hive table to hbase table:
create table hive_lookup_table(
    `card_id` string, 
    `last_postcode` string, 
    `last_transaction_dt` string,
    `credit_score` INT,
    `ucl` DOUBLE
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,cf1:last_postcode,cf1:last_transaction_dt,
cf1:credit_score,cf1:ucl")
TBLPROPERTIES ("hbase.table.name" = "hbase_lookup_table");

-- inserting records to hive_lookup_table automatically inserts to hbase_lookup_table
insert overwrite table hive_lookup_table
SELECT * FROM genuine_transactions;


----------------------------------------------------------------------------------------

-- mapping transactions table to hbase table
CREATE TABLE hive_card_transactions(
    card_id STRING,
    member_id STRING,
    amount DOUBLE,
    postcode INT,
    pos_id STRING,
    transaction_dt STRING,
    status STRING
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,cf1:member_id,cf1:amount,cf1:postcode,cf1:pos_id,
cf1:transaction_dt,cf1:status")
TBLPROPERTIES ("hbase.table.name" = "hbase_card_transactions");

insert overwrite table hive_card_transactions
SELECT * FROM card_transactions;