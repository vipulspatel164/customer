// create external table customer in hive for reference of hbase table customer 
// this table is used in spark to do analysis on using spark 

CREATE EXTERNAL TABLE customer (acct_addr_id STRING, firstname STRING, lastname STRING, income DOUBLE, city STRING, postalcode STRING) STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key, cust_info:firstname, cust_info:lastname, cust_info:income, cust_info:city, cust_info:postalcode') TBLPROPERTIES('hbase.table.name' = 'customer')

// create external table customer_trans in hive for reference of hbase table customer_trans 
// this table is used in spark to do analysis on using spark 

CREATE EXTERNAL TABLE customer_trans (trans_id BIGING, acct_addr_id STRING, trans_date DATE, trans_amount DOUBLE) STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key, cust_trans:acct_addr_id, cust_trans:trans_date, cust_trans:trans_amount') TBLPROPERTIES('hbase.table.name' = 'customer_trans')

