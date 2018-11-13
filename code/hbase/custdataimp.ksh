
echo "load custInfo (customer details) into customter table of hbase from file (customer/output/custInfo.csv)"

hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dimporttsv.separator=',' -Dimporttsv.columns=HBASE_ROW_KEY,cust_info:firstname,cust_info:lastname,cust_info:income,cust_info:city,cust_info:postalcode customer customer/output/custInfo.csv

echo "load custTrans (Transaction) into customer_trans table of hbase from file (customer/output/custTrans.csv)"

hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dimporttsv.separator=',' -Dimporttsv.columns=HBASE_ROW_KEY,cust_trans:acct_addr_id,cust_trans:trans_date,cust_trans:trans_amount customer_trans customer/output/custTrans.csv

