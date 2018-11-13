// val sqlContext = new org.apache.spark.sql.SQLContext(sc)

// Read customer table (hbase) and generate custRdd   
val custRdd = sqlContext.sql("select c.acct_addr_id, substr(c.acct_addr_id,1,instr(c.acct_addr_id,':')-1) acct_id, substr(c.acct_addr_id,instr(c.acct_addr_id,':')+1) addr_id, c.income,  c.firstname, c.lastname, c.city, c.postalcode  from customer c")

// Register Rdd to register temp table cust_tbl 
custRdd.toDF.registerTempTable("cust_tbl")

// Read customer_trans (hbase) and generate custtransRdd
val custransRdd = sqlContext.sql("select ct.acct_addr_id,concat(cast(year(to_date(cast(unix_timestamp(ct.trans_date,'MM/dd/yyyy') as timestamp))) as string),lpad(cast(month(to_date(cast(unix_timestamp(ct.trans_date,'MM/dd/yyyy') as timestamp))) as string),2,'0')) trans_yyyymm, ct.trans_amount from customer_trans ct")

// Register Rdd to register table cust_trans_tbl 
custransRdd.toDF.registerTempTable("cust_trans_tbl")

// generate report for income between 100000 and 150000 and month = 201809 and trans_amount atleast 1000 
val rep1DF = sqlContext.sql("select c.acct_id, c.firstname, c.lastname, c.city, c.income, ct.trans_yyyymm trans_month, sum(ct.trans_amount) amount from cust_tbl c join cust_trans_tbl ct on c.acct_addr_id = ct.acct_addr_id and c.income between 100000 and 150000 and ct.trans_yyyymm = '201809' group by c.acct_id, c.firstname, c.lastname, c.city, c.income, ct.trans_yyyymm having sum(ct.trans_amount) >= 1000")

// save output to hdfs (customer/output/rep1)
rep1DF.rdd.repartition(1).map(x => (x(0)+","+x(1)+","+x(2)+","+x(3)+","+x(4)+","+x(5)+","+x(6))).saveAsTextFile("customer/output/rep1")

exit(1)
