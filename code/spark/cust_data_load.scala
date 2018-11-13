//val sqlContext = new org.apache.spark.sql.SQLContext(sc)

//loading customer data

case class Customer(account_id:String, firstName:String, lastName:String, income:Double, address_id:String)

//1. load datafrom customer.csv
//2. split by ","
//3. filter records having null values in account_id, firstname and lastname 
//4. map to Customer class and create Rdd

val customerRdd = sc.textFile("customer/customer.csv").map(_.split(",")).filter(x => ((x(0) != null) && (x(1) != null) && (x(2) != null))).map(x => Customer(x(0),x(1),x(2),x(3).toDouble,x(4)))

// print RDD
// customerRdd.collect().foreach(println)

// register RDD to temptable Cust

customerRdd.toDF().registerTempTable("custTbl")

//loading customer address detail

case class CustAddr(address_id:String, city:String, postalCode:String)

//1. load datafrom cust_addr.csv
//2. split by ","
//3. filter records having null values in address_id 
//4. map to Customer class and create Rdd

val custAddrRdd = sc.textFile("customer/cust_addr.csv").map(_.split(",")).filter(x => (x(0) != null)).map(x => CustAddr(x(0),x(1),x(2)))

//print RDD
//custAddrRdd.collect().foreach(println)

//register temp table CustAddr

custAddrRdd.toDF().registerTempTable("CustAddr")


// join customer and customer address on address_id by using spark SQL

val custInfo = sqlContext.sql("select c.account_id,c.address_id, c.firstName, c.lastName, c.income, ca.city, ca.postalCode from CustTbl c join CustAddr ca on c.address_id = ca.address_id")

// save data in hdfs (customer/output/custInfo.csv)

custInfo.rdd.repartition(1).map(x => (x(0)+":"+x(1)+","+x(2)+","+x(3)+","+x(4)+","+x(5)+","+x(6))).saveAsTextFile("customer/output/custInfo.csv")

//loading customer Transaction

case class CustTrans(trans_id:Long, account_id:String, address_id:String, transDate:String, transAmt:Double)

//1. load datafrom cust_trans.csv
//2. split by ","
//3. filter records having null values in account_id, address_id, trans_date 
//4. map to Customer class and create Rdd

val custTransRdd = sc.textFile("customer/cust_trans.csv").map(_.split(",")).filter(x => ((x(1) != null) && (x(2) != null) && (x(3) != null) && (x(4) != null))).map(x => CustTrans(x(0).toLong,x(1),x(2),x(3),x(4).toDouble))

// print RDD
// custTransRdd.collect().foreach(println)

// save data in hdfs (customer/output/custTrans.csv)

custTransRdd.toDF.repartition(1).map(x => (x(0)+","+x(1)+":"+x(2)+","+x(3)+","+x(4))).saveAsTextFile("customer/output/custTrans.csv")


exit(1);
