 
#############################################################################################################################################################
#   Description : This Shell script will do the following
#     	         1. load customer.csv, cust_addr.csv and cust_trans.csv from hdfs and join datasource customer and cust_addr and write in combine data in hdfs
#                2. import data in hbase from generated output of #1 using hbase importtsv
#                3. access hbase table and generate report 
#                4. moving generated output to processed folder
##############################################################################################################################################################
trap 'echo aborted \(Sorry\) ;  exit 1' 1 2 3 5 15

# check and define usage

if [ "$1" = "help"] | ["$1" = ""];
then
    echo "Usage : $0 [-s stage_list]"
    echo "Ex: $0 -s 1,2"   
    exit 1
fi

if [ "$1" = "-s" ]
then
     stage_list=$2
     FROM_STAGE=`echo "$stage_list" | cut -f1 -d","`
     TO_STAGE=`echo "$stage_list" | cut -f2 -d","`
fi

# Logfile name
LOGFILE=${LOGDIR_BIN}/load_tel_metered.log

echo "from stage : $FROM_STAGE"
echo "to stage : $TO_STAGE"

CURR_DATE=`date +%m%d%Y`


CURR_STAGE=0
#######################################################################################################################################################
#                         Stage 1
#          load customer.csv, cust_addr.csv and cust_trans.csv from hdfs and join datasource customer and cust_addr and write in combine data in hdfs
#######################################################################################################################################################
    
CURR_STAGE=1

# check if current stage is within the parameters specified
    
if [ $CURR_STAGE -ge $FROM_STAGE -a $CURR_STAGE -le $TO_STAGE  ]
then
#
    echo "executing stage $CURR_STAGE ..."
    
    # spark-submit --master spark://localhost:7077  ./code/spark/cust_data_load.scala 1000
   
    spark-shell -i ./spark/cust_data_load.scala

    echo "stage $CURR_STAGE completed successfully"
#
fi  # check stage

#############################################################################################
#                         Stage 2
#                import data in hbase from generated output of #1 using hbase importtsv
##############################################################################################
    
CURR_STAGE=2

# check if current stage is within the parameters specified
    
if [ $CURR_STAGE -ge $FROM_STAGE -a $CURR_STAGE -le $TO_STAGE  ]
then

	echo "executing stage $CURR_STAGE ..."

	./hbase/custdataimp.ksh
	
	echo "stage $CURR_STAGE completed successfully"

fi  # check stage



##########################################################################
#                               Stage 3
#                       access hbase table and generate report
##########################################################################

CURR_STAGE=3

# check if current stage is within the parameters specified

if [ $CURR_STAGE -ge $FROM_STAGE -a $CURR_STAGE -le $TO_STAGE ]
then

    echo "executing stage $CURR_STAGE ..."
    
         # spark-submit --master spark://localhost:7077 ./code/spark/cust_data_load.scala 1000

	 spark-shell -i ./spark/cust_rep1.scala

    echo "stage $CURR_STAGE completed successfully"
	    
fi  # check stage

##########################################################################
#                               Stage 4
#                       moving generated data in proceessed folder
##########################################################################

CURR_STAGE=4

# check if current stage is within the parameters specified

if [ $CURR_STAGE -ge $FROM_STAGE -a $CURR_STAGE -le $TO_STAGE ]
then

    echo "executing stage $CURR_STAGE ..."
    
         dt=`date +%Y%m%d`

	 echo moving generated file to processed folder

         hdfs dfs -mkdir customer/processed/$dt
	 hdfs dfs -mv customer/output/* customer/processed/$dt

	 echo moving logfile to processed folder under log directory

	 mv ../log/cust_data.log ../log/processed/cust_data_$dt.log

    echo "stage $CURR_STAGE completed successfully"
	    
fi  # check stage
##########################################################################
# success notification

echo "End success $0 for date : `date`"

# exit with success
exit 0
