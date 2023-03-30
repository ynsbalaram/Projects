# Customer 360 Pipeline implementation with Amazon S3,Hive,Hbase, Spark,Airflow & Gmail for notifications

## List of components used for the pipeline.
### VSCode Editor
### Docker 
### Itversity Lab for distributed environment
### Amazon S3
### Sqoop
### Hive
### Spark
### HBase
### Airflow
### Gmail SMTP Server 

#Problem Statement
Order processing team will put Text files  in s3 buckets at 5pm-6pm daily.
Customer related information team populated all the customer information in a mysql/oracle db.

#Solution Approach
Step-1:-Download the files from s3 into local(edge node)
step-2:-Sqoop will fetch the customer information from my sql and dump to Hive
step-3:-upload s3 order file to HDFS location
step-4:-Spark program to process the closed orders against the Customer
step-5:-create hive table from the output path available in step 4
step-6:-upload it into Hbase
step-7:-slack channel for communication
       (Success/failure of the pipeline)


## Pipeline implementation involves the below steps 

Step1: Checking if the orders.csv file is available in the S3 bucket

Step2.1: Once the orders.csv file is available , we are downloading the file from the Amazon S3 bucket to the edge node

Step2.2: Sqoop import command to fetch the customers table from sql to the hive
		(complete dump at once no incremental load and non partitioned) 

Step4: Upload the orders.csv file from the edge node to the HDFS location

Step5: Spark Jar to process the orders.csv file to filter the closed orders and putting in an output path

Step6: Create hive table from the Spark output 

Step7: Create hive-Hbase TABLE for fast look up.


## Prerequisites

 Please make sure Docker Desktop is installed and up on running.

 


