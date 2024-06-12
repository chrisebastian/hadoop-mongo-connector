# hadoop-mongo-connector

Problem Statement:

Hadoop to MongoDB data load using CSV or JSON files won't be sufficient solution if the volume of row more than 150 Million. 

Solution:

Using Hadoop PySpark connector without exporting the records to files, we could load directly to mongoDB.
This Respository created to showcase the Hadoop to MongoDB connector using PySpark.

Prerequisites:

1. Hadoop
2. MongoDB
3. Python
4. Shell Script
5. Mongo Shell

Usage:

1. Download these two JAR files into your local system.
    mongo-spark-connector_2.11-2.3.4.jar
    mongo-java-driver-3.12.6.jar
2. Copy python file hadoop_mongo_load.py
3. Create parquet Hadoop table: Parquet table is expected.
```
   CREATE TABLE final_table(
    col1 int, col3 string
    ) 
    PARTITIONED BY (load_date string) 
    stored as PARQUET
    LOCATION
    'hdfs:///your_path/db_name/table_name'
```
5. Insert records from any source to above table.
6. Partition column should be one date column for faster data load.
7. From shell script or directly you can call python file using following command
```
   python ./hadoop_mongo_load.py ${mongo_host} ${mongo_db_name} ${mongo_user} ${mongo_password} ${collection_name} ${hadoop_db} ${hadoop_table}
```

Output:

Data will gets loaded with multiple process. Data should be loaded within 5 to 10 Mins minimum 100 Million records.


