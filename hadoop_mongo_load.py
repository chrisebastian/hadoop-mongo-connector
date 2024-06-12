import os
from pysparksql import SparkSession
import pandas as pd
from pyhive import hive
from collections import OrderedDict
from datetime import datetime
import subprocess
from pyspark.sql import SQLContext
import multiprocessing 
from multiprocessing import Pool
import sys

jar_directory = os.path.join(os.environ['MONGO_JARS_DIR'], 'spark_mongo_jars/*')

def write_hdfs_data(path):
        mongo_host = sys.argv[1]
        mongo_db = sys.argv[2]
        mongo_user = sys.argv[3]
        mongo_password = sys.argv[4]
        mongo_collection = sys.argv[5]
        mongo_url = "mongodb://" + mongo_user + ":" + mongo_password + "@" + mongo_host + "/" + mongo_db + "." + mongo_collection

        spark = SparkSession \
                .builder \
                .appName("your_app_name") \
                .master("local[*]") \
                .config("spark.driver.memory", "20g") \
                .config("spark.mongodb.input.uri", mongo_url) \
                .config("spark.mongodb.output.uri", mongo_url) \
                .config("spark.driver.extraClassPath", jar_directory) \
                .config("spark.driver.host", "127.0.0.1") \
                .config("spark.driver.bindAddress", "127.0.0.1") \
                .config("spark.ui.port", "4208") \
                .getOrCreate()
        
        sc = spark.sparkcontext
        sqlContext = SQLContext(sc)
        path_alone = "/a" + path.split("/a")[1]
        hdfs_data = sqlcontext.read.parquet(path_alone)
        print(datetime.now())
        hdfs_data.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").save()
        print(datetime.now())

if __name == '__main__':
        try:
              final_db = sys.argv[6]
              final_table = sys.argv[7]
              hdfs_path = sys.argv[8]
              count_cmd = 'hdfs dfs -ls /your_hdfs_path/{}/{}/* | wc -l'.format(final_db, final_table)
              count = subprocess.check_output(count_cmd, shell=True).decode('utf8')
              print("Number of parquet files " + count)
              if int(count) > 0 :
                    files_cmd = 'hdfs dfs -ls /your_hdfs_path/{}/{}/* | wc -l'.format(final_db, final_table)
                    files = subprocess.check_output(files_cmd, shell=True).decode('utf8').strip().split('\n')
                    files.pop(0)
                    p = Pool(10)
                    with p:
                        p.map(write_hdfs_data, files)

        except Exception as e:
              print(e)
              exit(1)
        
