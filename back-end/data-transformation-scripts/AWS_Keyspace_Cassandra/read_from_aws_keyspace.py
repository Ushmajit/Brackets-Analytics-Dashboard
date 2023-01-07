from pyspark import SparkConf, SparkContext
from credentials import aws
import sys
import json
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType,DateType,StructType,StructField
from datetime import datetime
from pyspark.sql.functions import col
from cassandra.cluster import Cluster
from ssl import SSLContext, PROTOCOL_TLSv1_2 , CERT_REQUIRED
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import BatchStatement, SimpleStatement, BatchType
from cassandra import ConsistencyLevel


# Below method is reading values from AWS Keyspace Cassandra where we store our 
# brackets_summary and brackets_usage table under brackets_data_analytics_tuba keyspace
if __name__ == '__main__':
    # AWS username and passowrd is coming from config file inside aws_user_credentials which is git ignored for security reasons. 
    # To run it, replace aws.username with AWS username and aws.password with AWS password.
    spark = SparkSession.builder \
    .appName('TUBA Spark')\
    .config('spark.jars.packages', 'com.datastax.spark:spark-cassandra-connector_2.11-2.5.1')\
    .config('spark.cassandra.connection.host', 'cassandra.us-east-1.amazonaws.com') \
    .config('spark.cassandra.connection.port', '9142') \
    .config('spark.cassandra.connection.ssl.enabled','true') \
    .config("spark.cassandra.auth.username",aws.username)\
    .config("spark.cassandra.auth.password",aws.password) \
    .getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext

    # Loading data from brackets_summary table from AWS Keyspace Cassandra table and putting it into dataframe
    df_summary = spark.read.format("org.apache.spark.sql.cassandra").options(table="brackets_summary", keyspace="brackets_data_analytics_tuba").load()
    df_summary.show()

    # Loading data from brackets_usage table from AWS Keyspace Cassandra table and putting it into dataframe
    df_usage = spark.read.format("org.apache.spark.sql.cassandra").options(table="brackets_usage", keyspace="brackets_data_analytics_tuba").load()
    df_usage.show()
