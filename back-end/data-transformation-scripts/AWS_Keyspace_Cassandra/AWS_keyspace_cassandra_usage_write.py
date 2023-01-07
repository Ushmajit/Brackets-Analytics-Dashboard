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


import uuid

# Below code is used to perform extract, load and transform of all raw input data which is over 15000 JSON files 
# that contains input logs of Bracket's users activities. We filter out corrupt files if any and extract useful data fields 
# that we would require to do our analysis from nested JSON structure by parsing through these log files. We are using 
# PYSpark operations like data frames and RDDs to parallelize the operation which would efficiently work on approximately 5 GB data files
# and then we perform ETL on this which is then stored in AWS KeySpace Cassandra table that is the clean output after performing our ETL.


# Removing corrupt file if any
def remove_corrupt(input):
    try:
        return json.loads(input)
    except Exception as e:
        pass

#  Extract client analytics from nested json and add timestamp to it from every file
def get_client_analytics(value):
        client_analytics = value['clientAnalytics']
        return client_analytics


# Performing business logic to extract usage fields required to do analysis from the nested JSON
def pre_process_usage(val_client_analytics):
    unix_time_stamp = val_client_analytics['unixTimestampUTC']
    year = datetime.fromtimestamp(unix_time_stamp/1000).year
    if year>2014:
        brackets_uuid = val_client_analytics['uuid']
        continent = val_client_analytics['geolocation']['continent']
        country = val_client_analytics['geolocation']['country']
        date = datetime.fromtimestamp(unix_time_stamp/1000).date()
        events = val_client_analytics['events']
        for usage_type in list(events['usage'].keys()):
            if usage_type=='fileOpen':
                for language in list(events['usage']['fileOpen'].keys()):
                    yield [str(uuid.uuid4()),brackets_uuid,usage_type,language,continent,country,date]
            else:
                yield [str(uuid.uuid4()),brackets_uuid,usage_type,"",continent,country,date]



# Filter out only those client analytics which contains events in them from nested json
def get_client_analytics_with_events(val_client_analytics):
    if 'events' in val_client_analytics:
        return val_client_analytics


# Filter out only those events which contains usage from nested json
def get_events_with_usage(val_client_analytics):
    if 'usage' in val_client_analytics['events']:
        return val_client_analytics


# Creating usage schema for usage table dataframe
def get_usage_schema():
    return StructType([
        StructField('uuid', StringType(), True),
        StructField('brackets_uuid', StringType(), True),
        StructField('usage_type', StringType(), True),
        StructField('language', StringType(), True),
        StructField('continent', StringType(), True),
        StructField('country', StringType(), True),
        StructField('date', DateType(), True)
        ])


# Write usage dataframe to AWS KeySpace Cassandra table brackets_usage under keyspace brackets_data_analytics_tuba
def write_to_cassandra(df_usage):
    df_usage.write.format("org.apache.spark.sql.cassandra").option("keyspace", "brackets_data_analytics_tuba").option("table", "brackets_usage").mode("APPEND").save()


# Creating RDDs and Dataframes to perform ETL over approximately 16,000 JSON files 
# thereby removing corrupt files and performing business logic eventually writing usage table to AWS KeySpace
def main(inputs):
    rdd = sc.textFile(inputs).map(remove_corrupt).filter(lambda x:type(x)==dict)
    client_analytics = rdd.flatMap(get_client_analytics).filter(get_client_analytics_with_events).cache()
    usage_rdd = client_analytics.filter(get_events_with_usage)
    usage=usage_rdd.flatMap(pre_process_usage)
    df_usage=spark.createDataFrame(usage,get_usage_schema())
    write_to_cassandra(df_usage)



if __name__ == '__main__':
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
    inputs = sys.argv[1]
    main(inputs)