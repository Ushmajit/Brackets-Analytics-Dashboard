from pyspark import SparkConf, SparkContext
import sys
import json
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType,DateType,StructType,StructField
from datetime import datetime

import uuid

# Below code is used to perform extract, load and transform of all raw input data which is over 15000 JSON files 
# that contains input logs of Bracket's users activities. We filter out corrupt files if any and extract useful data fields 
# that we would require to do our analysis from nested JSON structure by parsing through these log files. We are using 
# PYSpark operations like data frames and RDDs to parallelize the operation which would efficiently work on approximately 5 GB data files
# and then we perform ETL on this which is then stored in Parquet files partitioned by date that is the clean output after performing our ETL.

#  Extract client analytics from nested json and add timestamp to it from every file
def get_client_analytics(value):
        client_analytics = value['clientAnalytics']
        time_stamp=value['unixTimestampUTCAtServer']
        for client_analytic in client_analytics:
            client_analytic['serverTimeStamp']=time_stamp
            yield client_analytic

            
            
# Performing business logic to extract usage fields required to do analysis from the nested JSON
def pre_process_usage(val_client_analytics):
    unix_time_stamp = val_client_analytics['serverTimeStamp']
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
                
# Performing business logic to extract summary fields required to do analysis from the nested JSON
def pre_process_summary(val_client_analytics):
    unix_time_stamp = val_client_analytics['serverTimeStamp']
    year = datetime.fromtimestamp(unix_time_stamp/1000).year
    if year>2014:
        brackets_uuid = val_client_analytics['uuid']
        continent = val_client_analytics['geolocation']['continent']
        country = val_client_analytics['geolocation']['country']
        date = datetime.fromtimestamp(unix_time_stamp/1000).date()
        events = val_client_analytics['events']
        platform = list(events['PLATFORM']['os'].keys())[0]
        if 'THEMES' in events:
            theme = list(events['THEMES']['bracketsTheme'].keys())[0]     
            yield [str(uuid.uuid4()),brackets_uuid,platform,theme,continent,country,date]
        else:
            yield [str(uuid.uuid4()),brackets_uuid,platform,"",continent,country,date]

            
            
# Filter out only those client analytics which contains events in them from nested json
def get_client_analytics_with_events(val_client_analytics):
    if 'events' in val_client_analytics:
        return val_client_analytics

    
    
# Filter out only those events which contains usage from nested json
def get_events_with_usage(val_client_analytics):
    if 'usage' in val_client_analytics['events']:
        return val_client_analytics

    
    
# Filter out only those events which contains platform from nested json
def get_events_with_platform(val_client_analytics):
    if 'PLATFORM' in val_client_analytics['events']:
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



# Creating summary schema for summary table dataframe
def get_summary_schema():
    return StructType([
        StructField('uuid', StringType(), True),
        StructField('brackets_uuid', StringType(), True),
        StructField('platform', StringType(), True),
        StructField('theme', StringType(), True),
        StructField('continent', StringType(), True),
        StructField('country', StringType(), True),
        StructField('date', DateType(), True)
        ])



# Generating parquet output files from data frames partitioned on date field which is our partition key 
# and putting under summary and usage folders
def generate_parquet(df_summary,df_usage,output):
    df_summary.write.partitionBy('date').mode('overwrite').parquet(f'{output}/summary')
    df_usage.write.partitionBy('date').mode('overwrite').parquet(f'{output}/usage')

    
    
# Creating RDDs and Dataframes to perform ETL over approximately 16,000 JSON files 
# thereby removing corrupt files and performing business logic before geenrating parquet files
def main(inputs, output):
    rdd = sc.wholeTextFiles(inputs).map(lambda x: json.loads(x[1])).coalesce(100)
    client_analytics = rdd.flatMap(get_client_analytics).filter(get_client_analytics_with_events).cache()
    platform_rdd = client_analytics.filter(get_events_with_platform)
    usage_rdd = client_analytics.filter(get_events_with_usage)
    usage=usage_rdd.flatMap(pre_process_usage)
    df_usage=spark.createDataFrame(usage,get_usage_schema())
    summary=platform_rdd.flatMap(pre_process_summary)
    df_summary=spark.createDataFrame(summary,get_summary_schema())
    generate_parquet(df_summary, df_usage, output)

    
if __name__ == '__main__':
    spark = SparkSession.builder \
    .appName('TUBA Spark')\
    .getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs,output)
