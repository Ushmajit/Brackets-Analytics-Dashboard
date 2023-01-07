# main file for feature analysis
from credentials import aws

import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types


# add more functions as necessary

def main(inputs, output):
    # main logic starts here
    df_usage = spark.read.parquet(f'{inputs}/usage').drop('uuid', 'continent')
    df_summary = spark.read.parquet(f'{inputs}/summary').drop('uuid', 'continent')

    # Input: usage and summary tables
    # Output: usage information
    df_join = df_usage.join(df_summary, ['brackets_uuid', 'date', 'country'], how='left')
    df_agg = df_join.groupBy('date', 'platform', 'country').agg(functions.countDistinct('brackets_uuid').alias('users'))
    df_agg.write.partitionBy("date").parquet(f'{output}/users')

    # Input: usage table
    # Output: No of returning users within a date range
    df1 = df_usage.alias('df1')
    df2 = df_usage.alias('df2')

    returned_users = df1.join(df2, ["brackets_uuid"]).where('df1.date > df2.date')
    df_join2 = returned_users.join(df_summary, ['brackets_uuid', 'date', 'country'], how='left')
    returned_users_count = df_join2.groupBy("df1.date", "df1.country", "platform") \
        .agg(functions.countDistinct('brackets_uuid').alias('returned_count'))
    returned_users_count.write.partitionBy("date").parquet(f'{output}/return')

    # Input: usage table
    # Output: live preview table
    live_preview = df_usage.groupBy("date", "country", "usage_type", "language") \
        .agg(functions.count('usage_type').alias('usage_count')) \
        .sort("date", ascending=False)

    live_preview.write.partitionBy("date").parquet(f'{output}/live_preview')

    # Input: summary table
    # Output: Theme table
    theme_table = df_summary.groupBy("date", "platform", "country", "theme") \
        .agg(functions.count('theme').alias('theme_count')) \
        .sort("date", "theme_count", ascending=False)

    theme_table.write.partitionBy("date").parquet(f"{output}/theme")


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('T.U.B.A Spark Features') \
        .config("fs.s3a.access.key", aws.access_key) \
        .config("fs.s3a.secret.key", aws.secret_key) \
        .config("fs.s3a.endpoint", 'http://s3-us-west-2.amazonaws.com') \
        .config('spark.hadoop.fs.s3a.fast.upload.active.blocks', 1) \
        .config('spark.hadoop.fs.s3a.fast.upload.buffer', 'bytebuffer') \
        .getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)
