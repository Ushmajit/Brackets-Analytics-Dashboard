import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

def main(inputs, output):
    df_usage = spark.read.parquet(f'{inputs}/usage').drop('uuid','continent', 'usage_type', 'language')
    df_summary = spark.read.parquet(f'{inputs}/summary').drop('uuid','continent', 'theme')

    df_join = df_usage.join(df_summary, ['brackets_uuid','date', 'country'], how='left')
    df_agg = df_join.groupBy('date', 'platform', 'country').agg(f.countDistinct('brackets_uuid').alias('users'))
    df_agg.write.parquet(output)


if __name__ == '__main__':
    spark = SparkSession.builder.appName('TUBA Spark').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    inputs = sys.argv[1]
    output = sys.argv[2]

    main(inputs, output)