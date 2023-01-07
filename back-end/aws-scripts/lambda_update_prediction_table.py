# AWS Lambda trigger on new data loaded to update partition info
# Code inspired by https://stackoverflow.com/questions/47546670/how-to-make-msck-repair-table-execute-automatically-in-aws-athena

import boto3

def lambda_handler(event, context):
    bucket_name = 'brackets-analytics'

    client = boto3.client('athena')

    config = {
        'OutputLocation': 's3://' + bucket_name + '/',
        'EncryptionConfiguration': {'EncryptionOption': 'SSE_S3'}

    }

    # Query Execution Parameters
    sql = 'MSCK REPAIR TABLE user_predictions'
    context = {'Database': 'brackets_analytics'}

    client.start_query_execution(QueryString = sql, 
                                 QueryExecutionContext = context,
                                 ResultConfiguration = config)
