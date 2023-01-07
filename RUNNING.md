# T.U.B.A

## Data Source
The initial dataset can be downloaded at [here](https://drive.google.com/file/d/1Gjldh5ThKTG89BwmLMbXgfcR2RWUHjwq).  
The file needs to be unzipped before use.

Here, we are assuming the data can be found at ./brackets

## How to run

### ETL Script
This Spark script takes in a directory of json files and outputs to a directory of the loaded data in parquet format. 

```
spark-submit back-end/data-transformation-scripts/ParquetOutput_from_JSONinput_ETL/etl_parquet_brackets.py brackets brackets-etl
```

### Feature Extraction Script
This Spark script takes in a directory of parquet files, with subdirectories for `usage` and `summary` created by the ETL script. It outputs the result of 3 dataframes of feature data aggregated from the transformed data to a directory. The output directory can be an S3 URI if given the credentials are passed in via the module `credentials.aws`. 

#### Writing to local
```
spark-submit back-end/data-transformation-scripts/observable-features-script/features_main.py brackets-etl brackets-features
```

#### Writing to S3
When writing to S3, you have to make sure to pass the relevant hadoop aws connector to Spark


e.g. From our cluster to the bucket `s3://brackets-analytics/features`
```
spark-submit --jars /opt/hadoop/share/hadoop/tools/lib/aws-java-sdk-bundle-1.12.262.jar,/opt/hadoop/share/hadoop/tools/lib/hadoop-aws-3.3.4.jar back-end/data-transformation-scripts/observable-features-script/features_main.py brackets-etl s3a://brackets-analytics/features
```

### ML Regression scripts
#### Preprocessing
As part of our analysis we wanted to see if we can predict future number of users from current users. The regression relies on the users table given by preprocess script

```
spark-submit back-end/data-transformation-scripts/ml-preproccess-scripts/ml_preprocess.py brackets-etl brackets-ml-data
```

This table was eventually folded as a main feature and can be found at `s3://brackets-analytics/features/users`

#### Regression
The regression script takes in an input and output directory. The output is saved in Hive Partitioning style with the output directory as the root. Additionally, it takes a 3rd argument, `mode = {n|p}`. 

In mode `n` normal, it takes in another 2 parameters, (`platform` and `country` to filter on)  for 5 total. It runs a polynomial regression on date, to find number of users, where `country={country|None}` and `platform={win|mac|null|None}`

With the right credentials, in mode `p` persistent, it relies on AWS SQS messages to request jobs. It polls the message queue once a minute and runs a regression based on the request parameters.

##### Example: run regression once with no filters writing locally
```
spark-submit back-end/data-transformation-scripts/ml-preproccess-scripts/ml_predict_users.py brackets-ml-data user-predictions n None None
```
The server will save the results in `output/platform=None/country=None`


##### Example: run persistently writing to S3
On the cluster, make sure to pass the right S3 connectors to Spark.

```
spark-submit --jars /opt/hadoop/share/hadoop/tools/lib/aws-java-sdk-bundle-1.12.262.jar,/opt/hadoop/share/hadoop/tools/lib/hadoop-aws-3.3.4.jar back-end/data-transformation-scripts/ml-preproccess-scripts/ml_predict_users.py brackets-ml-data s3a://brackets-analytics/user_predictions p
```

When a message comes into the Queue with body 
```
{'Country':'Canada', 'Platform':'Windows', 'JobID':'1234-abcd-5678'}
```
The server will run the regression with the filters `country='Canada' and platfrom='win'` and save the results to `s3://brackets-analytics/user_predictions/platform=win/country=Canada/`

### Front end server
The front end is written in javascript with Node.js using the Express framework. To start the front end webserver with Node.js and npm installed:

```
npm install
npm start
```

The webserver will be accessible on `localhost:3000`