# T.U.B.A
## Analytics dashboard for Brackets(open source code editor platform)

#### Front End Link
http://ec2-35-89-25-69.us-west-2.compute.amazonaws.com:3000/

#### Running the project
The instructions on how to run the project and other scripts used are in [HERE](./RUNNING.md)

#### Credentials
Some parts of the project use AWS credentials. They are in aws.py under the credentials module, `from credentials import aws`. 
If you need the credentials file please email tjd10@sfu.ca

#### Other Considerations
##### Data Anonymization
The original data had the public IPs related to the client logs. 
We had to anonymize the public IPs into geolocation before doing any other analysis. 
The corresponding script is in `back-end/data-transformation/Ip-to-geolocation script` 

##### AWS Keyspace
When starting the project, we first decided to use a Cassandra database. We started to use AWS Keyspace because we wanted to further explore the AWS ecosystem.
Unfortunately, we had other needs that Keyspace cannot provide (no aggregation, etc.), so we switched to AWS Athena.
Under back-end/data-transformation-scripts/AWS_Keyspace_Cassandra is our code to run the ETL, that was abandoned.
