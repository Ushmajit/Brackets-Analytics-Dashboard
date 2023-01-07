// Import the AWS SDK
const AWS = require('aws-sdk');
//UUID for job id
const { v4: uuidv4 } = require('uuid');

// not a good idea to hardcode AWS creds
// move to .env file if we have time and before open sourcing it
// Configure the region
AWS.config.update({
    region: 'us-west-2',
    apiVersion: 'latest',
    credentials: {
      accessKeyId: '',
      secretAccessKey: ''
    }
  });

// Create an SQS service object
const sqs = new AWS.SQS();

async function requestDynamicJob(payload, queueUrl) {
    payload.JobId = uuidv4();
    var params = {
        // Remove DelaySeconds parameter and value for FIFO queues
       DelaySeconds: 10,
       MessageBody: JSON.stringify(payload),
       QueueUrl: queueUrl
     };
     
    await sqs.sendMessage(params, function(err, data) {
        if (err) {
          console.log("Error", err);
          payload.JobId = -1;
        } else {
          console.log("Success", data.MessageId);
        }
      });
      return payload.JobId;
}

async function getDynamicJobStatus(queueUrl) {
  console.log("polling the data");
    var params = {
        MaxNumberOfMessages: 10,
        QueueUrl: queueUrl,
        VisibilityTimeout: 20,
        // this is the configuration for long polling
        WaitTimeSeconds: 20
       };
    await sqs.receiveMessage(params, function(err, data) {
        if (err) {
          console.log("Receive Error", err);
          //return the data here after parsing.
        } else if (data.Messages) {
            console.log("Messages received : " + JSON.stringify(data.Messages));

        }
    });
}

module.exports = {
  requestDynamicJob,
  getDynamicJobStatus
}