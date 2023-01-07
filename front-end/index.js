const express = require("express");
const app = express();
const path = require("path");
const port = process.env.PORT || 3000;
const athenaHandler = require("./athena_handler.js");
const sqsHandler = require("./aws_sqs_handler.js");
const dynamicJobQueueUrl = "https://sqs.us-west-2.amazonaws.com/510556352750/DynamicJobRequest";
const dynamicJobStatusUrl = "https://sqs.us-west-2.amazonaws.com/510556352750/DynamicJobResponse";

// this is to tell express that static content is available
// on the directory 'public' to render
app.use(express.static(path.join(__dirname, "public/")));
app.use(express.urlencoded({ extended: true }));

// setting the view engine
app.set("view engine", "ejs");

// URL Route mappings
app.get("/", (req, res) => {
    res.render("index"); 
});

app.get("/map", (req, res) => {
    res.render("map"); 
});

app.get("/predictions", (req, res) => {
    res.render("predictions"); 
});

app.get("/event_metrics", (req, res) => {
    res.render("event_metrics"); 
});

//AJAX Request Handling to render the data back
// User Metrics
// Getting active users
app.post("/getActiveUsers", async (req, res) => {
    const result = await athenaHandler.getting_active_users(req.body.startDate, req.body.endDate, req.body.country, req.body.platform);
    res.json({
       labels: result.labels,
       data: result.data 
    });
});

// Getting returning users
app.post("/getReturningUsers", async (req, res) => {
    const result = await athenaHandler.getting_returning_users(req.body.startDate, req.body.endDate, req.body.country, req.body.platform);
    res.json({
       labels: result.labels,
       data: result.data  
    });
});

// Getting per platform users
app.post("/perPlatformUsers", async (req, res) => {
    const result = await athenaHandler.getting_per_platform_users(req.body.startDate, req.body.endDate, req.body.country, req.body.platform);
    res.json({
       labels: result.labels,
       data: result.data 
    });
});

// Getting top countries 
app.post("/getTopCountries", async (req, res) => {
    const result = await athenaHandler.getting_top_countries(req.body.startDate, req.body.endDate, req.body.country, req.body.platform);
    res.json({
       labels: result.labels.slice(0, 10),
       data: result.data.slice(0, 10)
    });
});

app.post("/getTotalUsers", async (req, res) => {
    console.log("Call made");
    const result = await athenaHandler.getting_top_countries(req.body.startDate, req.body.endDate, req.body.country, req.body.platform);
    res.json({
       labels: result.labels,
       data: result.data
    });
});




//User Action Metrics

// Most common user action performed
app.post("/getUserAction", async (req, res) => {
    const result = await athenaHandler.getting_user_action(req.body.startDate, req.body.endDate, req.body.country, req.body.platform);
    console.log("/getUserAction " + req.body.data);
    res.json({
       labels: result.labels,
       data: result.data 
    });
});

// Top Programming Languages being used
app.post("/topProgrammingLanguages", async (req, res) => {
    const result = await athenaHandler.getting_top_programming_languages(req.body.startDate, req.body.endDate, req.body.country, req.body.platform);
    console.log("/topProg " + req.body.data);
    res.json({
       labels: result.labels.slice(0, 5),
       data: result.data.slice(0, 5)
    });
});

// Getting count of users who performed live preview
app.post("/getLivePreview", async (req, res) => {
    const result = await athenaHandler.getting_live_preview(req.body.startDate, req.body.endDate, req.body.country, req.body.platform);
    res.json({
       labels: result.labels.slice(0,10),
       data: result.data.slice(0, 10)  
    });
});

// this method calls user's prediction
app.post("/getUsersPrediction", async (req, res) => {
    const result = await athenaHandler.getting_user_prediction(req.body.startDate, req.body.endDate, req.body.country, req.body.platform);
    res.json({
       labels: result.labels,
       data: result.data,
       prediction : result.prediction
    });
});

//AWS SQS Queue Polling and Triggering a Job
app.post("/triggerDynamicJob",async (req, res) => {
    var payload = {
        Country: req.body.country,
        Platform: req.body.platform,
        JobId: "" 
    };
    const result = await sqsHandler.requestDynamicJob(payload, dynamicJobQueueUrl);
    // console.log("/UsersPrediction " + req.body.data);
    res.json({
        jobId: result
    });
    console.log(result);
});

app.post("/pollJobStatus", async (req, res) => {
    const result = await sqsHandler.getDynamicJobStatus(dynamicJobStatusUrl);
    console.log(result);
});

app.get('*', function(req, res){
    res.send('404');
});

//Server Listen with Port number
app.listen(port, () => {
  console.log("server started on port 3000");
});