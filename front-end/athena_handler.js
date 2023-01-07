const AthenaExpress = require("athena-express");
//Importing the AWS SDK
const AWS = require("aws-sdk");
const { platform } = require("os");

//UUID for job id
const { v4: uuidv4 } = require('uuid');
// Configuring the region and credentials to make connection to AWS Athena
 const awsCredentials = {
    region: "us-west-2",
 	accessKeyId: "",
 	secretAccessKey: ""
 };
 AWS.config.update(awsCredentials);
//configuring athena-express with aws sdk object
 const athenaExpressConfig = { aws: AWS,
	getStats: true }; 
 // Creating Athena Express service object
 const athenaExpress = new AthenaExpress(athenaExpressConfig);

async function getting_user_prediction(startDate, endDate, country, platform){
    let base_query = `SELECT * FROM user_predictions where date>= date('${startDate}') and date <= date('${endDate}')`;
    if(country){
        base_query=base_query+` and country='${country}' `;
    } else {
        base_query = base_query + ` and country = 'None'`;
    }
    if(platform=='null'){
        base_query=base_query+` and platform is null `;
    } else if(platform) {
        base_query=base_query+` and platform='${platform}'`;
    } else {
        base_query = base_query + ` and platform = 'None'`;
    }
    base_query = base_query + ` order by date`
    let myQuery = {
        sql : base_query,
        db : "brackets_analytics"
    };
    console.log(myQuery.sql)
    const labels_arr = []
    const data_arr = []
    const prediction_arr = []
    await athenaExpress
	.query(myQuery)
	.then(results => {
        for (var i = 0; i < results.Items.length; i++){
            var obj = results.Items[i];
            labels_arr.push(obj.date);
            data_arr.push(Number(obj.users))
            prediction_arr.push(obj.prediction)
          }
        console.log(labels_arr);
		console.log(data_arr);
	})
	.catch(error => {
		console.log(error);
	});   

    return {labels:labels_arr, data:data_arr, prediction:prediction_arr}
}

async function getting_top_countries(startDate, endDate, country, platform){
    let base_query = `select sum(users) as users, country from users where date>= date('${startDate}') and date<= date('${endDate}')`;
    if(country){
        base_query=base_query+` and country='${country}' `;
    }
    if(platform=='null'){
        base_query=base_query+` and platform is null `;
    } else if(platform) {
        base_query=base_query+` and platform='${platform}' `;
    }
    base_query = base_query + `group by country order by users DESC;`
    let myQuery = {
        sql : base_query,
        db : "brackets_analytics"
    };
    console.log(myQuery.sql)
    const labels_arr = []
    const data_arr = []
    await athenaExpress
	.query(myQuery)
	.then(results => {
        for (var i = 0; i < results.Items.length; i++){
            var obj = results.Items[i];
            labels_arr.push(obj.country);
            data_arr.push(Number(obj.users))
          }
        console.log(labels_arr);
		console.log(data_arr);
	})
	.catch(error => {
		console.log(error);
	});   

    return {labels:labels_arr, data:data_arr}
}

async function getting_active_users(startDate, endDate, country, platform){
    let base_query = `select sum(users) as users, date from users where date>=date('${startDate}') and date<=date('${endDate}')`;
    if(country){
        base_query=base_query+` and country='${country}' `;
    }
    if(platform=='null'){
        base_query=base_query+` and platform is null `;
    } else if(platform) {
        base_query=base_query+` and platform='${platform}' `;
    }
    base_query = base_query + ` group by date order by date;`
    let myQuery = {
        sql : base_query,
        db : "brackets_analytics"
    };
    console.log(myQuery.sql)
    const labels_arr = []
    const data_arr = []
    await athenaExpress
	.query(myQuery)
	.then(results => {
        for (var i = 0; i < results.Items.length; i++){
            var obj = results.Items[i];
            labels_arr.push(obj.date);
            data_arr.push(Number(obj.users))
          }
        console.log(labels_arr);
		console.log(data_arr);
	})
	.catch(error => {
		console.log(error);
	});   

    return {labels:labels_arr, data:data_arr}
}

async function getting_returning_users(startDate, endDate, country, platform){
    let base_query = `select sum(returned_count) as returned_users_count, date from returned_users where date>=date('${startDate}') and date<=date('${endDate}')`;
    if(country){
        base_query=base_query+` and country='${country}' `;
    }
    if(platform=='null'){
        base_query=base_query+` and platform is null `;
    } else if(platform) {
        base_query=base_query+` and platform='${platform}' `;
    }
    base_query = base_query + ` group by date order by date;`
    let myQuery = {
        sql : base_query,
        db : "brackets_analytics"
    };
    console.log(myQuery.sql)
    const labels_arr = []
    const data_arr = []
    await athenaExpress
	.query(myQuery)
	.then(results => {
        for (var i = 0; i < results.Items.length; i++){
            var obj = results.Items[i];
            labels_arr.push(obj.date);
            data_arr.push(Number(obj.returned_users_count))
          }
        console.log(labels_arr);
		console.log(data_arr);
	})
	.catch(error => {
		console.log(error);
	});   

    return {labels:labels_arr, data:data_arr}
}

async function getting_per_platform_users(startDate, endDate, country){
    let base_query = `select platform, sum(users) as users from users where date>=date('${startDate}') and date<=date('${endDate}') `;
    if(country){
        base_query=base_query+` and country='${country}' `;
    }
    base_query = base_query + ` group by platform order by users DESC`
    let myQuery = {
        sql : base_query,
        db : "brackets_analytics"
    };
    console.log(myQuery.sql)
    const labels_arr = []
    const data_arr = []
    await athenaExpress
	.query(myQuery)
	.then(results => {
        for (var i = 0; i < results.Items.length; i++){
            var obj = results.Items[i];
            labels_arr.push(obj.platform);
            data_arr.push(Number(obj.users))
          }
        console.log(labels_arr);
		console.log(data_arr);
	})
	.catch(error => {
		console.log(error);
	});   

    return {labels:labels_arr, data:data_arr}
}

async function getting_user_action(startDate, endDate, country){
    let base_query = `select usage_type, sum(usage_count) as usage_count from event_metrics where date>=date('${startDate}') and date<=date('${endDate}')  `;
    if(country){
        base_query=base_query+` and country='${country}' `;
    }
    base_query = base_query + ` group by usage_type order by usage_count DESC `
    let myQuery = {
        sql : base_query,
        db : "brackets_analytics"
    };
    console.log(myQuery.sql)
    const labels_arr = []
    const data_arr = []
    await athenaExpress
	.query(myQuery)
	.then(results => {
        for (var i = 0; i < results.Items.length; i++){
            var obj = results.Items[i];
            labels_arr.push(obj.usage_type);
            data_arr.push(Number(obj.usage_count))
          }
        console.log(labels_arr);
		console.log(data_arr);
	})
	.catch(error => {
		console.log(error);
	});   

    return {labels:labels_arr, data:data_arr}
}

async function getting_top_programming_languages(startDate, endDate, country){
    let base_query = `select language, sum(usage_count) as usage_count from event_metrics where usage_type='fileOpen' and date>=date('${startDate}') and date<=date('${endDate}')   `;
    if(country){
        base_query=base_query+` and country='${country}' `;
    }
    base_query = base_query + ` group by language order by usage_count DESC `
    let myQuery = {
        sql : base_query,
        db : "brackets_analytics"
    };
    console.log(myQuery.sql)
    const labels_arr = []
    const data_arr = []
    await athenaExpress
	.query(myQuery)
	.then(results => {
        for (var i = 0; i < results.Items.length; i++){
            var obj = results.Items[i];
            labels_arr.push(obj.language);
            data_arr.push(Number(obj.usage_count))
          }
        console.log(labels_arr);
		console.log(data_arr);
	})
	.catch(error => {
		console.log(error);
	});   

    return {labels:labels_arr, data:data_arr}
}

async function getting_live_preview(startDate, endDate, country){
    let base_query = `select country,sum(usage_count) as usage_count from event_metrics where usage_type='livePreview' and date>=date('${startDate}') and date<=date('${endDate}') `;
    if(country){
        base_query=base_query+` and country='${country}' `;
    }
    base_query = base_query + ` group by country order by usage_count DESC `
    let myQuery = {
        sql : base_query,
        db : "brackets_analytics"
    };
    console.log(myQuery.sql)
    const labels_arr = []
    const data_arr = []
    await athenaExpress
	.query(myQuery)
	.then(results => {
        for (var i = 0; i < results.Items.length; i++){
            var obj = results.Items[i];
            labels_arr.push(obj.country);
            data_arr.push(Number(obj.usage_count));
          }
        console.log(labels_arr);
		console.log(data_arr);
	})
	.catch(error => {
		console.log(error);
	});   

    return {labels:labels_arr, data:data_arr}
}

module.exports = {
    getting_user_prediction,
    getting_top_countries,
    getting_active_users,
    getting_returning_users,
    getting_per_platform_users,
    getting_user_action,
    getting_top_programming_languages,
    getting_live_preview
};
