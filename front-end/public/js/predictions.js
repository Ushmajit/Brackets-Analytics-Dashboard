(function ($) {
    "use strict";

    // Spinner
    var spinner = function () {
        setTimeout(function () {
            if ($('#spinner').length > 0) {
                $('#spinner').removeClass('show');
            }
        }, 1);
    };
    spinner();
    
    
    // Back to top button
    $(window).scroll(function () {
        if ($(this).scrollTop() > 300) {
            $('.back-to-top').fadeIn('slow');
        } else {
            $('.back-to-top').fadeOut('slow');
        }
    });
    $('.back-to-top').click(function () {
        $('html, body').animate({scrollTop: 0}, 1500, 'easeInOutExpo');
        return false;
    });


    // Sidebar Toggler
    $('.sidebar-toggler').click(function () {
        $('.sidebar, .content').toggleClass("open");
        return false;
    });


    // Progress Bar
    $('.pg-bar').waypoint(function () {
        $('.progress .progress-bar').each(function () {
            $(this).css("width", $(this).attr("aria-valuenow") + '%');
        });
    }, {offset: '80%'});


    // Calender
    $('#calender').datetimepicker({
        inline: true,
        format: 'L'
    });

    // Chart Global Color
    Chart.defaults.color = "#6C7293";
    Chart.defaults.borderColor = "#000000";

    //Dropdown
    $('#country-predictions-dropdown li').click(function(){
        $('#country-predictions-span').text($(this).text());
      });

      $('#platform-predictions-dropdown li').click(function(){
        $('#platform-predictions-span').text($(this).text());
      }); 

      var inputData2 = {
        "startDate": "2022-01-01",
        "endDate":"2023-01-01",
        "country":"",
        "platform": ""
    };

    //onchange listeneners
    $("#predictions-start-date").datepicker({
        onSelect: function(dateText) {
            var splitDate = dateText.split('/');
            var newFormat = splitDate[2] + '-' + splitDate[0] + '-' + splitDate[1];
            console.log("Selected date: " + dateText + "; input's current value: " + this.value);
            inputData2["startDate"] = newFormat;
            getUserPredictions(inputData2);
        }
    });

      $("#predictions-end-date").datepicker({
        onSelect: function(dateText) {
            var splitDate = dateText.split('/');
            var newFormat = splitDate[2] + '-' + splitDate[0] + '-' + splitDate[1];
            console.log("Selected date: " + newFormat + "; input's current value: " + this.value);
            inputData2["endDate"] = newFormat;
            getUserPredictions(inputData2);
        }
    });

    $('#country-predictions-dropdown li').click(function() {
        inputData2["country"] = $(this).attr("val");
        getUserPredictions(inputData2);
    });

    $('#platform-predictions-ul li').click(function() {
        inputData2["platform"] = $(this).attr("val");
        getUserPredictions(inputData2);
    });

    //Create chart objects
    var context = $("#predicted-users-canvas").get(0).getContext("2d");
    var myChart1 = new Chart(context, {
        type: "line",
        data: {
        labels: [],
        datasets: [{
            label: "Users",
            data: [],
            backgroundColor: "rgba(235, 22, 22, .7)",
            fill: true
        }
        ]
    },
    options: {
        responsive: true
        }
    });

    //default
    getUserPredictions(inputData2);

    function createChart(canvas_id, resp_labels, resp_data, resp_predictions, chart_type, new_backgroundColor) {
        var ctx = $(canvas_id).get(0).getContext("2d");
            var myChart1 = new Chart(ctx, {
                type: chart_type,
                data: {
                labels: resp_labels,
                datasets: [{
                    label: "Actual User",
                    data: resp_data,
                    backgroundColor: new_backgroundColor[0],
                    fill: false},
                    {
                    label: "Predicted User",
                    data: resp_predictions,
                    backgroundColor: new_backgroundColor[1],
                    fill: false
                    }
                ]
            },
            options: {
                responsive: true
                }
            });
        
        return myChart1;
    }
    
    function removeData(chart) {
        chart.destroy();
    }
    
    console.log("Out");
    var refInterval = window.setInterval('getJobStatus()', 30000); // 30 seconds

    function getJobStatus() {
            // polling to AWS SQS Queue for Job Status
       if ($('#jobStatus-tbody').children().length && $('#jobStatus-tbody').children().length > 0) {
          console.log("Inside condition");
          $.post("/pollJobStatus",
          inputData2,
            function (data, status) {
          });
       }
    }

    var jobId = 0;

    function triggerDynamicJob() {
        var jobParameters = {
            "country":"",
            "platform": "",
        };

        jobParameters.country = inputData2.country;
        jobParameters.platform = jobParameters.platform

        $.post("/triggerDynamicJob",
        inputData2,
        function (data, status) {
            jobId = data.jobId;
            console.log(jobId);
        });
        return jobId;
    }


    function getUserPredictions(inputData2) {
        $.post("/getUsersPrediction",
        inputData2,
        function (data, status) {
            var resp = {
                labels: [],
                data: [],
                prediction: [],
            };
            console.log("Data received: " + JSON.stringify(data));
            resp.labels = data.labels;
            resp.data = data.data;
            resp.prediction = data.prediction;

            if (!resp.labels && !resp.data || (resp.labels.length == 0 && resp.data.length == 0)) {
                jobId = triggerDynamicJob();
                
                if ($('#jobStatusMessage').hasClass('invisible')) {
                    $('#jobStatusMessage').removeClass('invisible').addClass('visible');
                }
                if (jobId != 0) {
                    $('#jobStatus-tbody').append(`<tr>
                    <td>
                    ${jobId}
                    </td>
                    <td id=${jobId}>
                    <button type="button" class="btn btn-warning disabled">Pending</button>
                    </td>
                    <td class="disabled">
                      <button class="btn btn-danger remove disabled"
                        type="button">Plot</button>
                   </td>
                   </tr>`);
                }
            } else {
                if ($('#jobStatusMessage').hasClass('visible')) {
                    $('#jobStatusMessage').removeClass('visible').addClass('invisible');
                }
                removeData(myChart1);
                myChart1 = createChart('#predicted-users-canvas', resp.labels, resp.data, 
                resp.prediction, "line", ["rgba(74,2,2)", "rgba(50, 158, 168)"]);
            }
        });
    }
    
})(jQuery);
