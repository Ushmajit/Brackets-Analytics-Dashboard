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

    //Dropdown
    $('#country-index-dropdown li').click(function(){
        $('#country-index-span').text($(this).text());
      });

      $('#platform-index-dropdown li').click(function(){
        $('#platform-index-span').text($(this).text());
      }); 
      
      var inputData = {
          "startDate": "2022-01-01",
          "endDate":"2023-01-01",
          "country":"",
          "platform": ""
      };


      // Default Chart Objects
    var ctx1 = $("#active-users").get(0).getContext("2d");
    var myChart1 = new Chart(ctx1, {
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

    var ctx2 = $("#returning-users").get(0).getContext("2d");
    var myChart2 = new Chart(ctx2, {
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

    var ctx3 = $("#per-platform-users").get(0).getContext("2d");
    var myChart3 = new Chart(ctx3, {
        type: "pie",
        data: {
            labels: [],
            datasets: [{
                label: 'Users',
                backgroundColor: [
                    "rgba(235, 22, 22, .7)",
                    "rgba(235, 22, 22, .5)",
                    "rgba(235, 22, 22, .2)"
                ],
                data: []
            }]
        },
        options: {
            responsive: true
        }
    });

    var ctx4 = $("#top-countries").get(0).getContext("2d");
    var myChart4 = new Chart(ctx4, {
        type: "bar",
        data: {
        labels: [],
        datasets: [{
            label: 'Users', 
            backgroundColor: [
                "rgba(235, 22, 22, .7)",
                "rgba(235, 22, 22, .6)",
                "rgba(235, 22, 22, .5)",
                "rgba(235, 22, 22, .4)",
                "rgba(235, 22, 22, .3)"
            ],
        data: []
        }]
        },
        options: {
            responsive: true
        }
    });     


      //Default Loading
        getActiveUsers(inputData);
        getReturningUsers(inputData);
        getTopCountries(inputData);
        getPlatformUsers(inputData);

      // On change Listeners
      $("#startdate-input").datepicker({
        onSelect: function(dateText) {
            var splitDate = dateText.split('/');
            var newFormat = splitDate[2] + '-' + splitDate[0] + '-' + splitDate[1];
            console.log("Selected date: " + dateText + "; input's current value: " + this.value);
            inputData["startDate"] = newFormat;
            getActiveUsers(inputData);
            getReturningUsers(inputData);
            getTopCountries(inputData);
            getPlatformUsers(inputData);
        }
    });

      $("#enddate-input").datepicker({
        onSelect: function(dateText) {
            var splitDate = dateText.split('/');
            var newFormat = splitDate[2] + '-' + splitDate[0] + '-' + splitDate[1];
            console.log("Selected date: " + newFormat + "; input's current value: " + this.value);
            inputData["endDate"] = newFormat;
            getActiveUsers(inputData);
            getReturningUsers(inputData);
            getTopCountries(inputData);
            getPlatformUsers(inputData);
        }
    });

    $('#country-index-dropdown li').click(function() {
        inputData["country"] = $(this).attr("val");
        getActiveUsers(inputData);
        getReturningUsers(inputData);
        getTopCountries(inputData);
        getPlatformUsers(inputData);
    });

    $('#platform-index-ul li').click(function() {
        inputData["platform"] = $(this).attr("val");
        getActiveUsers(inputData);
        getReturningUsers(inputData);
        getTopCountries(inputData);
        getPlatformUsers(inputData);
    });

    // Chart Global Color
    Chart.defaults.color = "#6C7293";
    Chart.defaults.borderColor = "#000000";


    function createChart(canvas_id, resp_labels, resp_data, chart_type, new_backgroundColor) {
        var ctx = $(canvas_id).get(0).getContext("2d");
            var myChart1 = new Chart(ctx, {
                type: chart_type,
                data: {
                labels: resp_labels,
                datasets: [{
                    label: "Users",
                    data: resp_data,
                    backgroundColor: new_backgroundColor,
                    fill: true
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
    
    function getActiveUsers(inputData) {
        $.post("/getActiveUsers",
            inputData,
            function (data, status) {
                var resp = {
                    labels: [],
                    data: []
                };
                console.log("Active Data: " + JSON.stringify(data));
                resp.labels = data.labels;
                resp.data = data.data;
                //var new_chart_type = myChart1.type;
                //var new_background_color = myChart1.data.datasets.backgroundColor;
                removeData(myChart1);
                myChart1 = createChart('#active-users', resp.labels, resp.data, "line", ["rgba(235, 22, 22, .7)"]);
        });
    }

    function getReturningUsers(inputData){
        $.post("/getReturningUsers",
        inputData,
        function (data, status) {
            var resp = {
                labels: [],
                data: []
            };
            resp.labels = data.labels;
            resp.data = data.data;
            removeData(myChart2);
            myChart2 = createChart('#returning-users', resp.labels, resp.data, "line", ["rgba(235, 22, 22, .7)"]);
        });
    }

    function getPlatformUsers(inputData) {
            // get Platform Users
        $.post("/perPlatformUsers",
        inputData,
        function (data, status) {
            var resp = {
                labels: [],
                data: []
            };
            resp.labels = data.labels;
            resp.data = data.data;
            removeData(myChart3);
            myChart3 = createChart('#per-platform-users', resp.labels, resp.data, "pie", [
                "rgb(124, 4, 4)",
                "rgb(173, 5, 5)",
                "rgb(223, 6, 6)"
            ]);
        });
    }

    function getTopCountries(inputData) {
            // get Top Countries
       $.post("/getTopCountries",
        inputData,
        function (data, status) {
            var resp = {
                labels: [],
                data: []
            };
            resp.labels = data.labels;
            resp.data = data.data;
            removeData(myChart4);
            myChart4 = createChart('#top-countries', resp.labels, resp.data, "bar", [
                "rgb(25, 1, 1)",
                "rgb(74, 2, 2)",
                "rgb(124, 4, 4)",
                "rgb(173, 5, 5)",
                "rgb(223, 6, 6)",
                "rgb(249, 32, 32)",
                "rgb(249, 32, 32)",
                "rgb(251, 131, 131)",
                "rgb(253, 181, 181)",
                "rgb(254, 230, 230)"
            ]);
       });
    }
    
})(jQuery);

