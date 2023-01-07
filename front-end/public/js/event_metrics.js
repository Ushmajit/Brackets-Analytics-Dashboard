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
    $('#languages-dropdown li').click(function(){
        $('#languages-span').text($(this).text());
      });
    
    $('#event-metrics-dropdown li').click(function(){
        $('#event-metrics-span').text($(this).text());
    });

    // Chart Global Color
    Chart.defaults.color = "#6C7293";
    Chart.defaults.borderColor = "#000000";


    //
    var inputData1 = {
        "startDate": "2022-01-01",
        "endDate":"2023-01-01",
        "country":"",
        "platform": ""
    };

    //Create Chart Objects
    var ctx5 = $("#user-action-metrics").get(0).getContext("2d");
    var myChart5 = new Chart(ctx5, {
        type: "bar",
        data: {
        labels: [],
        datasets: [{
            label: "Users",
            data: [],
            backgroundColor: "rgba(235, 22, 22, .7)",
            fill: true
            }]
        },
        options: {
            responsive: true
        }
    });

    var ctx6 = $("#top-programming-languages").get(0).getContext("2d");
    var myChart6 = new Chart(ctx6, {
        type: "pie",
        data: {
            labels: [],
            datasets: [{
                label: 'Users',
                backgroundColor: [
                    "rgba(235, 22, 22, .7)",
                    "rgba(235, 22, 22, .4)"
                ],
            data: []
        }]
    },
    options: {
        responsive: true
    }
});

var ctx7 = $("#countries-live-preview").get(0).getContext("2d");
var myChart7 = new Chart(ctx7, {
    type: "bar",
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

    //Default Charts
    getUserAction(inputData1);
    topProgrammingLanguages(inputData1);
    getLivePreview(inputData1);

    //onChange Listeners
    $("#metrics-start-date").datepicker({
        onSelect: function(dateText) {
            var splitDate = dateText.split('/');
            var newFormat = splitDate[2] + '-' + splitDate[0] + '-' + splitDate[1];
            console.log("Selected date: " + dateText + "; input's current value: " + this.value);
            inputData1["startDate"] = newFormat;
            getUserAction(inputData1);
            topProgrammingLanguages(inputData1);
            getLivePreview(inputData1);
        }
    });

      $("#metrics-end-date").datepicker({
        onSelect: function(dateText) {
            var splitDate = dateText.split('/');
            var newFormat = splitDate[2] + '-' + splitDate[0] + '-' + splitDate[1];
            console.log("Selected date: " + newFormat + "; input's current value: " + this.value);
            inputData1["endDate"] = newFormat;
            getUserAction(inputData1);
            topProgrammingLanguages(inputData1);
            getLivePreview(inputData1);
        }
    });

    $('#event-metrics-ul li').click(function() {
        inputData1["country"] = $(this).attr("val");
        getUserAction(inputData1);
    });

    $('#languages-ul li').click(function() {
        inputData1["country"] = $(this).attr("val");
        topProgrammingLanguages(inputData1);
    });

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
    
    function getUserAction(inputData1) {
        $.ajax({
            url: "/getUserAction",
            type:'POST',
            data: inputData1,
            dataType: 'json',
            success: function(response) {
                console.log("Response Labels" + response.labels);
                removeData(myChart5);
                myChart5 = createChart("#user-action-metrics", response.labels, response.data, "bar", [
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
            },
            error: function(xhr) {
            //Do Something to handle error
            }
        });
    }

    // Top Programming Languages
    function topProgrammingLanguages(inputData1) {
        $.post("/topProgrammingLanguages",
        inputData1,
        function (data, status) {
            var resp = {
                labels: [],
                data: []
            };
            resp.labels = data.labels;
            resp.data = data.data;
            removeData(myChart6);
            myChart6 = createChart("#top-programming-languages", resp.labels, resp.data, "pie",  [
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
    
    // get live preview
    function getLivePreview(inputData1) {
        $.post("/getLivePreview",
        inputData1,
        function (data, status) {
            var resp = {
                labels: [],
                data: []
            };
            resp.labels = data.labels;
            resp.data = data.data;
            removeData(myChart7);
            myChart7 = createChart('#countries-live-preview', resp.labels, resp.data, "bar", 
            ["rgb(25, 1, 1)",
            "rgb(74, 2, 2)",
            "rgb(124, 4, 4)",
            "rgb(173, 5, 5)",
            "rgb(223, 6, 6)",
            "rgb(249, 32, 32)",
            "rgb(249, 32, 32)",
            "rgb(251, 131, 131)",
            "rgb(253, 181, 181)",
            "rgb(254, 230, 230)"])
        });
    }

})(jQuery);

