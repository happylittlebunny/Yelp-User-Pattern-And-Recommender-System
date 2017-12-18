var uid = document.getElementById('uid');
var url = '/loaduser/' + uid.innerHTML;
var url_radar = '/loaduser_radar/' + uid.innerHTML;
var url_wordcloud_good = '/loaduser_wordcloudgood/' + uid.innerHTML;
var url_wordcloud_bad = '/loaduser_wordcloudbad/' + uid.innerHTML;


d3.queue()
.defer(d3.json, url)
.defer(d3.json, url_radar)
.defer(d3.json, url_wordcloud_good)
.defer(d3.json, url_wordcloud_bad)
.await(visUser);

//Charts
var rateChart = dc.rowChart("#rate-chart");
var categoryChart = dc.rowChart("#category-chart");
var count = dc.dataCount(".dc-count");

function visUser(error, projectsJson, radarJson, wordcloudgood, wordcloudbad) {
    
    if (error) throw error;
    //Clean projectsJson data
    drawRadarChart(radarJson[0]);
    drawWordCloud(wordcloudgood, wordcloudbad);

    
	var yelpProjects = projectsJson;

    //console.log(yelpProjects[0]);
	//Create a Crossfilter instance
	var ndx = crossfilter(yelpProjects);
    var all = ndx.groupAll();

	//Define Dimensions
    var userRateStars = ndx.dimension(function(d) { return d["user_rate_stars"]; });
    var businessCategory = ndx.dimension(function(d) { return d["business_category"]; });
    var allDim = ndx.dimension(function(d) { return d;});


	//group
	var userRatesGroup = userRateStars.group(); 
	var categoriesGroup = businessCategory.group();



    rateChart
        .dimension(userRateStars)
        .group(userRatesGroup)
        .elasticX(true)
        .colors(d3.scale.category10())
        .ordering(function(d){ return -d.value });

    categoryChart
		.dimension(businessCategory)
        .group(categoriesGroup)
        .height(300)
        .elasticX(true)
        .colors(d3.scale.category20())
        .ordering(function(d){ return -d.value });

    count
        .dimension(ndx)
        .group(all);


    var map = L.map('map');

    var iconColorDic = {};
    iconColorDic["Restaurants"]="blue";
    iconColorDic["Food"]="red";
    iconColorDic["Nightlife"]="black";
    iconColorDic["Shopping"]="purple";
    iconColorDic["Beauty & Spas"]="beige";
    iconColorDic["Event Planning & Services"]="cadetblue";
    iconColorDic["Arts & Entertainment"]="green";
    iconColorDic["Active Life"]="orange";
    iconColorDic["Hotels & Travel"]="darkpurple";
    iconColorDic["Health & Medical"]="pink";
    iconColorDic["Local Services"]="darkred";
    iconColorDic["Pets"]="cadetblue";
    iconColorDic["Automotive"]="darkgreen";
    iconColorDic["Home Services"]="darkred";
    iconColorDic["Professional Services"]="lightblue";
    iconColorDic["Public Services & Government"]="lightgray";
    iconColorDic["Education"]="gray";
    iconColorDic["Financial Services"]="lightgreen";
    iconColorDic["Local Flavor"]="lightgreen";
    iconColorDic["Others"]="lightgreen";

    var iconDic = {};
    iconDic["Restaurants"]="cutlery"; //cutlery
    iconDic["Food"]="spoon";
    iconDic["Nightlife"]="glass";
    iconDic["Shopping"]="shopping-cart";
    iconDic["Beauty & Spas"]="heart";
    iconDic["Event Planning & Services"]="smile-o";
    iconDic["Arts & Entertainment"]= "music";
    iconDic["Active Life"]="bicycle";
    iconDic["Hotels & Travel"]="plane";
    iconDic["Health & Medical"]="medkit";
    iconDic["Local Services"]="flag";
    iconDic["Pets"]="paw";
    iconDic["Automotive"]="car";
    iconDic["Home Services"]="home";
    iconDic["Professional Services"]="user";
    iconDic["Public Services & Government"]="home";
    iconDic["Education"]="graduation-cap";
    iconDic["Financial Services"]="credit-card";
    iconDic["Local Flavor"]="bookmark";
    iconDic["Others"]="tags";

    var markerArray = [];
    var drawMap = function(){
        map.setView([43.655374, -79.381743], 13);

		mapLink = '<a href="http://openstreetmap.org">OpenStreetMap</a>';
		L.tileLayer(
			'http://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
				attribution: '&copy; ' + mapLink + ' Contributors',
				maxZoom: 18,
            }).addTo(map);     
            
        _.each(allDim.top(Infinity), function (d) {
            var lat = d["business_latitude"];
            var longi = d["business_longitude"];
            if (lat != null && longi != null)
            {
                //console.log(lat, longi);
                var categoryMarker = L.AwesomeMarkers.icon({
                    icon: iconDic[d["business_category"]],
                    prefix: 'fa',
                    markerColor: iconColorDic[d["business_category"]]
                  });                
                var content = '<strong>' + d["business_name"] + '</strong></br>User gave it '+d["user_rate_stars"]+' stars at ' +d["review_date"];
                var marker = L.marker([lat, longi], {icon: categoryMarker})
                .bindPopup(content);
                marker.on('mouseover', function (e) {
                    this.openPopup();
                });
                marker.on('mouseout', function (e) {
                    this.closePopup();
                });
                
                marker.addTo(map);
                markerArray.push(marker);
            }
            });
        var group = L.featureGroup(markerArray); //add markers array to featureGroup
        map.fitBounds(group.getBounds());   
    }

    drawMap();

    dcCharts = [rateChart, categoryChart];
    
        _.each(dcCharts, function (dcChart) {
            dcChart.on("filtered", function (chart, filter) {
                map.eachLayer(function (layer) {
                    map.removeLayer(layer)
                }); 
                drawMap();
            });
        });

        
    dc.renderAll();

};

var mycfg = {
    w: 300,
    h: 300,
    maxValue: 0.6,
    levels: 6,
    ExtraWidthX: 300
  }

  var mycfg1 = {
    w: 250,
    h: 250,
    maxValue: 0.6,
    levels: 6,
    ExtraWidthX: 300
  }

function drawRadarChart(rj){
    var temp = [], result = [], adj = [], adjResult = [];
    $.each(rj, function(key, value) {
        if (key == "name") {
            
        } else if (key == "user_id"){
            
        } else if (key == "cool") {
            adj.push({axis: key, value:value})
        } else if (key == "useful") {
            adj.push({axis: key, value:value})
        } else if (key == "funny") {
            adj.push({axis: key, value:value})
        } 
        else {
            temp.push({axis: key, value: value});
        }
    })
    result.push(temp);
    adjResult.push(adj);

    RadarChart.draw("#radar-chart1", result, mycfg);
    RadarChart.draw("#radar-chart2", adjResult, mycfg1);
};

function drawWordCloud(goodwords, badwords){
    d3.wordcloud("#wordcloud-good")
    .size([500, 300])
    .fill(d3.scale.ordinal().range(["#884400", "#448800", "#888800", "#444400"]))
    .words(goodwords)
    .onwordclick(function(d, i) {
      if (d.href) { window.location = d.href; }
    })
    .start();    

    d3.wordcloud("#wordcloud-bad")
    .size([500, 300])
    .fill(d3.scale.ordinal().range(["#884400", "#448800", "#888800", "#444400"]))
    .words(badwords)
    .onwordclick(function(d, i) {
      if (d.href) { window.location = d.href; }
    })
    .start();  
};