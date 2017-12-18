var tableChart = dc.dataTable(".dc-data-table");

function recommend()
{
    var type = document.getElementById("type").value;
    var topN = document.getElementById("num").value;
    var uid = document.getElementById('uid').innerHTML;

    var url = '/recommend/'+ uid + '/' + type + '/' + topN;
    //window.location.href = url;
    d3.queue()
    .defer(d3.json, url) 
    .await(visReBus);
    $("#loading").show();       
}

function visReBus(error, businessInfo)
{
    $("#loading").hide()
    if (error) throw error;
    drawMapMarkers(businessInfo);

    var ndx = crossfilter(businessInfo);
    var all = ndx.groupAll();  
    
    var bname = ndx.dimension(function(d) { return d["name"]; });
    var bnameGroup = bname.group(); 

    tableChart
        .dimension(bname)
        .group(function(d) { return ""})
        .columns([
            {
                label: "Rank",
                format: function (d) { return d["rank"]; }
            },            
            {
                label: "Name",
                format: function (d) { return d["name"]; }
            },
            {
                label: "Address",
                format: function (d) { return d["address"]; }
            },
            {
                label: "Average Rating",
                format: function (d) { return d["stars"]; }
            }
        ])
        .sortBy(function(d){ return +d["rank"] });

        tableChart.render();
}

var map = L.map('map');
map.setView([43.655374, -79.381743], 13);

mapLink = '<a href="http://openstreetmap.org">OpenStreetMap</a>';
L.tileLayer(
    'http://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        attribution: '&copy; ' + mapLink + ' Contributors',
        maxZoom: 18,
    }).addTo(map);     

function drawMapMarkers (businessInfo)
{
    map.eachLayer(function (layer) {
        map.removeLayer(layer);
    });

    var markerArray = [];
    L.tileLayer(
        'http://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            attribution: '&copy; ' + mapLink + ' Contributors',
            maxZoom: 18,
        }).addTo(map);   

    for(var i = 0; i < businessInfo.length; i++) {
        var d = businessInfo[i];
        var lat = d["latitude"];
        var longi = d["longitude"];
        if (lat != null && longi != null)
        {
            var content = '<strong>Rank ' + d['rank'] + ':</strong></br><a href="' + d["link"] +'" target="_blank">' + d["name"] +'</a>'
            var marker = L.marker([lat, longi])
            .bindPopup(content);
            marker.addTo(map);
            markerArray.push(marker);
        }
    }
    var group = L.featureGroup(markerArray); //add markers array to featureGroup
    map.fitBounds(group.getBounds());  
};