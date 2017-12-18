# Yelp Toronto User Pattern Analysis and Recommender System

In this project, we used [yelp challenge data](https://www.yelp.com/dataset). Our goal is to analyze user pattern and build a recommender system for users. We focus on the users who have rated businesses in Great Toronto Area. We processed data and built a web application tool to demonstrate the results by using big data technologies.

## Demo URL:
<a href="http://www.youtube.com/watch?feature=player_embedded&v=hoNKMHuzXDM
" target="_blank"><img src="http://img.youtube.com/vi/hoNKMHuzXDM/0.jpg" 
alt="IMAGE ALT TEXT HERE" width="240" height="180" border="10" /></a>

## Repository Description 

* _data/db_ <br/>
Contains mongoDB data used in this application
* _data_preprocess_code_ <br/>
Contains codes that accomplish following tasks: <br/> 
  * Collect data in the Great Toronto Area
  * Convert string id to int id
  * Process data for building user-business relationship
  * Process data for finding user compliments and votes.
  * Process data for analyzing user reviews by using TF*IDF
  * Process data for gathering user rated businesses, identifying new categories and re-assigning new categories
  * Train the recommendation system
* _yelpserver_ <br/>
Contains the visualization web application tool that shows all our results. <br/>
The web application does following tasks: <br/>
  * Query data from mongoDB ( _yelpserver/app.py, db.py_ )
  * Populate data to web frontend ( _yelpserver/app.py_  )  
  * Visualize data at web frontend ( _yelpserver/static/js/recommend.js,vs.js_ , _yelpserver/templates/index.html,user.html, recommend.html_)
  * Construct user id and new business id to feed to pre-trained recommender model ( _yelpserver/app.py_ )
  * Invoke spark to load pre-trained model and make prediction (_yelpserver/recommenderSystem.py_)


## Application Setup:
* Install Mongo DB server locally
* Git clone repository 
* Under the repository folder, start mongo db with data
```
mongod --dbpath data/db
```
* CD to yelpserver folder
* Start web server by using command:
```
spark-submit server.py
```
* In browser, type 0.0.0.0/5000


## Technologies we used:

* Data Processing: Spark, Spark SQL, Spark MLlib 
* Web backend: flask, spark, cherrypy
* Web frontend: D3.js, DC.js, crossfilter.js, Leaflet.js, keen.js, bootstrap v4
* Data Storage: MongoDB
* Other tools/technologies: Gephi for user-business relationship graph, yelp GraphQL for addition data query

# Yelp-User-Pattern-And-Recommender-System
