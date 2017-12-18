from flask import Flask,render_template
from flask import request
from flask import Blueprint
from flask.json import jsonify

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import lit

import sys
from pyspark.sql import Row
from pyspark.sql.functions import udf
from operator import add
import logging
import json
from bson import json_util
from bson.json_util import dumps
import requests

from recommenderSystem import RecommenderSystem
from db import DataBase

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

main=Blueprint('main', __name__)

MONGODB_HOST = 'localhost'
MONGODB_PORT = 27017
DB_NAME = 'yelp'

app=Flask(__name__)
@main.route('/')
def index():
    return render_template('index.html')

@main.route('/loaduser/<string:uid>', methods=['GET'])
def get_data(uid):
    json_users = []
    user = mdb.getCollection(DB_NAME, 'torontodata') \
        .find({'user_sid':uid}, projection={'_id': False})
    for u in user:
        json_users.append(u)
    json_users = json.dumps(json_users, default=json_util.default)
    return json_users


@main.route('/loaduser_wordcloudgood/<string:uid>', methods=['GET'])
def get_data_for_wordcloudgood(uid):
    goodwords = mdb.getCollection(DB_NAME, 'goodreview') \
        .find({'user_id':uid}, {'text': 1, 'size': 1, '_id':False})
    gws = [ gw for gw in goodwords]
    return json.dumps(gws, default=json_util.default)

@main.route('/loaduser_wordcloudbad/<string:uid>', methods=['GET'])
def get_data_for_wordcloudbad(uid):
    badwords = mdb.getCollection(DB_NAME, 'badreview') \
        .find({'user_id':uid}, {'text': 1, 'size': 1, '_id':False})
    bws = [ bw for bw in badwords]
    return json.dumps(bws, default=json_util.default)

@main.route('/loaduser_radar/<string:uid>', methods=['GET'])
def get_data_for_radarChart(uid):
    user = mdb.getCollection(DB_NAME, 'usercomps') \
        .find({'user_id':uid}, projection={'_id': False})
    json_users = [u for u in user]
    json_users = json.dumps(json_users, default=json_util.default)

    return json_users

@main.route('/user/<string:uid>', methods=['GET'])
def show_user(uid):
    user = mdb.getCollection(DB_NAME, 'toronto_distinct_users') \
        .find_one({'user_sid':uid}, projection={'_id': False})  
    return render_template('user.html', user_id=uid, 
                           user_name=user["user_name"], user_ys=user["user_ys"],
                           user_elite=user["user_elite"], user_rc=user["user_review_count"],
                           user_as=user["user_as"])

# Query Yelp GraphQL to get business link
def find_business_link(business_info):
    query='{'
    i = 1
    for bi in business_info:
        sub = 'b' + str(i) + ':business(id:"'+bi['business_id']+'") {url} '
        query = query + sub
        i = i+1
    query = query + '}'
    headers = {"Authorization": "Bearer 8WLNfXuHYa79Ozp9pSkjPuH5sf2ZLWWkE5E2ZCdAedA8iQzr26QEbJHfMcGNL0yevLYOd3IJywdgSqw6LThHe_JRLEb_owql9Bj8wQtLRhkNluAz0L-nRP-uobAhWnYx",
            "Content-Type": "application/json"}

    r = requests.post("https://api.yelp.com/v3/graphql", headers=headers, data=json.dumps({"query":query}))
   # print (r.json)
    return r.json()['data']

@main.route('/recommend/<string:uid>/<string:t>/<int:n>', methods=['GET'])
def recommend(uid, t, n):
    testdata = []
    business_not_rated = mdb.getCollection(DB_NAME, 'torontodata') \
                .distinct('business_id', {'business_is_open':1, 'user_sid':{ '$ne': uid }}, projection={'_id': False})
    uid = mdb.getCollection(DB_NAME, 'torontodata') \
             .distinct('user_id', {'user_sid':uid})[0]
    for bnr in business_not_rated:
        testdata.append((uid, bnr))
    # convert json list to spark rdd
    #print (testdata)
    test = spark.createDataFrame(testdata, ['user_id', 'item_id'])
    #test.show()
    bids = rs.get_top_rating_b(test, t, n) \
             .rdd.map(lambda x:x['item_id']) \
             .collect()
    #print (bids)
    json_b = []
    
    business_info = mdb.getCollection(DB_NAME, 'toronto_business_id')\
                       .find({ 'id': { '$in': bids } })
    temp=[ bi for bi in business_info]
    # mongo can't gareentee the order, 
    # here sort the order by the original business ids from recommender system
    order_temp = []
    for id in bids:
        for bi in temp:
            if id == bi["id"]:
                order_temp.append(bi)
                break
    #print (order_temp)
    links = find_business_link(order_temp)
    i = 1
    for bi in order_temp:
        x = {}
        x["rank"]=i
        x["name"]=bi["name"]
        x["address"]=bi["address"]
        x["stars"]=bi["stars"]
        x["latitude"] = bi["latitude"]
        x["longitude"] = bi["longitude"]
        x["link"]=links["b"+str(i)]['url']
        i = i+1
        json_b.append(x)
        
    json_b = json.dumps(json_b, default=json_util.default)
    return json_b


@main.route('/recommendersystem/<string:uid>', methods=['GET'])
def recommend_top_n(uid):
    return render_template('recommend.html', user_id=uid)

def create_app(spark_context):    
    global sc 
    global rs
    global spark
    global mdb

    sc = spark_context
    spark = SparkSession(sc)
    app = Flask(__name__)
    app.register_blueprint(main)

    rs = RecommenderSystem(sc, spark)
    mdb = DataBase()
    mdb.connect(MONGODB_HOST, MONGODB_PORT)

    return app  

if __name__ == '__main__':
    app.run(debug=True)