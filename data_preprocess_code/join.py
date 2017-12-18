from pyspark.sql import SparkSession
from pyspark.sql.types import *
import sys
from pyspark.sql import Row
from pyspark.sql.functions import udf


spark = SparkSession.builder.appName('join').getOrCreate()

new_cats = [   
   'Restaurants',
   'Food',
   'Nightlife',
   'Shopping',
   'Beauty & Spas',
   'Event Planning & Services',
   'Arts & Entertainment',
   'Active Life',
   'Hotels & Travel',
   'Health & Medical',
   'Local Services',
   'Pets',
   'Automotive',
   'Home Services',
   'Professional Services',
   'Public Services & Government',
   'Education',
   'Financial Services',
   'Local Flavor'
   ]

def add_new_categories(categories):
    cate = categories[1:-1]
    cate_list = cate.split(',')
    for nc in new_cats:
        nnc = '"'+nc+'"'
        if nnc in cate_list:
            return nc
    return 'Others'

def main():
    user_file = sys.argv[1]
    business_file = sys.argv[2]
    review_file = sys.argv[3]
    bid_convert_file = sys.argv[4]
    uid_convert_file = sys.argv[5]
    output = sys.argv[6]

    schema1= StructType([
        StructField('user_id', StringType(), False),
        StructField('name', StringType(), False),
        StructField('review_count', IntegerType(), False),
        StructField('yelping_since', StringType(), False),
        StructField('friends', StringType(), False),
        StructField('useful', IntegerType(), False),
        StructField('funny', IntegerType(), False),
        StructField('cool', IntegerType(), False),
        StructField('fans', IntegerType(), False),
        StructField('elite', StringType(), False),
        StructField('average_stars', FloatType(), False),
        StructField('compliment_hot', IntegerType(), False),
        StructField('compliment_more', IntegerType(), False),
        StructField('compliment_profile', IntegerType(), False),
        StructField('compliment_cute', IntegerType(), False),
        StructField('compliment_list', IntegerType(), False),
        StructField('compliment_note', IntegerType(), False),
        StructField('compliment_plain', IntegerType(), False),
        StructField('compliment_cool', IntegerType(), False),
        StructField('compliment_funny', IntegerType(), False),
        StructField('compliment_writer', IntegerType(), False),
        StructField('compliment_photos', IntegerType(), False)
    ])

    schema2= StructType([
        StructField('business_id', StringType(), False),
        StructField('name', StringType(), False),
        StructField('neighborhood', StringType(), False),
        StructField('address', StringType(), False),
        StructField('city', StringType(), False),
        StructField('state', StringType(), False),
        StructField('postal_code', StringType(), False),
        StructField('latitude', FloatType(), False),
        StructField('longitude', FloatType(), False),
        StructField('stars', FloatType(), False),
        StructField('review_count', IntegerType(), False),
        StructField('is_open', IntegerType(), False),
        StructField('attributes', StringType(), False),
        StructField('categories', StringType(), False),
        StructField('hours', StringType(), False)
        
    ])

    schema3=StructType([
        StructField('review_id', StringType(), False),
        StructField('user_id', StringType(), False),
        StructField('business_id', StringType(), False),
        StructField('stars', IntegerType(), False),
        StructField('date', DateType(), False),
        StructField('text', StringType(), False),
        StructField('useful', IntegerType(), False),
        StructField('funny', IntegerType(), False),
        StructField('cool', IntegerType(), False)     
    ])

    schema4=StructType([
        StructField('string_id', StringType(), False),
        StructField('int_id', IntegerType(), False)    
    ])    


    yelp_user = spark.read.json(user_file, schema1)
    yelp_user.createOrReplaceTempView('yelp_user')
    yelp_business = spark.read.json(business_file, schema2)
    
    yelp_review = spark.read.json(review_file, schema3)
    yelp_review.createOrReplaceTempView('yelp_review')
    bid_convert = spark.read.csv(bid_convert_file, schema4)
    bid_convert.createOrReplaceTempView('bid_convert')
    uid_convert = spark.read.csv(uid_convert_file, schema4)
    uid_convert.createOrReplaceTempView('uid_convert')

    addNewCatetory = udf(add_new_categories, StringType())
    yelp_business = yelp_business.withColumn('category', addNewCatetory('categories')) 
    yelp_business.createOrReplaceTempView('yelp_business')
    new_data = spark.sql("""
    SELECT bc.int_id as business_id, b.business_id as business_sid, b.name as business_name,
    b.latitude as business_latitude, b.longitude as business_longitude, b.category as business_category,
    b.is_open as business_is_open,
    r.stars as user_rate_stars, r.date as review_date, uc.int_id as user_id, u.user_id as user_sid, u.name as user_name
    FROM yelp_business b JOIN yelp_review r
    ON b.business_id = r.business_id
    JOIN yelp_user u
    ON r.user_id = u.user_id
    JOIN bid_convert bc
    ON  bc.string_id = b.business_id
    JOIN uid_convert uc
    ON uc.string_id = u.user_id
    WHERE lower(b.city) like '%toronto%'  
    """)
    #WHERE lower(city) LIKE '%toronto%'
    #b.city like '%Toronto%' or b.city like '%toronto%' 
    print ('total count:'+str(new_data.count()))
    #new_data.show()
    #new_users.createOrReplaceTempView('new_users')

    new_data.write.save(output, format='json', mode='overwrite')

if __name__ == "__main__":
    main()