from pyspark.sql import SparkSession
from pyspark.sql.types import *
import sys
from pyspark.sql import Row
from pyspark.sql.functions import udf

spark = SparkSession.builder.appName('join').getOrCreate()

def main():
    user_file = sys.argv[1] #uid_to_num.csv
    toronto_business_file = sys.argv[2]
    review_file = sys.argv[3]
    output = sys.argv[4]

    schema1= StructType([
        StructField('user_id', StringType(), False),
        StructField('id', LongType(), False)       
    ])

    schema2= StructType([
        StructField('id', LongType(), False),
        StructField('business_id', StringType(), False)
    ])

    schema3=StructType([
        StructField('review_id', StringType(), False),
        StructField('user_id', StringType(), False),
        StructField('business_id', StringType(), False),
        StructField('stars', IntegerType(), False) 
    ])

    yelp_user = spark.read.csv(user_file, schema1)
    yelp_user.createOrReplaceTempView('yelp_user')
    toronto_business_file = spark.read.json(toronto_business_file, schema2)
    toronto_business_file.createOrReplaceTempView('toronto_business_file')
    yelp_review = spark.read.json(review_file, schema3)
    yelp_review.createOrReplaceTempView('yelp_review')

    new_data = spark.sql("""
    SELECT u.id as user_id, tb.id as item_id, r.stars as rate  
    FROM toronto_business_file tb JOIN yelp_review r
    ON tb.business_id = r.business_id
    JOIN yelp_user u
    ON r.user_id = u.user_id
    """)
 
    print ('total count:'+str(new_data.count()))
    #new_data.show()
    #new_users.createOrReplaceTempView('new_users')

    new_data.write.save(output, format='json', mode='overwrite')

if __name__ == "__main__":
    main()