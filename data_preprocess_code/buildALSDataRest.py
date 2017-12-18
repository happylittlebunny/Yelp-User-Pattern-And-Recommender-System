from pyspark.sql import SparkSession
from pyspark.sql.types import *
import sys
from pyspark.sql import Row
from pyspark.sql.functions import udf

spark = SparkSession.builder.appName('join').getOrCreate()

def main():
    user_file = sys.argv[1] #uid_to_num.csv
    usersintot_file = sys.argv[2]
    business_file = sys.argv[3] #bid_to_num.csv
    output = sys.argv[4]

    schema1= StructType([
        StructField('user_id', StringType(), False),
        StructField('uid', LongType(), False)       
    ])

    schema2= StructType([
        StructField('business_id', StringType(), False),
        StructField('business_name', StringType(), False),
        StructField('business_neighborhood', StringType(), False),
        StructField('business_address', StringType(), False),
        StructField('business_city', StringType(), False),
        StructField('business_state', StringType(), False),
        StructField('business_postal_code', StringType(), False),
        StructField('business_latitude', FloatType(), False),
        StructField('business_longitude', FloatType(), False),
        StructField('business_categories', StringType(), False),
        StructField('user_rate_stars', IntegerType(), False),
        StructField('user_id', StringType(), False),
        StructField('business_category', StringType(), False)
    ])

    schema3= StructType([
        StructField('business_id', StringType(), False),
        StructField('bid', LongType(), False),
    ])

    user_ids = spark.read.csv(user_file, schema1)
    user_ids.createOrReplaceTempView('user_ids')
    usersintot = spark.read.json(usersintot_file, schema2)
    usersintot.createOrReplaceTempView('usersintot')
    business_ids = spark.read.csv(business_file, schema3)
    business_ids.createOrReplaceTempView('business_ids')

    new_data = spark.sql("""
    SELECT u.uid as user_id, b.bid as item_id, ut.user_rate_stars as rate
    FROM user_ids u JOIN usersintot ut
    ON u.user_id = ut.user_id
    JOIN business_ids b
    ON b.business_id = ut.business_id
    WHERE ut.business_category='Restaurants'
    """)
 
    print ('total count:'+str(new_data.count()))
    #new_data.show()
    #new_users.createOrReplaceTempView('new_users')

    new_data.write.save(output, format='json', mode='overwrite')

if __name__ == "__main__":
    main()