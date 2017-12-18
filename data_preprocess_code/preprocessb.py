from pyspark.sql import SparkSession
from pyspark.sql.types import *
import sys

spark = SparkSession.builder.appName('preprocessdata').getOrCreate()

def main():
    business_file = sys.argv[1]
    convert_id_file = sys.argv[2]
   # user_file = sys.argv[2]
   # review_file = sys.argv[3]

    output = sys.argv[3]

    schema1= StructType([
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

    schema2= StructType([
        StructField('business_id', StringType(), False),
        StructField('bid', LongType(), False),
    ])

    yelp_business = spark.read.json(business_file, schema1)
    yelp_business.createOrReplaceTempView('yelp_business')
    bid = spark.read.csv(convert_id_file, schema2)
    bid.createOrReplaceTempView('bid')
   
    new_business = spark.sql(""" 
    SELECT b.bid as id, y.business_id as business_id, y.name as name, y.neighborhood as neighborhood, 
    y.address as address, y.city as city, y.state as state, y.postal_code as postal_code,
    y.latitude as latitude, y.longitude as longitude, y.stars as stars, 
    y.review_count as review_count, y.is_open as is_open, y.attributes as attributes,
    y.categories as categories, y.hours as hours
    FROM bid b JOIN yelp_business y
    ON b.business_id = y.business_id
    WHERE lower(y.city) like '%toronto%'    
    """)
    print ('businesscount + '+ str(new_business.count()))
    #new_business.show()
    new_business.write.save(output, format='json', mode='overwrite')

if __name__ == "__main__":
    main()