from pyspark.sql import SparkSession
from pyspark.sql.types import *
import sys
from pyspark.sql import Row
from pyspark.sql.functions import udf
from operator import add

spark = SparkSession.builder.appName('compute').getOrCreate()
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

def count_categories(row):
    cate = row['business_categories']
    cate_new = cate[1:-1]
    cate_list = cate_new.split(',')
    for c in cate_list:
        yield(c, 1)

def add_new_categories(categories):
    cate = categories[1:-1]
    cate_list = cate.split(',')
    for nc in new_cats:
        nnc = '"'+nc+'"'
        if nnc in cate_list:
            return nc
    return 'Others'


def main():
    myfile = sys.argv[1]
    output = sys.argv[2]

    schema1= StructType([
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
        StructField('user_id', StringType(), False)
    ])

    yelp_ub = spark.read.json(myfile, schema1)
    yelp_ub.createOrReplaceTempView('yelp_ub')
   
    addNewCatetory = udf(add_new_categories, StringType())
    #cat = spark.sql(""" select business_categories from yelp_ub """)
    #print (cat.toJSON().first())
    yelp_ub.withColumn('business_category', addNewCatetory('business_categories')) \
           .drop('business_categories') \
           .write.save(output, format='json', mode='overwrite')
    #yelp_ub.createOrReplaceTempView('yelp_ub')
    
    #data = spark.sql(""" select business_category, count(*) as count from yelp_ub group by business_category order by count desc""")
    #data.show()
    #data.rdd.coalesce(1).saveAsTextFile(output)
    #data2=spark.sql(""" select * from yelp_ub where business_categories = '[]' """)
    #data2.show(20)

    #cat.rdd.flatMap(count_categories).reduceByKey(add) \
    #   .sortBy(lambda x: x[1], ascending=False) \
   #    .coalesce(1).saveAsTextFile(output)
    #print(.collect())
    
    #print ('total count:'+str(new_data.count()))
    #new_data.show()
    #new_users.createOrReplaceTempView('new_users')

   # new_data.rdd.coalesce(1).saveAsTextFile(output)

if __name__ == "__main__":
    main()