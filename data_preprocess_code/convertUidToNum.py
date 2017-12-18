from pyspark.sql import SparkSession
from pyspark.sql.types import *
import sys

spark = SparkSession.builder.appName('convertUidToNum').getOrCreate()
# last business_id is 156638, we start user id from 156640
def main():
    user_file = sys.argv[1]
    output = sys.argv[2]
    
    schema1= StructType([
        StructField('user_id', StringType(), False)       
    ])

    yelp_users = spark.read.json(user_file, schema1)
    yelp_users.createOrReplaceTempView('yelp_users')
    user_id = spark.sql(""" SELECT DISTINCT user_id FROM yelp_users""")
    user_id = user_id.rdd.map(lambda x: x['user_id']).zipWithIndex().toDF(['user_id','uid'])
    user_id.createOrReplaceTempView('user_id')
    user_id = spark.sql(""" SELECT user_id, uid + 156640 as uid FROM user_id""")

    user_id.write.save(output, format='csv', mode='overwrite')

if __name__ == "__main__":
    main()