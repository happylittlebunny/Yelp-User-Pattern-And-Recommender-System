from pyspark.sql import SparkSession
from pyspark.sql.types import *
import sys

spark = SparkSession.builder.appName('convertBidToNum').getOrCreate()

def main():
    business_file = sys.argv[1]
    output = sys.argv[2]

    schema1= StructType([
        StructField('business_id', StringType(), False)       
    ])

    yelp_business = spark.read.json(business_file, schema1)
    yelp_business.createOrReplaceTempView('yelp_business')
    businiess_id = spark.sql(""" SELECT DISTINCT business_id FROM yelp_business""")
    businiess_id = businiess_id.rdd.map(lambda x: x['business_id']).zipWithIndex().toDF(['business_id','bid'])
    businiess_id.write.save(output, format='csv', mode='overwrite')

if __name__ == "__main__":
    main()