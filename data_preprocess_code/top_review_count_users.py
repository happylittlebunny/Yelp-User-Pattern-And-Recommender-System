import sys
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import explode, lit, desc, count
from pyspark.sql.types import *

# inputs = 'business.json'
# output = sys.argv[1]
  
spark = SparkSession.builder.appName('clean data').getOrCreate()
# sc = spark.sparkContext
assert sys.version_info >= (3, 4) # make sure we have Python 3.4+
assert spark.version >= '2.2' # make sure we have Spark 2.2+
 
def main():
	schema = StructType([
		StructField('business_id', StringType(), False),
        StructField('business_name', StringType(), False),
        StructField('business_neighborhood', StringType(), False),
        StructField('business_address', StringType(), False),
        StructField('business_city', StringType(), False),
        StructField('business_state', StringType(), False),
        StructField('business_postal_code', StringType(), False),
        StructField('business_latitude', FloatType(), False),
        StructField('business_longitude', FloatType(), False),
        StructField('business_category', StringType(), False),
        StructField('user_rate_stars', IntegerType(), False),
        StructField('user_id', StringType(), False)
	])
	data = spark.read.json('usersintorontonewcategory', schema = schema)
	data.createOrReplaceTempView('data')

	# print(data.count())
	# print('*********data***********')


	schema_user = StructType([
		StructField('user_id', types.StringType(), False),
		StructField('name', types.StringType(), False),
		StructField('review_count', types.IntegerType(), False),
		StructField('yelping_since', types.StringType(), False),
		StructField('friends', types.ArrayType(StringType()), False),
		StructField('useful', types.IntegerType(), False),
		StructField('funny', types.IntegerType(), False),
		StructField('cool', types.IntegerType(), False),
		StructField('fans', types.IntegerType(), False),
		StructField('elite', types.StringType(), False),
		StructField('average_stars', types.FloatType(), False),
		StructField('compliment_hot', types.IntegerType(), False),
		StructField('compliment_more', types.IntegerType(), False),
		StructField('compliment_profile', types.IntegerType(), False),
		StructField('compliment_cute', types.IntegerType(), False),
		StructField('compliment_list', types.IntegerType(), False),
		StructField('compliment_note', types.IntegerType(), False),
		StructField('compliment_plain', types.IntegerType(), False),
		StructField('compliment_cool', types.IntegerType(), False),
		StructField('compliment_funny', types.IntegerType(), False),
		StructField('compliment_writer', types.IntegerType(), False),
		StructField('compliment_photos', types.IntegerType(), False),
	])
	user = spark.read.json('user.json', schema = schema_user)
	user.createOrReplaceTempView('user')
	user = user.select('user_id', 'review_count')

	# add review to original toronto_business_user table
	data = data.join(user, 'user_id')

	user_node = data.select('user_id', 'review_count').distinct() \
								.sort(desc('review_count')) \
								.limit(500) \
								.select('user_id')
	
	toronto_user_node = data.join(user_node, 'user_id', 'inner') \
								.select('user_id','business_id').distinct() \
								.groupby('user_id').agg(count('business_id')) \
								.sort(desc('count(business_id)'))
	toronto_user_node.createOrReplaceTempView('toronto_user_node')
	toronto_user_node.write.save('toronto_TOP_user_node', format='json', mode='overwrite')
	# print(toronto_user_node.count())
	# print('*********toronto_user_node***********')
	# toronto_user_node.show()

	# user_business_link = data.join(toronto_user_node, 'user_id', 'inner') \
	# 							.select('user_id','business_id', 'user_rate_stars').distinct()

	# user_business_link.createOrReplaceTempView('user_business_link')
	# # print(user_business_link.count())
	# # print('*********user_business_link***********')
	# # user_business_link.show()
	# user_business_link.write.save('user_business_link', format='json', mode='overwrite')	

	# business_node = data.join(toronto_user_node, 'user_id', 'inner') \
	# 					.select('business_id', 'business_category') \
	# 					.distinct()
	# business_node.createOrReplaceTempView('business_node')
	# business_node.write.save('business_node', format='json', mode='overwrite')					
	# # business_node.show()
	# print(business_node.count())
	# print('*********business_node***********')
	
if __name__ == "__main__":
	main()
