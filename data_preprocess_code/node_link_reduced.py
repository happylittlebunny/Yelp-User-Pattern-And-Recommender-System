import sys
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import explode, lit
from pyspark.sql.types import *

# inputs = 'business.json'
# output = sys.argv[1]
  
spark = SparkSession.builder.appName('clean data').getOrCreate()
# sc = spark.sparkContext
assert sys.version_info >= (3, 4) # make sure we have Python 3.4+
assert spark.version >= '2.2' # make sure we have Spark 2.2+
 
def main():
	# categories_list =["Restaurants", "Food", "Shopping", "Beauty & Spas", "Nightlife"]
	categories_list =["Restaurants", "Food", "Shopping", "Beauty & Spas", "Nightlife", "Active Life", "Event Planning & Services", "Arts & Entertainment", "Hotels & Travel", "Local Services", "Health & Medical", "Pets", "Automotive", "Home Services", "Public Services & Government", "Local Flavor", "Others", "Education", "Professional Services", "Financial Services"]

	# load required json files
	# for business node
	schema_business = StructType([
		StructField('business_id', types.StringType(), False),
		StructField('name', types.StringType(), False),
		StructField('neighborhood', types.StringType(), False),
		StructField('address', types.StringType(), False),
		StructField('city', types.StringType(), False),
		StructField('state', types.StringType(), False),
		StructField('postal_code', types.StringType(), False),
		StructField('latitude', types.FloatType(), False),
		StructField('longitude', types.FloatType(), False),
		StructField('stars', types.FloatType(), False),
		StructField('review_count', types.IntegerType(), False),
		StructField('is_open', types.IntegerType(), False),
		StructField('attributes', types.StringType(), False),
		StructField('categories', types.ArrayType(StringType()), False),
		StructField('hours', types.StringType(), False),
	])
	business_node = spark.read.json('business.json', schema = schema_business)
	business_node.createOrReplaceTempView('business_node')

	toronto_business = business_node.select('business_id', 'city', 'categories') \
									.where("lower(city) like '%toronto%'") \
									.select('business_id', 'categories')
	toronto_business.createOrReplaceTempView('toronto_business')

	business_node = toronto_business.withColumn('categories', explode('categories'))
	business_node = business_node[business_node.categories.isin(categories_list)]
	business_node = business_node.select('business_id').distinct() \
									.join(toronto_business, ['business_id'])
									# .select('business_id', 'categories')

	business_node.show()
	print(business_node.count())
	print('*********business_node***********')
	
	# business_node.write.save('business_node', format='json', mode='overwrite')

	# # user_business_link
	# schema_review = StructType([
	# 	StructField('review_id', types.StringType(), False),
	# 	StructField('user_id', types.StringType(), False),
	# 	StructField('business_id', types.StringType(), False),
	# 	StructField('stars', types.IntegerType(), False),
	# 	StructField('date', types.StringType(), False),
	# 	StructField('text', types.StringType(), False),
	# 	StructField('useful', types.IntegerType(), False),
	# 	StructField('funny', types.IntegerType(), False),
	# 	StructField('cool', types.IntegerType(), False),
	# ])
	# review = spark.read.json('review.json', schema = schema_review)
	# review.createOrReplaceTempView('review')

	# user_business_link = review.select('user_id', 'business_id', 'useful', 'funny', 'cool') \
	# 				.where(review['useful'] > 0).where(review['funny'] > 0).where(review['cool'] > 0) \
	# 				.join(business_node, ['business_id']) \
	# 				.drop('group', 'useful', 'funny', 'cool')	
	# user_business_link.createOrReplaceTempView('user_business_link')
	# # print(user_business_link.count())
	# # print('*********user_business_link***********')
	# # # user_business_link.show()
	# user_business_link.write.save('user_business_link', format='json', mode='overwrite')

	# # # user node
	# toronto_user = user_business_link.select('user_id').distinct()
	# toronto_user.createOrReplaceTempView('toronto_user')
	# toronto_user_node = toronto_user.withColumn('group', lit('2'))
	# toronto_user_node.createOrReplaceTempView('toronto_user_node')
	# # print(toronto_user_node.count())
	# # print('*********toronto_user***********')
	# # toronto_user_node.show()
	# toronto_user_node.write.save('toronto_user_node', format='json', mode='overwrite')


if __name__ == "__main__":
	main()
