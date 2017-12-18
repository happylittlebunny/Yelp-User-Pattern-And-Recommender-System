from pyspark.sql import SparkSession
from pyspark.sql.types import *
import sys
from pyspark.sql import Row
from pyspark.sql.functions import udf

spark = SparkSession.builder.appName('preprocessdata').getOrCreate()

def convert_friends_id(friends, uid):   
    print ('123456')
    friends_str = friends[1:-1]
    fs = '(' + friends_str + ')'
    x =uid.where(uid['user_id'].isin(fs)).select('uid')
    #x = spark.sql(""" select uid from uid where user_id in {0}""".format(fs))
    friends_uids = x.rdd.map(lambda p:p.uid)
    print ('friends_id+'+str(friends_uids))
    return x

def main():
    user_file = sys.argv[1]
    convert_id_file = sys.argv[2]
   # user_file = sys.argv[2]
   # review_file = sys.argv[3]

    #output = sys.argv[3]

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
        StructField('user_id', StringType(), False),
        StructField('uid', LongType(), False),
    ])

    yelp_users = spark.read.json(user_file, schema1)
    yelp_users.createOrReplaceTempView('yelp_users')
    uid = spark.read.csv(convert_id_file, schema2)
    uid.createOrReplaceTempView('uid')

   # convertFriendsIds=udf(convert_friends_id, StringType())
  #  yelp_users.withColumn('friends_ids', convertFriendsIds('friends'))
    user_with_friends = spark.sql("select friends from yelp_users where friends !='[]' Limit 1")
 #   print ('friends + '+ str(user_with_friends.count()) + 'usercount + ' + str(new_users.count()))
   # user_with_friends.show()
    friends = user_with_friends.rdd.map(lambda p:p.friends).collect()[0]
    #print (friends_str)
    friends_str = '('+ ''.join(friends) +')'
    #fs = '(' + friends_str + ')'
    print (friends_str)
    x =uid.where(uid['user_id'].isin(friends_str)).select('uid')    
    x.show()
    #t = user_with_friends.rdd.map(lambda row: convert_friends_id(row, uid))
    #print (t.take(3))

    """
    new_users = spark.sql( 
    SELECT u.uid as id, y.name as name, y.review_count as review_count, 
    y.yelping_since as yelping_since, y.friends as friends, y.useful as useful, y.funny as funny,
    y.cool as cool, y.fans as fans, y.elite as elite, 
    y.average_stars as average_stars, y.compliment_hot as compliment_hot, y.compliment_more as compliment_more,
    y.compliment_profile as compliment_profile, y.compliment_cute as compliment_cute,
    y.compliment_list as compliment_list, y.compliment_note as compliment_note,
    y.compliment_plain as compliment_plain, y.compliment_cool as compliment_cool,
    y.compliment_funny as compliment_funny, y.compliment_writer as compliment_writer,
    y.compliment_photos as compliment_photos
    FROM uid u JOIN yelp_users y
    ON u.user_id = y.user_id
    )
    new_users.createOrReplaceTempView('new_users')

    #new_business.write.save(output, format='json', mode='overwrite')
    """
if __name__ == "__main__":
    main()