from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import SparkSession, Row, functions, Column
from pyspark.sql.types import *
import sys

spark = SparkSession.builder.appName('YelpTorontoRS').getOrCreate()
spark.sparkContext.setCheckpointDir('./spark-date/checkpoint')
schema = StructType([
    StructField('user_id', LongType(), False),
    StructField('item_id', LongType(), False),
    StructField('rate', IntegerType(), False)
])

def selectModel():
    data=spark.read.json(sys.argv[2], schema=schema)
    (training, test) = data.randomSplit([0.8, 0.2])
    # For mltraindatall: best:1.30
    # rank=10,20,30,50,100, regParam=0.2,0.4,0.6   maxIter=20,30,35,50, alpha=0.4,0.6,1.0,2.0
    # best param: rank=20, regParam=0.4, maxIter=30
    # For mltraindatrest: best:1.26
    # rank=10,20,30, regParam=0.2,0.4,0.8   maxIter=20,30,50, alpha=0.4, 1.0
    # best param rank=20, regParam=0.4, maxIter=30
    als = ALS(rank=20, regParam=0.4, maxIter=30, userCol='user_id',
    itemCol='item_id', ratingCol='rate', coldStartStrategy="drop")
    model = als.fit(training)
    #test = test.drop('rate')
    predictions = model.transform(test)
    predictions.show()
    #evaluator = RegressionEvaluator(metricName='rmse', labelCol='rate', predictionCol='prediction')
    #rmse = evaluator.evaluate(predictions)
    #print ("Root-mean-square error = " + str(rmse))

def saveModel():
    data=spark.read.json(sys.argv[2], schema=schema)
    #als = ALS(rank=30, regParam=0.4, maxIter=30, userCol='user_id',
    #itemCol='item_id', ratingCol='rate', coldStartStrategy="drop")
    als = ALS(rank=20, regParam=0.4, maxIter=30, userCol='user_id',
    itemCol='item_id', ratingCol='rate', coldStartStrategy="drop")   
    model = als.fit(data)
    model.save(sys.argv[3])
    print ("Model has been saved to: " + sys.argv[2])

def main():
    if sys.argv[1] == 'select':
        selectModel()
    else:
        saveModel()

if __name__ == "__main__":
    main()
