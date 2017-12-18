from pyspark.ml.recommendation import ALSModel
from pyspark.sql import SparkSession

modelAll_input_dir = './input/modelAll'
modelRest_input_dir = './input/modelRest'

class RecommenderSystem:
    def __init__(self, sc, spark):
        self.sc = sc
        self.spark = spark

    def predict_ratings(self, df, type):
        if type == 'Restaurants':
            model = ALSModel.load(modelRest_input_dir)
        else:
            model = ALSModel.load(modelAll_input_dir)
            
        predictions = model.transform(df)
        return predictions

    def get_top_rating_b(self, df, type, n):
        predictions = self.predict_ratings(df, type)
        predictions.write.save('predictions', format='json', mode='overwrite')
        predictions.createOrReplaceTempView('predictions')
        top_b = self.spark.sql(" SELECT user_id, item_id, prediction FROM predictions ORDER BY prediction DESC LIMIT {0}".format(n))
        #top_b.show()
        return top_b

        
