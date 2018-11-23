from pyspark.sql import SparkSession
# from pyspark.ml.evaluation import RegressionEvaluator
# from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
from pyspark.sql.functions import explode

spark = SparkSession.builder.appName("sentiment_analysis").getOrCreate()

# tweets = spark.read.load("hdfs:///user/maria_dev/data/tweet_test.csv",
tweets = spark.read.load("hdfs:///user/maria_dev/data/tweet_test.csv",
                    format="csv", sep=",", inferSchema="true", header="true")
training_data = spark.read.load("hdfs:///user/maria_dev/data/sentiment_training_data/subjectivity-polarity.csv",
                    format="csv", sep=",", inferSchema="true", header="true")
                
tweets.createOrReplaceTempView("tweets")
training_data.createOrReplaceTempView("training_data")

result = spark.sql("""
    SELECT *
    FROM training_data
    LIMIT 10
""")

for row in result.collect():
    print(row)