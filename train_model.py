from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF, StringIndexer
from pyspark.ml.classification import NaiveBayes

spark = SparkSession.builder.appName("TrainJobModel").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv("train.csv", header=True)

indexer = StringIndexer(inputCol="label", outputCol="label_idx").fit(df)
pipeline = Pipeline(stages=[
    Tokenizer(inputCol="text", outputCol="words"),
    StopWordsRemover(inputCol="words", outputCol="filtered"),
    HashingTF(inputCol="filtered", outputCol="tf", numFeatures=1000),
    IDF(inputCol="tf", outputCol="features"),
    NaiveBayes(featuresCol="features", labelCol="label_idx"),
])

data = indexer.transform(df)
train, test = data.randomSplit([0.8, 0.2], seed=42)
model = pipeline.fit(train)

# Évaluation rapide (utile à montrer au prof)
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
acc = MulticlassClassificationEvaluator(
    labelCol="label_idx", metricName="accuracy"
).evaluate(model.transform(test))
print("Accuracy :", acc)

model.write().overwrite().save("job_model")
indexer.write().overwrite().save("job_indexer")