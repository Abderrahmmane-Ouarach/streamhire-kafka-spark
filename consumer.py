from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("JobClassifier").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# SOURCE : le topic Kafka lu comme un flux
raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", "localhost:9092")
       .option("subscribe", "jobs")
       .option("startingOffsets", "earliest")
       .load())

texts = raw.selectExpr("CAST(value AS STRING) AS text")

# TRANSFORMATION : classification par mots-clés (l'ordre = priorité)
KEYWORDS = {
    "Securite": ["sécurité", "securite", "security", "pentest", "soc", "cyber", "siem"],
    "Cloud": ["cloud", "aws", "azure", "gcp", "kubernetes", "docker", "devops", "terraform"],
    "Data": ["data", "donnée", "spark", "sql", "power bi", "machine learning", "etl"],
    "Developpement": ["développeur", "developer", "java", "python", "react", "angular", "node", "full stack", "backend", "frontend"],
}

def classify(text):
    t = (text or "").lower()
    for domaine, words in KEYWORDS.items():
        if any(w in t for w in words):
            return domaine
    return "Autre"

result = texts.withColumn("domaine", udf(classify, StringType())(col("text")))

# SINK : affichage console
query = (result.writeStream.format("console")
         .outputMode("append")
         .option("truncate", "false")
         .start())
query.awaitTermination()
