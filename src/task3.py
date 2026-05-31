from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, when, round as spark_round

# Task 3: Sentiment vs Engagement
spark = SparkSession.builder.appName("SentimentEngagement").getOrCreate()

posts_df = spark.read.option("header", True).option("inferSchema", True).csv("input/posts.csv")

labeled_df = posts_df.withColumn(
    "Sentiment",
    when(col("SentimentScore") > 0.3, "Positive")
    .when(col("SentimentScore") < -0.3, "Negative")
    .otherwise("Neutral"),
)

sentiment_engagement = (
    labeled_df.groupBy("Sentiment")
    .agg(
        spark_round(avg("Likes"), 2).alias("Avg_Likes"),
        spark_round(avg("Retweets"), 2).alias("Avg_Retweets"),
    )
    .orderBy(col("Avg_Likes").desc())
)

sentiment_engagement.show(truncate=False)
sentiment_engagement.toPandas().to_csv("outputs/sentiment_engagement.csv", index=False)

spark.stop()