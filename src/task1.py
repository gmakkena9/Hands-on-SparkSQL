from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, count

# Initialize Spark Session
spark = SparkSession.builder.appName("HashtagTrends").getOrCreate()

# Load posts data
posts_df = spark.read.option("header", True).csv("input/posts.csv")

# Split the Hashtags column, count frequency of each hashtag, sort descending
hashtag_counts = (
    posts_df
    .withColumn("Hashtag", explode(split(col("Hashtags"), ",")))
    .groupBy("Hashtag")
    .agg(count("*").alias("Count"))
    .orderBy(col("Count").desc())
)

hashtag_counts.show(truncate=False)

# Save result (pandas write avoids needing winutils.exe on Windows)
hashtag_counts.toPandas().to_csv("outputs/hashtag_trends.csv", index=False)

spark.stop()