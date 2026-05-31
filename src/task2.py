from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, round as spark_round

# Task 2: Engagement by Age Group
spark = SparkSession.builder.appName("EngagementByAge").getOrCreate()

posts_df = spark.read.option("header", True).option("inferSchema", True).csv("input/posts.csv")
users_df = spark.read.option("header", True).option("inferSchema", True).csv("input/users.csv")

engagement_by_age = (
    posts_df.join(users_df, on="UserID", how="inner")
    .groupBy("AgeGroup")
    .agg(
        spark_round(avg("Likes"), 2).alias("Avg_Likes"),
        spark_round(avg("Retweets"), 2).alias("Avg_Retweets"),
    )
    .orderBy(col("Avg_Likes").desc())
)

engagement_by_age.show(truncate=False)
engagement_by_age.toPandas().to_csv("outputs/engagement_by_age.csv", index=False)

spark.stop()