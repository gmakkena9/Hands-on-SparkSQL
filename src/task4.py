from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum

# Task 4: Top Verified Users by Reach
spark = SparkSession.builder.appName("TopVerifiedUsers").getOrCreate()

posts_df = spark.read.option("header", True).option("inferSchema", True).csv("input/posts.csv")
users_df = spark.read.option("header", True).option("inferSchema", True).csv("input/users.csv")

verified_users = users_df.filter(col("Verified") == True)

top_verified = (
    posts_df.join(verified_users, on="UserID", how="inner")
    .withColumn("Reach", col("Likes") + col("Retweets"))
    .groupBy("Username")
    .agg(spark_sum("Reach").alias("Total_Reach"))
    .orderBy(col("Total_Reach").desc())
    .limit(5)
)

top_verified.show(truncate=False)
top_verified.toPandas().to_csv("outputs/top_verified_users.csv", index=False)

spark.stop()