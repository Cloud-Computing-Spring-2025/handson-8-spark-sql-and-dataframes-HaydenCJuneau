from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import when, avg, col

spark = SparkSession.builder.appName("SentimentVsEngagement").getOrCreate()

# Load posts data
posts_df = spark.read.option("header", True).csv("/opt/bitnami/spark/Social/input/input/posts.csv", inferSchema=True)

# Positive (> 0.3), Neutral (-0.3 to 0.3), Negative (< -0.3)
def sentiment_stats(df: DataFrame) -> DataFrame:
    labeled = df.withColumn(
        "SentimentLabel",
        when(df.SentimentScore > 0.3, "Positive")
        .when(df.SentimentScore.between(-0.3, 0.3), "Neutral")
        .when(df.SentimentScore < -0.3, "Negative")
    )

    averages = labeled.groupBy("SentimentLabel").agg(
        avg(labeled.Likes).alias("avg_likes"),
        avg(labeled.Retweets).alias("avg_retweets"),
    )

    return averages.orderBy("avg_likes", ascending=False)


# Save result
sentiment_stats(posts_df).coalesce(1).write.mode("overwrite").csv("/opt/bitnami/spark/Social/output/sentiment_engagement", header=True)
