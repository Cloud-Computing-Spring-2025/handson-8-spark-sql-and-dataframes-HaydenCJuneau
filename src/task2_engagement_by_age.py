from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import avg, col

spark = SparkSession.builder.appName("EngagementByAgeGroup").getOrCreate()

# Load datasets
posts_df = spark.read.option("header", True).csv("/opt/bitnami/spark/Social/input/input/posts.csv", inferSchema=True)
users_df = spark.read.option("header", True).csv("/opt/bitnami/spark/Social/input/input/users.csv", inferSchema=True)

def engagement_by_age(posts: DataFrame, users: DataFrame) -> DataFrame:
    df = posts.join(users, posts.UserID == users.UserID, "inner")

    
    averages = df.groupBy("AgeGroup").agg(
        avg(df.Likes).alias("avg_likes"), 
        avg(df.Retweets).alias("avg_retweets"),
    )

    return averages.orderBy("avg_likes", ascending=False)

# Save result
engagement_by_age(posts_df, users_df).coalesce(1).write.mode("overwrite").csv("outputs/engagement_by_age.csv", header=True)
