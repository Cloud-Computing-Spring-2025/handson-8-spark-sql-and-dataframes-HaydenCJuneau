from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, sum as _sum

spark = SparkSession.builder.appName("TopVerifiedUsers").getOrCreate()

# Load datasets
posts_df = spark.read.option("header", True).csv("/opt/bitnami/spark/Social/input/input/posts.csv", inferSchema=True)
users_df = spark.read.option("header", True).csv("/opt/bitnami/spark/Social/input/input/users.csv", inferSchema=True)

def top_verified(posts: DataFrame, users: DataFrame) -> DataFrame:
    verified = users.filter(users.Verified == True)

    combined = verified.join(posts, "UserID", "inner")

    summed = (
        combined.groupBy("Username")
        .sum("Likes", "Retweets")
        .withColumnRenamed("sum(Likes)", "total_likes")
        .withColumnRenamed("sum(Retweets)", "total_retweets")
    )

    return summed.orderBy("total_likes", ascending=False).limit(5)

# Save result
top_verified(posts_df, users_df).coalesce(1).write.mode("overwrite").csv("/opt/bitnami/spark/Social/output/top_verified_users.csv", header=True)
