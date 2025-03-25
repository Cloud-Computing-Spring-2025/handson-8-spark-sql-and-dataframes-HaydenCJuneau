from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import explode, split, col

# Initialize Spark Session
spark = SparkSession.builder.appName("HashtagTrends").getOrCreate()

# Load posts data
posts_df = spark.read.option("header", True).csv("/opt/bitnami/spark/Social/input/input/posts.csv")

# TODO: Split the Hashtags column into individual hashtags and count the frequency of each hashtag and sort descending

def detect_hashtag_trends(df: DataFrame) -> DataFrame:
    df_split = df.select(split(df.Hashtags, ',').alias("Hashtags"))
    df_expanded = df_split.select(explode(df_split.Hashtags).alias("Hashtags"))

    total_counts = df_expanded.groupBy("Hashtags").count().withColumnRenamed("count", "Count") 

    return total_counts.orderBy("Count", ascending=False).limit(10)
    

# Save result
detect_hashtag_trends(posts_df).coalesce(1).write.mode("overwrite").csv("/opt/bitnami/spark/Social/output/hashtag_trends.csv", header=True)
