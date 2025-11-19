import os
import pyspark
from pyspark.sql import SparkSession, functions as F, types as T

# =========================
# 0. Cấu hình chung
# =========================

# CHỈ DÙNG 1 BROKER ĐANG SỐNG
BROKERS = "203.205.33.134:29090"

MOVIES_TOPIC  = "Gr6h50_movies"
RATINGS_TOPIC = "Gr6h50_ratings"
TAGS_TOPIC    = "Gr6h50_tags"

OUTPUT_DIR = "output_exercise"


# =========================
# 1. Khởi tạo Spark + Kafka connector 3.5.4 (Scala 2.12)
# =========================
def init_spark():
    print("=== Init Spark for Exercises (Gr6h50) ===")
    print("PySpark version :", pyspark.__version__)

    kafka_pkg = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4"
    kafka_clients = "org.apache.kafka:kafka-clients:3.4.1"

    print("Kafka package   :", kafka_pkg)

    builder = (
        SparkSession.builder
        .appName("Lab01_Exercises_Gr6h50")
        .master("local[*]")
        .config("spark.jars.packages", f"{kafka_pkg},{kafka_clients}")
        .config("spark.sql.shuffle.partitions", "4")
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# =========================
# 2. Đọc 1 topic Kafka về DataFrame STRING
# =========================
def read_kafka_topic(spark, topic: str):
    print(f"\n=== Reading topic '{topic}' from brokers: {BROKERS} ===")

    df = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", BROKERS)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    df_str = df.selectExpr("CAST(value AS STRING) AS value", "timestamp")

    print(f"Sample from topic {topic}:")
    df_str.show(5, truncate=False)
    print(f"Total records in {topic} (raw) =", df_str.count())
    return df_str


# =========================
# 3. Parse JSON từng topic (JSON field đều là STRING)
# =========================
def parse_movies(df_str):
    # JSON: {"movieId": "1", "title": "Movie 1", "genres": "Comedy"}
    schema_str = T.StructType([
        T.StructField("movieId", T.StringType(), True),
        T.StructField("title",   T.StringType(), True),
        T.StructField("genres",  T.StringType(), True),
    ])

    raw = (
        df_str
        .select(F.from_json("value", schema_str).alias("data"))
        .select("data.*")
    )

    parsed = (
        raw
        .withColumn("movieId", F.col("movieId").cast("int"))
        .dropna(subset=["movieId"])
    )

    print("\n[DEBUG] Movies parsed count =", parsed.count())
    parsed.show(5, truncate=False)
    return parsed


def parse_ratings(df_str):
    # JSON: {"userId": "36", "movieId": "22", "rating": "5", "timestamp": "1604..."}
    schema_str = T.StructType([
        T.StructField("userId",    T.StringType(), True),
        T.StructField("movieId",   T.StringType(), True),
        T.StructField("rating",    T.StringType(), True),
        T.StructField("timestamp", T.StringType(), True),
    ])

    raw = (
        df_str
        .select(F.from_json("value", schema_str).alias("data"))
        .select("data.*")
    )

    parsed = (
        raw
        .withColumn("userId",    F.col("userId").cast("int"))
        .withColumn("movieId",   F.col("movieId").cast("int"))
        .withColumn("rating",    F.col("rating").cast("double"))
        .withColumn("timestamp", F.col("timestamp").cast("long"))
        .dropna(subset=["movieId", "rating"])
    )

    print("\n[DEBUG] Ratings parsed count =", parsed.count())
    parsed.show(5, truncate=False)
    return parsed


def parse_tags(df_str):
    # JSON: {"userId": "28", "movieId": "53", "tag": "must-see", "timestamp": "1603..."}
    schema_str = T.StructType([
        T.StructField("userId",    T.StringType(), True),
        T.StructField("movieId",   T.StringType(), True),
        T.StructField("tag",       T.StringType(), True),
        T.StructField("timestamp", T.StringType(), True),
    ])

    raw = (
        df_str
        .select(F.from_json("value", schema_str).alias("data"))
        .select("data.*")
    )

    parsed = (
        raw
        .withColumn("userId",    F.col("userId").cast("int"))
        .withColumn("movieId",   F.col("movieId").cast("int"))
        .withColumn("timestamp", F.col("timestamp").cast("long"))
        .dropna(subset=["movieId", "tag"])
    )

    print("\n[DEBUG] Tags parsed count =", parsed.count())
    parsed.show(5, truncate=False)
    return parsed


# =========================
# 4. Các câu hỏi & xuất CSV
# =========================
def save_q1_samples(movies_df, ratings_df, tags_df):
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    (
        movies_df.limit(20)
        .coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(os.path.join(OUTPUT_DIR, "q1_movies_sample"))
    )

    (
        ratings_df.limit(20)
        .coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(os.path.join(OUTPUT_DIR, "q1_ratings_sample"))
    )

    (
        tags_df.limit(20)
        .coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(os.path.join(OUTPUT_DIR, "q1_tags_sample"))
    )

    print("Q1: Saved samples to CSV under", OUTPUT_DIR)


def solve_q2_top5_movies(ratings_df, movies_df):
    agg = (
        ratings_df
        .groupBy("movieId")
        .agg(
            F.count("*").alias("rating_count"),
            F.avg("rating").alias("avg_rating"),
        )
        .filter(F.col("rating_count") > 10)
    )

    result = (
        agg.join(movies_df, on="movieId", how="left")
        .select("movieId", "title", "rating_count", "avg_rating")
        .orderBy(F.desc("avg_rating"))
        .limit(5)
    )

    out_path = os.path.join(OUTPUT_DIR, "q2_top5_movies")
    (
        result.coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(out_path)
    )

    print("Q2: Saved top 5 movies to", out_path)
    result.show(truncate=False)
    return result


def solve_q3_worst_tags(ratings_df, tags_df):
    joined = (
        ratings_df.join(tags_df, on="movieId", how="inner")
        .filter(F.col("tag").isNotNull())
    )

    tag_stats = (
        joined
        .groupBy("tag")
        .agg(
            F.count("*").alias("rating_count"),
            F.avg("rating").alias("avg_rating"),
        )
        .filter(F.col("rating_count") > 10)
        .orderBy(F.asc("avg_rating"))
        .limit(5)
    )

    out_path = os.path.join(OUTPUT_DIR, "q3_worst_tags")
    (
        tag_stats.coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(out_path)
    )

    print("Q3: Saved 5 worst tags to", out_path)
    tag_stats.show(truncate=False)
    return tag_stats


def solve_q4_movies_for_worst_tags(ratings_df, tags_df, worst_tags_df, movies_df):
    worst_tags_list = [row["tag"] for row in worst_tags_df.collect()]
    print("Worst tags:", worst_tags_list)

    tags_filtered = tags_df.filter(F.col("tag").isin(worst_tags_list))

    joined = ratings_df.join(tags_filtered, on="movieId", how="inner")

    movie_tag_stats = (
        joined
        .groupBy("tag", "movieId")
        .agg(
            F.count("*").alias("rating_count"),
            F.avg("rating").alias("avg_rating"),
        )
    )

    movie_tag_stats = (
        movie_tag_stats
        .join(movies_df, on="movieId", how="left")
        .select("tag", "movieId", "title", "rating_count", "avg_rating")
        .orderBy("tag", F.desc("avg_rating"))
    )

    out_path = os.path.join(OUTPUT_DIR, "q4_movies_per_worst_tag")
    (
        movie_tag_stats.coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(out_path)
    )

    print("Q4: Saved movies per worst tag to", out_path)
    movie_tag_stats.show(50, truncate=False)
    return movie_tag_stats


# =========================
# 5. Main
# =========================
def main():
    spark = init_spark()

    movies_raw  = read_kafka_topic(spark, MOVIES_TOPIC)
    ratings_raw = read_kafka_topic(spark, RATINGS_TOPIC)
    tags_raw    = read_kafka_topic(spark, TAGS_TOPIC)

    movies_df  = parse_movies(movies_raw)
    ratings_df = parse_ratings(ratings_raw)
    tags_df    = parse_tags(tags_raw)

    print("\n=== Movies schema ===")
    movies_df.printSchema()
    print("\n=== Ratings schema ===")
    ratings_df.printSchema()
    print("\n=== Tags schema ===")
    tags_df.printSchema()

    save_q1_samples(movies_df, ratings_df, tags_df)
    top5_df = solve_q2_top5_movies(ratings_df, movies_df)
    worst_tags_df = solve_q3_worst_tags(ratings_df, tags_df)
    _ = solve_q4_movies_for_worst_tags(ratings_df, tags_df, worst_tags_df, movies_df)

    spark.stop()
    print("\n=== Done all questions (Q1–Q4) ===")


if __name__ == "__main__":
    main()
