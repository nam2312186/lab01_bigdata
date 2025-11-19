import os
from pyspark.sql import SparkSession, functions as F, types as T
import pyspark

# =========================
# 0. Config chung
# =========================

BROKERS = "203.205.33.134:29090,203.205.33.135:29091,203.205.33.136:29092"

MOVIES_TOPIC  = "Gr6h50_movies"
RATINGS_TOPIC = "Gr6h50_ratings"
TAGS_TOPIC    = "Gr6h50_tags"

OUTPUT_DIR = "output_exercise"   # thư mục chứa các file csv kết quả


# =========================
# 1. Khởi tạo Spark
# =========================
def init_spark():
    print("=== Init Spark for Exercises ===")
    print("PySpark version :", pyspark.__version__)

    # Spark 4.0.1 (Scala 2.13) dùng connector _2.13:3.5.4
    kafka_pkg = "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.4"
    extra_pkgs = ",".join([
        kafka_pkg,
        "org.apache.kafka:kafka-clients:3.4.1"
    ])

    print("Kafka package   :", kafka_pkg)

    spark = (
        SparkSession.builder
        .appName("Lab01-Exercises")
        .master("local[*]")
        .config("spark.jars.packages", extra_pkgs)
        .getOrCreate()
    )

    return spark

# =========================
# 2. Đọc dữ liệu từ Kafka
# =========================

def read_kafka_topic(spark, topic):
    print(f"\n=== Reading topic '{topic}' from brokers: {BROKERS} ===")
    df = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", BROKERS)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    # Kafka trả về: key (binary), value (binary), topic, partition, offset,...
    # Ta chỉ cần value, cast sang STRING (JSON)
    df_str = df.select(F.col("value").cast("string").alias("value"))
    print(f"Sample from topic {topic}:")
    df_str.show(5, truncate=False)
    return df_str


# =========================
# 3. Parse JSON -> DataFrame chuẩn
# =========================

def parse_movies(df_str):
    """
    Parse JSON của movies:
    {"movieId":"1","title":"Movie 1","genres":"Comedy"}
    """
    schema = T.StructType([
        T.StructField("movieId", T.IntegerType(), True),
        T.StructField("title",   T.StringType(),  True),
        T.StructField("genres",  T.StringType(),  True),
    ])

    df_parsed = df_str.select(
        F.from_json("value", schema).alias("data")
    ).select("data.*")

    print("\n[Movies] Parsed schema:")
    df_parsed.printSchema()
    df_parsed.show(5, truncate=False)
    return df_parsed


def parse_ratings(df_str):
    """
    Parse JSON của ratings:
    {"userId":1,"movieId":1,"rating":4.0,"timestamp":123456789}
    """
    schema = T.StructType([
        T.StructField("userId",    T.IntegerType(), True),
        T.StructField("movieId",   T.IntegerType(), True),
        T.StructField("rating",    T.DoubleType(),  True),
        T.StructField("timestamp", T.LongType(),    True),
    ])

    df_parsed = df_str.select(
        F.from_json("value", schema).alias("data")
    ).select("data.*")

    print("\n[Ratings] Parsed schema:")
    df_parsed.printSchema()
    df_parsed.show(5, truncate=False)
    return df_parsed


def parse_tags(df_str):
    """
    Parse JSON của tags:
    {"userId":1,"movieId":1,"tag":"funny","timestamp":123456789}
    """
    schema = T.StructType([
        T.StructField("userId",    T.IntegerType(), True),
        T.StructField("movieId",   T.IntegerType(), True),
        T.StructField("tag",       T.StringType(),  True),
        T.StructField("timestamp", T.LongType(),    True),
    ])

    df_parsed = df_str.select(
        F.from_json("value", schema).alias("data")
    ).select("data.*")

    print("\n[Tags] Parsed schema:")
    df_parsed.printSchema()
    df_parsed.show(5, truncate=False)
    return df_parsed


# =========================
# 4. Q1–Q4 + ghi CSV
# =========================

def q1_top5_movies(ratings_df, movies_df):
    """
    Q1: Top 5 movies (by Id) by average rating, whose rating count > 30.
    Ghi ra: output_exercise/q1_top5_movies.csv
    """
    print("\n=== Q1: Top 5 movies by avg rating (count > 30) ===")

    agg = (
        ratings_df.groupBy("movieId")
        .agg(
            F.count("*").alias("rating_count"),
            F.avg("rating").alias("avg_rating"),
        )
        .filter(F.col("rating_count") > 30)
    )

    result = (
        agg.join(movies_df, on="movieId", how="left")
        .orderBy(F.col("avg_rating").desc(), F.col("movieId"))
        .limit(5)
    )

    out_path = os.path.join(OUTPUT_DIR, "q1_top5_movies")
    (
        result.coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(out_path)
    )

    print(f"Q1 result written to: {out_path}")
    result.show(truncate=False)
    return result


def q2_worst_tags(ratings_df, tags_df):
    """
    Q2: Get 5 worst tags (associated with lowest average rating).
    Ý tưởng:
      - Join ratings + tags theo movieId
      - Group by tag -> avg(rating), count(*), count distinct movieId
      - Lấy 5 tag có avg_rating thấp nhất
    Ghi ra: output_exercise/q2_worst_tags.csv
    """
    print("\n=== Q2: 5 worst tags by avg rating ===")

    joined = (
        ratings_df.select("movieId", "rating")
        .join(tags_df.select("movieId", "tag"), on="movieId", how="inner")
    )

    tag_stats = (
        joined.groupBy("tag")
        .agg(
            F.countDistinct("movieId").alias("movie_count"),
            F.count("*").alias("rating_count"),
            F.avg("rating").alias("avg_rating"),
        )
        # Có thể lọc thêm: chỉ xét tag xuất hiện >= vài lần nếu muốn
        .orderBy(F.col("avg_rating").asc(), F.col("rating_count").desc())
        .limit(5)
    )

    out_path = os.path.join(OUTPUT_DIR, "q2_worst_tags")
    (
        tag_stats.coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(out_path)
    )

    print(f"Q2 result written to: {out_path}")
    tag_stats.show(truncate=False)
    return tag_stats


def q3_q4_movies_for_worst_tags(ratings_df, tags_df, movies_df, worst_tags_df):
    """
    Q3 + Q4:
      - Với 5 tag tệ nhất (Q2):
         + Check average rating của từng movie có các tag này
         + Đếm bao nhiêu movie gắn các tag này
    Ghi ra:
      - output_exercise/q3_movies_with_worst_tags.csv
      - output_exercise/q4_movies_count_per_worst_tag.csv
    """
    print("\n=== Q3 & Q4: Analyze movies for worst tags ===")

    worst_tags = [row["tag"] for row in worst_tags_df.collect()]
    print("Worst tags:", worst_tags)

    joined = (
        ratings_df.select("movieId", "rating")
        .join(tags_df.select("movieId", "tag"), on="movieId", how="inner")
        .filter(F.col("tag").isin(worst_tags))
    )

    # Q3: Thống kê theo movie
    movies_stats = (
        joined.groupBy("movieId")
        .agg(
            F.collect_set("tag").alias("tags"),
            F.count("*").alias("rating_count"),
            F.avg("rating").alias("avg_rating"),
        )
        .join(movies_df, on="movieId", how="left")
        .orderBy(F.col("avg_rating").asc())
    )

    out_q3 = os.path.join(OUTPUT_DIR, "q3_movies_with_worst_tags")
    (
        movies_stats.coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(out_q3)
    )
    print(f"Q3 result written to: {out_q3}")
    movies_stats.show(10, truncate=False)

    # Q4: Đếm số movie per tag trong 5 worst tags
    movies_per_tag = (
        movies_stats
        .select(F.explode("tags").alias("tag"), "movieId")
        .distinct()
        .groupBy("tag")
        .agg(F.count("*").alias("movie_count"))
        .orderBy("movie_count")
    )

    out_q4 = os.path.join(OUTPUT_DIR, "q4_movies_count_per_worst_tag")
    (
        movies_per_tag.coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(out_q4)
    )
    print(f"Q4 result written to: {out_q4}")
    movies_per_tag.show(truncate=False)

    return movies_stats, movies_per_tag


# =========================
# 5. Main
# =========================

def main():
    spark = init_spark()

    # Đảm bảo thư mục output tồn tại
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 1) Đọc 3 topic từ Kafka
    movies_raw  = read_kafka_topic(spark, MOVIES_TOPIC)
    ratings_raw = read_kafka_topic(spark, RATINGS_TOPIC)
    tags_raw    = read_kafka_topic(spark, TAGS_TOPIC)

    # 2) Parse JSON
    movies_df  = parse_movies(movies_raw)
    ratings_df = parse_ratings(ratings_raw)
    tags_df    = parse_tags(tags_raw)

    # 3) Q1
    q1_result = q1_top5_movies(ratings_df, movies_df)

    # 4) Q2
    worst_tags_df = q2_worst_tags(ratings_df, tags_df)

    # 5) Q3 + Q4
    q3_q4_movies_for_worst_tags(ratings_df, tags_df, movies_df, worst_tags_df)

    spark.stop()
    print("\n=== DONE Lab01 Exercises (Gr6h50) ===")


if __name__ == "__main__":
    main()
