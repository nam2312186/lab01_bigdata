# lab01_exercise.py
# Big Data Lab 01 – Exercises with Kafka + Spark
from pyspark.sql import SparkSession
import pyspark
import os


def init_spark():
    """
    Khởi tạo SparkSession cho bài Exercises, có Kafka connector.
    Lưu ý: PySpark 4.0.1 + Spark 4.0.1 nhưng KAFKA CONNECTOR DÙNG 3.5.4
    vì spark-sql-kafka-0-10_2.12:4.0.1 chưa có trên Maven.
    """
    print("=== Init Spark for Exercises ===")
    print("PySpark version :", pyspark.__version__)

    # ⚠️ KHÔNG dùng pyspark.__version__ cho Kafka connector nữa
    KAFKA_CONNECTOR_VERSION = "3.5.4"
    kafka_pkg = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{KAFKA_CONNECTOR_VERSION}"
    print("Kafka package   :", kafka_pkg)

    builder = (
        SparkSession.builder
        .appName("BigData Lab01 - Exercises")
        .master("local[*]")
        # Download connector Kafka 3.5.4
        .config("spark.jars.packages", kafka_pkg)
        .config("spark.sql.shuffle.partitions", "4")
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# =========================
# 2. Helper: đọc Kafka từ 3 broker
# =========================

def read_kafka_topic(spark, topic, starting_offsets="earliest"):
    """
    Đọc 1 topic Kafka từ cả 3 broker, rồi union lại.

    Schema Kafka raw:
      key: binary
      value: binary
      topic: string
      partition: int
      offset: long
      timestamp: timestamp
      timestampType: int
    """

    brokers_list = [
        "203.205.33.134:29090",
        "203.205.33.135:29091",
        "203.205.33.136:29092",
    ]
    bootstrap_servers = ",".join(brokers_list)

    print(f"\n=== Reading topic '{topic}' from brokers: {bootstrap_servers} ===")

    df = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .load()
    )

    # value là binary -> cast sang string
    df = df.selectExpr("CAST(value AS STRING) as value")

    return df


# =========================
# 3. Parse từng loại data
# =========================

def parse_ratings_df(kafka_df):
    """
    Input từ Kafka: value = 'userId,movieId,rating,timestamp'
    Trả về DataFrame: (userId, movieId, rating, timestamp)
    """

    print("=== Parsing ratings data ===")

    schema = StructType([
        StructField("userId", IntegerType(), True),
        StructField("movieId", IntegerType(), True),
        StructField("rating", FloatType(), True),
        StructField("timestamp", LongType(), True),
    ])

    # split bằng "," rồi ép schema thủ công
    split_col = F.split(F.col("value"), ",")

    parsed_df = (
        kafka_df
        .withColumn("userId", split_col.getItem(0).cast(IntegerType()))
        .withColumn("movieId", split_col.getItem(1).cast(IntegerType()))
        .withColumn("rating", split_col.getItem(2).cast(FloatType()))
        .withColumn("timestamp", split_col.getItem(3).cast(LongType()))
        .drop("value")
    )

    # Bỏ record lỗi format
    parsed_df = parsed_df.na.drop(subset=["userId", "movieId", "rating"])

    return parsed_df


def parse_movies_df(kafka_df):
    """
    Input: value = 'movieId,title,genres'
    Một số title có dấu phẩy, nhưng thường lab dùng dạng đơn giản.
    Nếu format chuẩn là CSV, tốt nhất dùng Spark CSV parser, nhưng ở đây
    mình split đơn giản: movieId = col0, title = col1, genres = col2+.
    """

    print("=== Parsing movies data ===")

    raw = (
        kafka_df
        .withColumn("splits", F.split(F.col("value"), ","))
    )

    movies = (
        raw
        .withColumn("movieId", F.col("splits").getItem(0).cast(IntegerType()))
        .withColumn("title", F.col("splits").getItem(1))
        .withColumn(
            "genres",
            F.array_join(F.expr("slice(splits, 3, size(splits)-2)"), "|")
        )
        .drop("value", "splits")
    )

    movies = movies.na.drop(subset=["movieId"])
    return movies


def parse_tags_df(kafka_df):
    """
    Input: value = 'userId,movieId,tag,timestamp'
    Output: (userId, movieId, tag, timestamp)
    """

    print("=== Parsing tags data ===")

    split_col = F.split(F.col("value"), ",")

    tags = (
        kafka_df
        .withColumn("userId", split_col.getItem(0).cast(IntegerType()))
        .withColumn("movieId", split_col.getItem(1).cast(IntegerType()))
        .withColumn("tag", split_col.getItem(2))
        .withColumn("timestamp", split_col.getItem(3).cast(LongType()))
        .drop("value")
    )

    tags = tags.na.drop(subset=["movieId", "tag"])
    return tags


# =========================
# 4. Câu hỏi & export CSV
# =========================

def save_csv(df, path):
    """
    Ghi DataFrame ra CSV (overwrite, có header).
    """
    (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(path)
    )
    print(f"--> Saved CSV: {path}")


def question1_output_raw(ratings_df, movies_df, tags_df):
    """
    Không phải câu hỏi trong đề, nhưng để lab rõ ràng:
    Xuất dữ liệu thô ra CSV để kiểm tra.

    - output/q1_ratings_raw.csv
    - output/q1_movies_raw.csv
    - output/q1_tags_raw.csv
    """

    os.makedirs("output", exist_ok=True)

    save_csv(ratings_df, "output/q1_ratings_raw")
    save_csv(movies_df, "output/q1_movies_raw")
    save_csv(tags_df, "output/q1_tags_raw")


def question2_top5_movies(ratings_df, movies_df):
    """
    Q2: Get top 5 movies (by Id) by average rating,
        whose rating count is higher than 30.
    Ghi ra CSV: output/q2_top5_movies.csv
    """

    print("\n=== Q2: Top 5 movies by avg rating (count > 30) ===")

    movie_stats = (
        ratings_df
        .groupBy("movieId")
        .agg(
            F.count("*").alias("rating_count"),
            F.avg("rating").alias("avg_rating")
        )
        .filter(F.col("rating_count") > 30)
    )

    joined = (
        movie_stats
        .join(movies_df, on="movieId", how="left")
        .select("movieId", "title", "rating_count", "avg_rating")
        .orderBy(F.col("avg_rating").desc())
        .limit(5)
    )

    joined.show(truncate=False)

    save_csv(joined, "output/q2_top5_movies")


def question3_worst5_tags(ratings_df, tags_df):
    """
    Q3: Get 5 worst tags (associated with lowest average rating).

    Ý tưởng:
      - Join ratings với tags theo movieId (và/hoặc userId nếu cần).
      - Group by tag -> avg rating.
      - Lấy 5 tag có avg_rating thấp nhất.
    Ghi ra CSV: output/q3_worst5_tags.csv
    """

    print("\n=== Q3: Worst 5 tags by avg rating ===")

    # join theo movieId là đủ để "gắn" rating vào tag
    ratings_tags = (
        tags_df
        .join(ratings_df, on=["movieId"], how="inner")
    )

    tag_stats = (
        ratings_tags
        .groupBy("tag")
        .agg(
            F.count("*").alias("rating_count"),
            F.avg("rating").alias("avg_rating")
        )
        .filter(F.col("rating_count") >= 5)  # bỏ tag quá ít sample
        .orderBy(F.col("avg_rating").asc())
        .limit(5)
    )

    tag_stats.show(truncate=False)
    save_csv(tag_stats, "output/q3_worst5_tags")

    return tag_stats  # để dùng tiếp cho Q4


def question4_tag_movie_stats(ratings_df, tags_df, worst_tags_df):
    """
    Q4: Với 5 worst tags, kiểm tra:
      - Mỗi tag gắn với bao nhiêu movie khác nhau?
      - Average rating của các movie đó là bao nhiêu?

    Output: output/q4_tag_stats.csv
    """

    print("\n=== Q4: Movies stats for worst tags ===")

    worst_tags_list = [row["tag"] for row in worst_tags_df.collect()]
    print("Worst tags:", worst_tags_list)

    filtered_tags = tags_df.filter(F.col("tag").isin(worst_tags_list))

    # join rating
    rt = filtered_tags.join(ratings_df, on="movieId", how="inner")

    # per-movie stats
    movie_stats = (
        rt.groupBy("tag", "movieId")
        .agg(F.avg("rating").alias("movie_avg_rating"))
    )

    # per-tag overall stats
    tag_movie_stats = (
        movie_stats
        .groupBy("tag")
        .agg(
            F.countDistinct("movieId").alias("movie_count"),
            F.avg("movie_avg_rating").alias("tag_avg_rating_over_movies")
        )
        .orderBy("tag")
    )

    tag_movie_stats.show(truncate=False)
    save_csv(tag_movie_stats, "output/q4_tag_stats")


# =========================
# 5. Main
# =========================

def main():
    spark = init_spark()

    # ---- 5.1: đọc Kafka các topic ----
    ratings_raw = read_kafka_topic(spark, "Lab1_ratings")
    movies_raw = read_kafka_topic(spark, "Lab1_movies")
    tags_raw = read_kafka_topic(spark, "Lab1_tags")

    # ---- 5.2: parse & cache ----
    ratings_df = parse_ratings_df(ratings_raw).cache()
    movies_df = parse_movies_df(movies_raw).cache()
    tags_df = parse_tags_df(tags_raw).cache()

    print(f"\nRatings count: {ratings_df.count()}")
    print(f"Movies  count: {movies_df.count()}")
    print(f"Tags    count: {tags_df.count()}")

    # ---- 5.3: Xuất dữ liệu thô (cho dễ debug) ----
    question1_output_raw(ratings_df, movies_df, tags_df)

    # ---- 5.4: Q2: Top 5 movies ----
    question2_top5_movies(ratings_df, movies_df)

    # ---- 5.5: Q3: Worst 5 tags ----
    worst_tags_df = question3_worst5_tags(ratings_df, tags_df)

    # ---- 5.6: Q4: Tag movie stats ----
    question4_tag_movie_stats(ratings_df, tags_df, worst_tags_df)

    print("\n=== DONE Lab01 – Exercises ===")
    spark.stop()


if __name__ == "__main__":
    main()
