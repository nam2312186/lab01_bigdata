import os
import sys

# ===============================
# 0. Chuẩn hoá môi trường Spark
# ===============================
def prepare_env():
    # 1) Bỏ SPARK_HOME bên ngoài (tránh dính cấu hình MinIO lỗi)
    old_spark_home = os.environ.pop("SPARK_HOME", None)
    if old_spark_home:
        print(f"SPARK_HOME removed for this run: {old_spark_home}")

    # 2) Xoá mọi submit args cũ (trong đó có chuỗi MinIO)
    for var in ("PYSPARK_SUBMIT_ARGS", "SPARK_SUBMIT_ARGS", "SPARK_SUBMIT_OPTS"):
        if var in os.environ:
            print(f"Clearing {var} from env (was: {os.environ[var]!r})")
            os.environ.pop(var)

    # 3) Bắt Spark dùng đúng Python của venv hiện tại
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

prepare_env()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum


# ===============================
# 2.1 Khởi tạo SparkSession
# ===============================
def initialize_spark():
    print("=== 2.1 Spark Initialization ===")
    print(f"Using Python executable: {sys.executable}")

    # Dùng Spark đi kèm gói pyspark + nạp thêm hadoop-aws & aws-sdk để chơi MinIO
    spark = (
        SparkSession.builder
        .appName("Lab01 - 2.x DataFrame")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367",
        )
        .getOrCreate()
    )

    print("Spark version:", spark.version)
    return spark


# ===============================
# 2.2 Làm việc với DataFrame
# ===============================
def section_2_2_dataframe(spark):
    print("\n=== 2.2 DataFrame basic operations ===")

    data = [
        ("Abraham", "Apples", 5),
        ("Jacob", "Bananas", 3),
        ("Jacob", "Oranges", 2),
        ("Benjamin", "Apples", 2),
        ("David", "Bananas", 2),
        ("David", "Oranges", 5),
        ("Esau", "Apples", 3),
        ("Esau", "Bananas", 5),
        ("Ishmael", "Oranges", 4),
        ("Samael", "Apples", 4),
        ("Samael", "Bananas", 1),
        ("Samael", "Oranges", 1),
    ]
    columns = ["Customer", "Item", "Quantity"]

    df = spark.createDataFrame(data, columns)

    print("\n=== Grocery DataFrame ===")
    df.show()

    print("\nRow count:", df.count())

    print("\n=== Schema ===")
    df.printSchema()

    # Tổng số Apples bán ra (cách 1)
    apples_total = (
        df.filter(col("Item") == "Apples")
        .groupBy()
        .agg(_sum("Quantity").alias("TotalApples"))
        .collect()[0]["TotalApples"]
    )
    print(f"\n[Approach 1] Total Apples sold: {apples_total}")

    # Bảng giá
    price_data = [("Apples", 1.0), ("Bananas", 0.5), ("Oranges", 0.8)]
    price_columns = ["Item", "Price"]
    price_df = spark.createDataFrame(price_data, price_columns)

    print("\n=== Price DataFrame ===")
    price_df.show()

    # Join purchases + price
    joined_df = (
        df.join(price_df, on="Item", how="inner")
        .select("Item", "Customer", "Quantity", "Price")
    )

    print("\n=== Joined DataFrame (purchases + price) ===")
    joined_df.show()

    # Tiền Jacob phải trả
    jacob_total = (
        joined_df.filter(col("Customer") == "Jacob")
        .withColumn("Cost", col("Quantity") * col("Price"))
        .groupBy()
        .agg(_sum("Cost").alias("TotalCost"))
        .collect()[0]["TotalCost"]
    )

    print(f"\nJacob's total grocery spending: ${jacob_total:.2f}")

    return df


# ===============================
# 2.3 Ghi DataFrame lên MinIO
# ===============================
def configure_minio(spark):
    print("\n=== 2.3 Configure MinIO (S3A) ===")
    hconf = spark._jsc.hadoopConfiguration()

    # Cấu hình tối thiểu để nói chuyện với MinIO
    hconf.set("fs.s3a.endpoint", "http://127.0.0.1:9000")
    hconf.set("fs.s3a.access.key", "admin")
    hconf.set("fs.s3a.secret.key", "admin12345")
    hconf.set("fs.s3a.path.style.access", "true")
    hconf.set("fs.s3a.connection.ssl.enabled", "false")

    # Timeout dùng số thuần (ms), không dùng "60s" nữa để tránh NumberFormatException
    hconf.set("fs.s3a.connection.establish.timeout", "60000")
    hconf.set("fs.s3a.connection.request.timeout", "60000")
    hconf.set("fs.s3a.connection.maximum", "200")

    print("MinIO endpoint set to http://127.0.0.1:9000 (bucket: big-data)")


def write_dataframe_to_minio(df, spark):
    print("\n=== 2.3 Writing DataFrame to MinIO ===")
    configure_minio(spark)

    # Chỉ lấy các dòng của Jacob
    jacob_df = df.filter(col("Customer") == "Jacob")

    # Spark sẽ tạo folder 'test/jacob' trong bucket 'big-data'
    # bên trong là các file part-*.csv (không phải 1 file jacob.csv duy nhất)
    output_path = "s3a://big-data/test/jacob"

    jacob_df.write.mode("overwrite").csv(output_path, header=True)

    print(f"✅ Jacob's rows written to {output_path}")
    print("   (Spark tạo nhiều part-*.csv trong folder đó)")


# ===============================
# Main
# ===============================
def main():
    spark = None
    try:
        spark = initialize_spark()
        df = section_2_2_dataframe(spark)
        write_dataframe_to_minio(df, spark)
    finally:
        if spark is not None:
            print("\nStopping SparkSession...")
            spark.stop()


if __name__ == "__main__":
    main()
