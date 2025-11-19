from pyspark.sql import SparkSession

# Function tạo SparkSession giống Solution 2 của lab
def init_spark():
    spark = (
        SparkSession.builder
        .appName("Lab01-SparkInitialization")   # tên app
        .master("local[*]")                      # chạy local, đúng nội dung lab
        .config("spark.driver.memory", "4g")     # config giống lab
        .config("spark.executor.memory", "4g")
        .config("spark.executor.cores", "4")
        # config lab thường dùng (có thể bật nếu bạn muốn)
        # .config("spark.cores.max", "4")
        # .config("spark.executor.instances", "1")
        .getOrCreate()
    )

    return spark


if __name__ == "__main__":
    spark = init_spark()

    print("=== SparkSession Created Successfully ===")
    print(spark)

    # test giống lab: tạo DataFrame và show
    df = spark.range(0, 10)
    df.show()
