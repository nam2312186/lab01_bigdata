# Lab01 – Spark + Kafka – Movie Ratings/Tags (Nhóm Gr6h50)

## 1. Thông tin nhóm & mục tiêu bài lab

**MSSV / Nhóm:** Nhóm Gr6h50 – môn Big Data (CO311 / BigData311).

**Mục tiêu:**

- Tự sinh bộ dữ liệu giả về phim – đánh giá – tag.
- Đưa dữ liệu này lên Kafka cluster của HPCC (server 203.205.33.134:29090) dưới 3 topic:
  - `Gr6h50_movies`
  - `Gr6h50_ratings`
  - `Gr6h50_tags`
- Dùng PySpark + Spark SQL Kafka connector để:
  - Đọc dữ liệu từ Kafka.
  - Parse JSON thành DataFrame.
  - Trả lời 4 câu hỏi:
    - **Q1:** Lấy mẫu dữ liệu từ Kafka (3 topic) và lưu CSV.
    - **Q2:** Top 5 phim có điểm trung bình cao nhất với số lượt rating > 10.
    - **Q3:** 5 tags tệ nhất (average rating thấp nhất).
    - **Q4:** Với các tags tệ ở Q3, kiểm tra rating trung bình và số lượng rating cho từng phim mang các tag đó.

---

## 2. Cấu trúc thư mục

Trong thư mục `lab01`:

```
lab01/
│  lab01_gen_data.py          # Sinh dữ liệu giả vào thư mục input/
│  lab01_produce_Gr6h50.py    # Producer: đẩy dữ liệu lên Kafka server HPCC
│  lab01_exercise.py          # Dùng Spark + Kafka để trả lời Q1–Q4
│  requirements.txt           # Danh sách thư viện Python cần cài
│  .gitignore
│
├─BigData311/                 # Virtualenv (sẽ được tạo)
├─input/                      # movies.csv, ratings.csv, tags.csv (sau khi gen)
└─output_exercise/            # chứa các kết quả CSV Q1–Q4
```

---

## 3. Chuẩn bị môi trường

### 3.1. Yêu cầu chung

- Python 3.10 hoặc 3.11
- Java JDK 8+ (để Spark chạy được)
- Đã cài Kafka client (thư mục kiểu `D:\Kafka\kafka_2.13-3.7.0\bin\windows\...`) để kiểm tra topic bằng `kafka-topics.bat`, `kafka-console-consumer.bat`.
- Có file cấu hình OpenVPN của HPCC và đã kết nối thành công.

Khi kết nối OpenVPN xong sẽ thấy icon OpenVPN báo:
```
Connected to: h6-2025, Assigned IP: 10.8.0.x
```

### 3.2. Kết nối VPN và kiểm tra server Kafka HPCC

1. Mở OpenVPN GUI, connect với profile được cấp (vd: `h6-2025`).

2. Mở PowerShell và kiểm tra port 29090:

```powershell
Test-NetConnection 203.205.33.134 -Port 29090
```

Nếu `TcpTestSucceeded : True` là OK.

**Lưu ý:** trong bài này chỉ dùng broker `203.205.33.134:29090` vì các broker 29091/29092 đôi khi không truy cập được từ ngoài trường.

---

## 4. Tạo virtualenv & cài thư viện

Trong CMD hoặc PowerShell, cd tới thư mục lab:

```cmd
cd D:\Daihoc\Nam3\BIGDATA\LAB\lab01
```

### 4.1. Tạo virtualenv tên BigData311

```cmd
python -m venv BigData311
```

### 4.2. Kích hoạt virtualenv

```cmd
BigData311\Scripts\activate
```

Khi thành công sẽ thấy `(BigData311)` ở đầu dòng prompt.

### 4.3. Cài các thư viện cần thiết

```cmd
pip install -r requirements.txt
```

Trong đó có các package quan trọng:

- `pyspark==3.5.4` (Spark 3.5.4 kèm connector phù hợp)
- `confluent-kafka` (dùng cho script sinh/đẩy dữ liệu nếu cần)
- (các lib phụ khác…)

---

## 5. Bước 1 – Sinh dữ liệu giả (movies/ratings/tags)

Chạy trong thư mục `lab01`, virtualenv vẫn đang bật:

```cmd
(BigData311) D:\Daihoc\Nam3\BIGDATA\LAB\lab01> python lab01_gen_data.py
```

Script này:

- Sinh dữ liệu random nhưng hợp lý cho:
  - `input/movies.csv`
  - `input/ratings.csv`
  - `input/tags.csv`
- Mỗi file có header đúng định dạng.

Bạn có thể mở thử:

```
input/movies.csv
input/ratings.csv
input/tags.csv
```

để kiểm tra.

---

## 6. Bước 2 – Đưa dữ liệu lên Kafka server HPCC

Vẫn ở thư mục `lab01`, đảm bảo:

- Đã kết nối VPN.
- Kafka server `203.205.33.134:29090` ping được.

Chạy:

```cmd
(BigData311) D:\Daihoc\Nam3\BIGDATA\LAB\lab01> python lab01_produce_Gr6h50.py
```

Script này sẽ:

- Đọc 3 file CSV trong `input/`:
  - `movies.csv`, `ratings.csv`, `tags.csv`
- Convert từng dòng thành JSON.
- Produce lên Kafka server `203.205.33.134:29090` với 3 topic:
  - `Gr6h50_movies`
  - `Gr6h50_ratings`
  - `Gr6h50_tags`

Nếu chạy thành công, terminal sẽ log số record đã gửi mỗi topic.

### 6.1. Kiểm tra dữ liệu thực sự nằm trên Kafka

Mở một CMD khác và cd đến thư mục bin của Kafka:

```cmd
cd D:\Kafka\kafka_2.13-3.7.0\bin\windows
```

Liệt kê topic:

```cmd
kafka-topics.bat --bootstrap-server 203.205.33.134:29090 --list
```

Đảm bảo có:

- `Gr6h50_movies`
- `Gr6h50_ratings`
- `Gr6h50_tags`

Kiểm tra nhanh nội dung:

```cmd
:: 5 bản ghi đầu tiên của topic movies
kafka-console-consumer.bat ^
  --bootstrap-server 203.205.33.134:29090 ^
  --topic Gr6h50_movies ^
  --from-beginning ^
  --max-messages 5

:: 5 bản ghi đầu tiên của topic ratings
kafka-console-consumer.bat ^
  --bootstrap-server 203.205.33.134:29090 ^
  --topic Gr6h50_ratings ^
  --from-beginning ^
  --max-messages 5

:: 5 bản ghi đầu tiên của topic tags
kafka-console-consumer.bat ^
  --bootstrap-server 203.205.33.134:29090 ^
  --topic Gr6h50_tags ^
  --from-beginning ^
  --max-messages 5
```

Nếu thấy JSON giống kiểu:

```json
{"movieId": "1", "title": "Movie 1", "genres": "Comedy"}
{"userId": "36", "movieId": "22", "rating": "5", "timestamp": "1604026727"}
{"userId": "28", "movieId": "53", "tag": "must-see", "timestamp": "1603350234"}
```

là OK.

---

## 7. Bước 3 – Chạy Spark để trả lời Q1–Q4

Quay lại cửa sổ terminal đang bật virtualenv trong thư mục `lab01`:

```cmd
(BigData311) D:\Daihoc\Nam3\BIGDATA\LAB\lab01> python lab01_exercise.py
```

Script `lab01_exercise.py` thực hiện:

1. **Khởi tạo SparkSession** (pyspark 3.5.4) với Kafka connector:
   - `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4`
   - `org.apache.kafka:kafka-clients:3.4.1`

2. **Đọc 3 topic từ Kafka** (chỉ từ broker `203.205.33.134:29090`):
   - `Gr6h50_movies`
   - `Gr6h50_ratings`
   - `Gr6h50_tags`

3. **Parse JSON value → DataFrame** với schema:
   - **Movies:** (movieId, title, genres)
   - **Ratings:** (userId, movieId, rating, timestamp)
   - **Tags:** (userId, movieId, tag, timestamp)

4. **Trả lời các câu hỏi:**

### Q1 – Lấy sample từ Kafka và lưu CSV

**Hàm:** `save_q1_samples(movies_df, ratings_df, tags_df)`

**Output:**

```
output_exercise/q1_movies_sample/part-*.csv
output_exercise/q1_ratings_sample/part-*.csv
output_exercise/q1_tags_sample/part-*.csv
```

Mỗi file chứa ~20 dòng đầu tiên, dùng để chứng minh đã đọc/parse Kafka OK.

### Q2 – Top 5 phim có điểm trung bình cao nhất (rating_count > 10)

**Hàm:** `solve_q2_top5_movies(ratings_df, movies_df, min_count=10)`

**Logic:**

Tính:

```
rating_count = số lần movieId được rating
avg_rating   = trung bình rating của movieId đó
```

- Lọc các phim có `rating_count > 10` (do data giả nhỏ, thay vì 30).
- Join với bảng movies để lấy title.
- Sắp xếp theo `avg_rating` giảm dần, lấy 5 phim đầu.

**Output CSV:**

```
output_exercise/q2_top5_movies/part-*.csv
```

Với các cột:

```
movieId,title,rating_count,avg_rating
```

### Q3 – 5 tags tệ nhất (average rating thấp nhất)

**Hàm:** `solve_q3_worst_tags(ratings_df, tags_df)`

**Logic:**

- Join ratings + tags theo movieId.
- Group theo tag:
  ```
  rating_count = số rating gắn với tag đó (qua movieId).
  avg_rating = điểm trung bình của các rating này.
  ```
- Bỏ các tag có `rating_count` quá ít (ví dụ > 10).
- Chọn 5 tag có `avg_rating` thấp nhất.

**Output CSV:**

```
output_exercise/q3_worst_tags/part-*.csv
```

Các cột:

```
tag,rating_count,avg_rating
```

### Q4 – Phim thuộc các tags tệ ở Q3

**Hàm:**
`solve_q4_movies_for_worst_tags(ratings_df, tags_df, worst_tags_df, movies_df)`

**Logic:**

- Lấy list các tag tệ ở Q3.
- Lọc `tags_df` chỉ giữ các dòng có tag nằm trong list đó.
- Join với `ratings_df` theo movieId.
- Group theo (tag, movieId):
  ```
  rating_count = số rating của phim đó với tag đó.
  avg_rating = điểm trung bình rating.
  ```
- Join thêm với `movies_df` để lấy title.
- Sắp xếp theo tag, rồi `avg_rating` giảm dần.

**Output CSV:**

```
output_exercise/q4_movies_per_worst_tag/part-*.csv
```

Các cột:

```
tag,movieId,title,rating_count,avg_rating
```

---

## 8. Chạy lại / dọn dẹp

Mỗi lần chạy lại `lab01_exercise.py`, các thư mục dưới `output_exercise/` sẽ được ghi đè (`mode="overwrite"`), nên không cần xoá tay.

Nếu muốn reset lại dữ liệu Kafka:

- Có thể thay đổi seed trong `lab01_gen_data.py` để sinh data khác.
- Xoá topic cũ và produce lại (nếu được phép) – nhưng vì là Kafka server của HPCC nhiều nhóm dùng chung, không nên tự ý delete topic của người khác.
