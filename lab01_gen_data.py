import csv
import random
from pathlib import Path

BASE = Path(r"D:\Daihoc\Nam3\BIGDATA\LAB\lab01\input")
BASE.mkdir(parents=True, exist_ok=True)

N_MOVIES = 100
N_USERS = 50
N_RATINGS = 1000
N_TAGS = 300

def gen_movies():
    path = BASE / "movies.csv"
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["movieId", "title", "genres"])
        for mid in range(1, N_MOVIES + 1):
            title = f"Movie {mid}"
            genres = random.choice(
                ["Action|Adventure", "Drama", "Comedy", "Sci-Fi", "Romance"]
            )
            writer.writerow([mid, title, genres])
    print("✔ movies.csv:", path)

def gen_ratings():
    path = BASE / "ratings.csv"
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["userId", "movieId", "rating", "timestamp"])
        for _ in range(N_RATINGS):
            uid = random.randint(1, N_USERS)
            mid = random.randint(1, N_MOVIES)
            rating = random.choice([0.5, 1, 1.5, 2, 2.5, 3, 3.5, 4, 4.5, 5])
            ts = random.randint(1_600_000_000, 1_700_000_000)
            writer.writerow([uid, mid, rating, ts])
    print("✔ ratings.csv:", path)

def gen_tags():
    tags_pool = ["funny", "boring", "classic", "underrated", "overrated",
                 "must-see", "slow", "violent", "family", "confusing"]
    path = BASE / "tags.csv"
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["userId", "movieId", "tag", "timestamp"])
        for _ in range(N_TAGS):
            uid = random.randint(1, N_USERS)
            mid = random.randint(1, N_MOVIES)
            tag = random.choice(tags_pool)
            ts = random.randint(1_600_000_000, 1_700_000_000)
            writer.writerow([uid, mid, tag, ts])
    print("✔ tags.csv:", path)

if __name__ == "__main__":
    gen_movies()
    gen_ratings()
    gen_tags()
    print("✅ Done generating synthetic data.")
