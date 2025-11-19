import csv
import json
from pathlib import Path
from confluent_kafka import Producer

# === ĐƯỜNG DẪN DATA ===
BASE = Path(r"D:\Daihoc\Nam3\BIGDATA\LAB\lab01\input")

# === KAFKA CLUSTER HPCC ===
BOOTSTRAP = (
    "203.205.33.134:29090,"
    "203.205.33.135:29091,"
    "203.205.33.136:29092"
)

# === TÊN TOPIC NHÓM Gr6h50 ===
TOPICS = {
    "movies":  "Gr6h50_movies",
    "ratings": "Gr6h50_ratings",
    "tags":    "Gr6h50_tags",
}

def make_producer():
    conf = {
        "bootstrap.servers": BOOTSTRAP,
        "client.id": "Gr6h50-producer",
        "linger.ms": 10,
    }
    return Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")

def produce_csv(producer, csv_path: Path, topic: str, key_field: str | None = None):
    print(f"\n=== Producing {csv_path.name} → {topic} ===")
    count = 0
    
    with csv_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            value_str = json.dumps(row)

            key_bytes = None
            if key_field and key_field in row:
                key_bytes = str(row[key_field]).encode("utf-8")

            producer.produce(
                topic,
                key=key_bytes,
                value=value_str.encode("utf-8"),
                callback=delivery_report,
            )
            count += 1

            # flush định kỳ tránh đầy buffer
            if count % 1000 == 0:
                producer.flush()

    producer.flush()
    print(f"✔ Sent {count} messages to {topic}")

def main():
    p = make_producer()

    produce_csv(p, BASE / "movies.csv",  TOPICS["movies"],  key_field="movieId")
    produce_csv(p, BASE / "ratings.csv", TOPICS["ratings"], key_field="movieId")
    produce_csv(p, BASE / "tags.csv",    TOPICS["tags"],    key_field="movieId")

    print("\n🎉 DONE: All Gr6h50 topics uploaded.")

if __name__ == "__main__":
    main()
