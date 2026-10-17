import json, time, random, uuid, argparse
from datetime import datetime, timedelta, timezone
from kafka import KafkaProducer
from faker import Faker

ap = argparse.ArgumentParser()
ap.add_argument("--rate", type=int, default=20, help="events per second")
args = ap.parse_args()

fake = Faker()
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    key_serializer=lambda k: str(k).encode(),
    value_serializer=lambda v: v if isinstance(v, bytes) else json.dumps(v).encode(),
    acks="all",
    linger_ms=50,
)

def make_event():
    late = timedelta(minutes=random.randint(1, 8)) if random.random() < 0.05 else timedelta(0)
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": random.choices(["page_view", "add_to_cart", "purchase"], [70, 20, 10])[0],
        "event_time": (datetime.now(timezone.utc) - late).isoformat(),
        "user_id": random.randint(1, 5000),
        "product_id": random.randint(1, 500),
        "price": round(random.uniform(5, 500), 2),
        "qty": random.randint(1, 3),
        "session_id": fake.uuid4(),
        "device": random.choice(["ios", "android", "web"]),
    }

sent = 0
try:
    while True:
        ev = make_event()
        producer.send("ecom.events", key=ev["user_id"], value=ev)
        if random.random() < 0.02:                       # ~2% duplicates
            producer.send("ecom.events", key=ev["user_id"], value=ev)
        if random.random() < 0.005:                      # ~0.5% garbage
            producer.send("ecom.events", key="bad", value=b'{"event_id": "oops", "price": ')
        sent += 1
        if sent % 100 == 0:
            print(f"sent {sent} events", flush=True)
        time.sleep(1 / args.rate)
except KeyboardInterrupt:
    producer.flush()
    print(f"stopped after {sent} events")