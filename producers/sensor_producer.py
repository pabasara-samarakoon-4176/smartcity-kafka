import json
import time
import random
import os
from datetime import datetime, timezone
from kafka import KafkaProducer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "raw-traffic")
SENSORS = os.getenv("SENSORS", "junction-001,junction-002,junction-003,junction-004").split(",")
INTERVAL_SEC = float(os.getenv("INTERVAL_SEC", "1.0"))
CRITICAL_PROB = float(os.getenv("CRITICAL_PROB", "0.05"))
FREE_FLOW = float(os.getenv("FREE_FLOW", "40.0"))

def create_producer():
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )

def gen_reading(sensor_id):
    if random.random() < CRITICAL_PROB:
        avg_speed = random.uniform(3.0, 9.5)
    else:
        avg_speed = max(0.0, random.gauss(30, 8))
    vehicle_count = max(0, int(random.gauss(8, 3)))

    return {
        "sensor_id": sensor_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "vehicle_count": vehicle_count,
        "avg_speed": round(avg_speed, 2),
    }

def run():
    p = create_producer()
    print(f"Producing to {TOPIC} -> {KAFKA_BOOTSTRAP}. Sensors: {SENSORS}. Interval: {INTERVAL_SEC}s :: Critical Prob: {CRITICAL_PROB}")
    try:
        while True:
            for s in SENSORS:
                payload = gen_reading(s)
                p.send(TOPIC, key=s, value=payload)
            p.flush()
            time.sleep(INTERVAL_SEC)
    except KeyboardInterrupt:
        print("Shutting down producer.")
    finally:
        p.close()

if __name__ == "__main__":
    run()