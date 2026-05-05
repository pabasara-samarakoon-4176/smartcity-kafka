import json
import time
import random
import os
from datetime import datetime, timezone
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

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
    simulated_hour = datetime.now().minute % 24
    base_date = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    simulated_timestamp = base_date.replace(hour=simulated_hour)

    with open("producers/junctions.json", "r") as f:
        JUNCTION_PROFILES = json.load(f)

        profile = JUNCTION_PROFILES.get(sensor_id, {})

        if 7 <= simulated_hour <= 9:         # Morning rush
            traffic_factor = 2.2
        elif 12 <= simulated_hour <= 14:     # Midday
            traffic_factor = 1.5
        elif 17 <= simulated_hour <= 19:     # Evening rush
            traffic_factor = 2.5
        elif 22 <= simulated_hour or simulated_hour <= 5:
            traffic_factor = 0.5             # Night
        else:
            traffic_factor = 1.0

        vehicle_count = int(
            random.gauss(
                profile["base_volume"] * traffic_factor,
                profile["base_volume"] * 0.15
            )
        )

        vehicle_count = max(5, vehicle_count)

        if random.random() < CRITICAL_PROB:
            avg_speed = random.uniform(3.0, 9.5)
        else:
            avg_speed = random.gauss(
                profile["base_speed"] / traffic_factor,
                3
            )

        avg_speed = max(2.0, avg_speed)

        return {
            "sensor_id": sensor_id,
            "timestamp": simulated_timestamp.isoformat(),
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