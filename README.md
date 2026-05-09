## Smart City Kafka Streaming System

A real-time Smart City data streaming platform built using **Apache Kafka**, designed to simulate and process live city-related events such as traffic monitoring, environmental sensing, transportation updates, and IoT device communication.

###  Project Overview

This project demonstrates how Apache Kafka can be used to build a scalable real-time event-driven architecture for smart city applications.

The system enables:

- Real-time data production and consumption
- Scalable event streaming
- Distributed messaging between services
- IoT and sensor data simulation
- Efficient processing of city-related events

###  Technologies Used

- Apache Kafka
- Zookeeper
- Java / Spring Boot
- Docker
- Maven / Gradle
- REST APIs
- Kafka Producer & Consumer APIs

###  Project Structure
```
smartcity-kafka/
├── producers/
│   ├── sensor_producer.py            # Python Kafka producer (16 junctions)
│   ├── junctions.json                # Junction profiles (base_volume, base_speed)
│   ├── requirements.txt              # Python dependencies
│   └── .env                          # Producer configuration
├── spark_app/
│   ├── traffic_stream_processor.py   # Spark Structured Streaming job
│   ├── batch_report.py               # Nightly batch report Spark job
│   └── test_submit.py                # Spark connection test
├── dags/
│   └── traffic_batch_dag.py          # Airflow DAG definition
├── data/
│   ├── traffic_aggregates/           # Parquet output (auto-created)
│   └── traffic_reports/              # CSV report output (auto-created)
├── Dockerfile                        # Custom Airflow image with Java + Spark
└── docker-compose.yml                # Full stack definition
```
### Features

- Kafka Producer for publishing smart city events
- Kafka Consumer for processing live data streams
- Topic-based message handling
- Real-time event simulation
- Fault-tolerant messaging architecture
- Scalable distributed system design

### Example Use Cases

- Traffic monitoring systems
- Smart parking management
- Environmental sensor monitoring
- Emergency alert systems
- Public transportation tracking
- IoT device communication

### How to run

1. **Navigate to project**

```
git clone https://github.com/pabasara-samarakoon-4176/smartcity-kafka.git
cd smartcity-kafka
```

2. **Start Infrastructure**

Build the custom airflow image:
```
docker buildx build \
--platform linux/arm64 \
-t smartcity-airflow:2.8.2-custom \
. \
--load
```

Start all containers:
```
docker compose up -d
```

Check containers:
```
docker ps
```

Expected containers:

zookeeper
kafka
kafdrop
spark-master
spark-worker-1
airflow-postgres
airflow-webserver
airflow-scheduler

Note: Docker deamon engine should be running. Open Docker Desktop in MacOS.

3. **Create Kafka Topics**

Enter Kafka container:
```
docker exec -it kafka bash
```

Create raw traffic topic:
```
kafka-topics \
--bootstrap-server localhost:9092 \
--create \
--topic raw-traffic \
--partitions 4 \
--replication-factor 1
```

Create critical alerts topic:
```
kafka-topics \
--bootstrap-server localhost:9092 \
--create \
--topic critical-traffic \
--partitions 4 \
--replication-factor 1
```

Verify:
```
kafka-topics \
--bootstrap-server localhost:9092 \
--list
```

Exit:
```
exit
```

4. **Verify Spark Cluster**

Open Spark UI:

```
http://localhost:8081
```

Confirm:

Master running
Worker registered
CPU resources available

5. **Start Spark Streaming Job**

Enter Spark master:
```
docker exec -it spark-master bash
```

Run stream processor:
```
/opt/spark/bin/spark-submit \
--master spark://spark-master:7077 \
/opt/spark/app/traffic_stream_processor.py
```

Keep this terminal running.

6. **Start Sensor Producer**

Open another terminal.

Run producer:
```
python producers/sensor_producer.py
```

Producer sends live traffic events to Kafka.

7. **Verify Streaming**

Observe data streaming in both kafka topics:

```
docker exec -it kafka bash
kafka-console-consumer --bootstrap-server localhost:9092 --topic raw-traffic --from-beginning
```

In another terminal:
```
docker exec -it kafka bash
kafka-console-consumer --bootstrap-server localhost:9092 --topic critical-traffic --from-beginning
```

Verify:

raw-traffic contains events
critical-traffic receives congestion alerts

8. **Verify Parquet Output**

Check generated aggregates:
```
docker exec -it spark-master bash
ls -R /data/traffic_aggregates
```

Expected:
```
traffic_date=YYYY-MM-DD/
```

9. **Initialize Airflow**

Open Airflow UI:
```http://localhost:8088```

Login to airflow.

Navigate:
```
Admin → Connections
```

Click + Add Connection

Use the following values:
| Field           | Value                  |
| --------------- | ---------------------- |
| Connection Id   | `spark_default`        |
| Connection Type | `Spark`                |
| Host            | `spark://spark-master` |
| Port            | `7077`                 |

Save.

Verify it appears as: `spark_default`

10. **Run Batch Job**

Navigate to DAG tab.
Verify DAG `daily_traffic_batch_report`.
Trigger DAG manually.

Check report:
```
docker exec -it spark-master bash
ls -R /data/traffic_reports
```

Expected CSV report:
```
sensor_id,peak_hour,hourly_volume,...
```

11. Run Dashboard

Start dashboard:
```
streamlit run dashboard/traffic_dashboard.py
```

Dashboard shows:

Live traffic conditions
Congestion metrics
Critical alerts
Daily intervention report

Note: If stream state gets corrupted,
```
rm -rf ./data/traffic_aggregates/*
rm -rf ./data/traffic_reports/*
```

Then restart spark containers:
```
docker compose restart spark-master spark-worker-1
```
