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
