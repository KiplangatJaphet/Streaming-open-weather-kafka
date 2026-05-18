🌤️ Streaming Open Weather Kafka
**A production-grade, end-to-end real-time data streaming pipeline that fetches live weather data from 13 cities across Kenya, East Africa, and the world. The pipeline streams data through Apache Kafka on Confluent Cloud, stores it in PostgreSQL, visualizes it in Grafana, orchestrates it with Apache Airflow, and is fully containerized with Docker.**
---


📖 Project Overview
**This project builds a real-time weather data streaming pipeline using modern data engineering tools. Weather data is fetched from the OpenWeatherMap API every 10 seconds and streamed through Confluent Cloud (managed Apache Kafka). A consumer reads the messages and stores them in a PostgreSQL database. The data is visualized in real-time using Grafana dashboards, orchestrated by Apache Airflow, and the entire pipeline is containerized using Docker.**
---
🏗️ Architecture
```
┌─────────────────────────────────────────────────────────────────┐
│                  WEATHER STREAMING PIPELINE                     │
│                                                                 │
│   OpenWeatherMap API                                            │
│          │                                                      │
│          ▼                                                      │
│   producer_weather_data.py                                      │
│   (Fetches 13 cities every 10 seconds)                          │
│          │                                                      │
│          ▼                                                      │
│   Confluent Cloud (Managed Apache Kafka)                        │
│   Topic: weather_data                                           │
│          │                                                      │
│          ▼                                                      │
│   consumer_weather_data.py                                      │
│   (Receives messages & saves to PostgreSQL)                     │
│          │                                                      │
│          ▼                                                      │
│   PostgreSQL Database                                           │
│   Database: weather_db                                          │
│   Table: weather_data                                           │
│          │                                                      │
│          ▼                                                      │
│   Grafana Dashboard                                             │
│   (Real-time visualizations)                                    │
│                                                                 │
│   Apache Airflow                                                │
│   (Orchestrates pipeline every 10 minutes)                      │
│                                                                 │
│   Docker                                                        │
│   (Containerizes the entire pipeline)                           │
└─────────────────────────────────────────────────────────────────┘
```
---
🛠️ Technologies Used
Technology	Purpose
Apache Kafka	Message streaming
Confluent Cloud	Managed Kafka broker
Python 3.12	Producer & Consumer
kafka-python	Kafka client library
PostgreSQL 18	Data storage
psycopg2	PostgreSQL Python adapter
Grafana	Real-time visualization
Apache Airflow	Pipeline orchestration
Docker	Containerization
Docker Compose	Multi-container management
OpenWeatherMap API	Weather data source
DBeaver	Database management
---
📁 Project Structure
```
Streaming-open-weather-kafka/
├── producer/
│   ├── Dockerfile
│   └── producer_weather_data.py
├── consumer/
│   ├── Dockerfile
│   └── consumer_weather_data.py
├── dags/
│   └── weather_pipeline_dag.py
├── images/
│   ├── kafka_streaming.png
│   ├── kafka_topic.png
│   ├── grafana_dashboard.png
│   ├── grafana_temperature.png
│   ├── grafana_humidity.png
│   ├── airflow_dag.png
│   └── airflow_tasks.png
├── docker-compose.yml
├── .gitignore
└── README.md
```
---
✅ Prerequisites
Before running this project, make sure you have:
Python 3.12+
Confluent Cloud account — free tier at confluent.io
OpenWeatherMap API key — free at openweathermap.org
PostgreSQL 18
Grafana
Apache Airflow
Docker & Docker Compose
---
⚙️ Installation & Setup
1. Clone the repository
```bash
git clone https://github.com/KiplangatJaphet/Streaming-open-weather-kafka.git
cd Streaming-open-weather-kafka
```
2. Create and activate virtual environment
```bash
python3 -m venv kafkaenv
source kafkaenv/bin/activate
```
3. Install dependencies
```bash
pip install kafka-python requests psycopg2-binary apache-airflow
```
4. Set up Confluent Cloud
Create a free account at confluent.io
Create a cluster (Basic tier)
Create a topic named `weather_data`
Generate API Keys and save your Key and Secret
Copy your Bootstrap server URL
5. Set up PostgreSQL
```bash
sudo pg_ctlcluster 18 main start
sudo -u postgres psql -p 5433
```
```sql
CREATE DATABASE weather_db;
\c weather_db
CREATE TABLE weather_data (
    id SERIAL PRIMARY KEY,
    city VARCHAR(100),
    temp FLOAT,
    humidity INT,
    condition VARCHAR(100),
    timestamp BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
\q
```
6. Configure credentials
Update `producer/producer_weather_data.py` and `consumer/consumer_weather_data.py` with your credentials:
```python
# OpenWeatherMap
WEATHER_API_KEY = "your_openweathermap_api_key"

# Confluent Cloud
CONFLUENT_BOOTSTRAP = "your_bootstrap_server"
CONFLUENT_API_KEY = "your_confluent_api_key"
CONFLUENT_API_SECRET = "your_confluent_api_secret"

# PostgreSQL (consumer only)
conn = psycopg2.connect(
    host="localhost",
    port=5433,
    database="weather_db",
    user="postgres",
    password="your_password"
)
```
---
▶️ Running the Pipeline
Start PostgreSQL
```bash
sudo pg_ctlcluster 18 main start
```
Terminal 1 — Run Producer
```bash
cd producer
source ../kafkaenv/bin/activate
python3 producer_weather_data.py
```
Terminal 2 — Run Consumer
```bash
cd consumer
source ../kafkaenv/bin/activate
python3 consumer_weather_data.py
```
Terminal 3 — Start Airflow
```bash
source kafkaenv/bin/activate
airflow standalone
```
Start Grafana
```bash
sudo service grafana-server start
```
Start Docker
```bash
sudo service docker start
docker compose up -d
```
---
📡 Kafka Streaming
The producer fetches weather data every 10 seconds and sends it to Confluent Cloud Kafka topic `weather_data`. The consumer listens to the topic and saves each message to PostgreSQL in real time.
Producer Output
![Kafka Producer Streaming](images/kafka_streaming.png)
<img width="1137" height="668" alt="image" src="https://github.com/user-attachments/assets/966a6a13-9e08-4710-8390-eac774ab2871" />

Confluent Cloud Topic
![Confluent Cloud Topic](images/kafka_topic.png)
<img width="1350" height="572" alt="image" src="https://github.com/user-attachments/assets/bee20762-4939-41e0-bf56-75928ee88f50" />

---
📊 Grafana Dashboard
Access Grafana at `http://localhost:3000`
Default credentials:
```
Username: admin
Password: admin
```
Full Dashboard Overview
![Grafana Dashboard](images/grafana_dashboard.png)
<img width="1311" height="666" alt="image" src="https://github.com/user-attachments/assets/f49f6429-380a-4d6f-b5c7-3b628765d945" />
<img width="1314" height="663" alt="image" src="https://github.com/user-attachments/assets/bc10b482-09d8-44f6-b97d-89922ab02487" />

Dashboard Panels
Panel	Type	Description
Temperature by City	Time series	Live temperature trends
Humidity by City	Time series	Live humidity trends
Current Weather by City	Table	Latest weather per city
Hottest Cities Right Now	Bar chart	Top hot cities
Weather Conditions Distribution	Pie chart	Clouds/Rain/Clear breakdown
Average Temperature per City	Bar chart	Average temps comparison
Total Weather Records	Stat	Total records collected
Coldest Cities Right Now	Bar chart	Top cold cities
Sample Queries
Temperature by City:
```sql
SELECT created_at AS time, city, temp
FROM weather_data
ORDER BY created_at ASC;
```
Hottest Cities:
```sql
SELECT city, MAX(temp) as temp
FROM weather_data
WHERE created_at >= NOW() - INTERVAL '1 hour'
GROUP BY city
ORDER BY temp DESC;
```
Weather Conditions Distribution:
```sql
SELECT condition, COUNT(*) as count
FROM weather_data
GROUP BY condition
ORDER BY count DESC;
```
Average Temperature per City:
```sql
SELECT city, ROUND(AVG(temp)::numeric, 2) as avg_temp
FROM weather_data
GROUP BY city
ORDER BY avg_temp DESC;
```
Total Records:
```sql
SELECT COUNT(*) as value
FROM weather_data;
```
Coldest Cities:
```sql
SELECT city, MIN(temp) as temp
FROM weather_data
WHERE created_at >= NOW() - INTERVAL '1 hour'
GROUP BY city
ORDER BY temp ASC;
```
---
🔄 Apache Airflow
Access Airflow at `http://localhost:8080`
DAG Overview
![Airflow Tasks](images/airflow_tasks.png)
<img width="1365" height="614" alt="image" src="https://github.com/user-attachments/assets/e9addc90-b8ae-47d1-8495-cdfae244bf50" />

DAG: weather_streaming_pipeline
Schedule: Every 10 minutes (`*/10 * * * *`)
Owner: kiplangat
Tags: confluent, kafka, weather
Tasks:
`start_producer` — Starts the weather data producer
`start_consumer` — Starts the Kafka consumer

---
🐳 Docker Deployment
Build and run all containers
```bash
docker compose up --build -d
```
Docker services
Container	Image	Port	Purpose
weather_producer	Custom	-	Fetches & sends weather data
weather_consumer	Custom	-	Receives & saves to PostgreSQL
weather_postgres	postgres:15	5432	PostgreSQL database
weather_grafana	grafana/grafana	3001	Grafana dashboard
Check running containers
```bash
docker ps
```
Check logs
```bash
docker logs weather_producer
docker logs weather_consumer
docker logs weather_postgres
docker logs weather_grafana
```
Stop all containers
```bash
docker compose down
```
Access Docker Grafana
```
http://localhost:3001
Username: admin
Password: admin123
```
---
🌍 Cities Tracked
Region	Cities
Kenya	Nairobi, Nakuru, Eldoret, Mombasa, Kisumu
East Africa	Dodoma, Kampala, Kigali, Dar es Salaam
World	London, New York, Tokyo, Dubai
---
🗄️ Database Schema
```sql
CREATE TABLE weather_data (
    id          SERIAL PRIMARY KEY,
    city        VARCHAR(100),
    temp        FLOAT,
    humidity    INT,
    condition   VARCHAR(100),
    timestamp   BIGINT,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
**```
Column	Type	Description
id	SERIAL	Auto-increment primary key
city	VARCHAR	City name
temp	FLOAT	Temperature in °C
humidity	INT	Humidity percentage
condition	VARCHAR	Weather condition (Rain/Clouds/Clear)
timestamp	BIGINT	Unix timestamp from API
created_at	TIMESTAMP	Record creation time**
---

