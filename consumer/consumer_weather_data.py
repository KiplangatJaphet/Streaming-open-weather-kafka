import json
import psycopg2
from kafka import KafkaConsumer 

CONFLUENT_BOOTSTRAP ="pkc-619z3.us-east1.gcp.confluent.cloud:9092"  # yours
CONFLUENT_API_KEY ="GNCXNU26FBCZSH5Y"
CONFLUENT_API_SECRET = "cfltoyvgACIIrChamF7NI0ADK54oA3GvtXZoUw4nQEffxUrsYQXFg3hbDH9DacIw"

# PostgreSQL connection
conn = psycopg2.connect(
    host="172.24.67.244",
    port=5433,
    database="weather_db",
    user="postgres",
    password="12345"
	)
cursor = conn.cursor()


# Kafka Consumer
consumer = KafkaConsumer(
    "weather_data",
    bootstrap_servers=CONFLUENT_BOOTSTRAP,
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_plain_username=CONFLUENT_API_KEY,
    sasl_plain_password=CONFLUENT_API_SECRET,
    auto_offset_reset="earliest",
    api_version=(2, 0, 0),
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

print("Listening and saving to PostgreSQL...")

for message in consumer:
    data = message.value
    print(f"Received: {data}")
    cursor.execute("""
        INSERT INTO weather_data (city, temp, humidity, condition, timestamp)
        VALUES (%s, %s, %s, %s, %s)
    """, (data['city'], data['temp'], data['humidity'], data['condition'], data['timestamp']))
    conn.commit()
    print(f"Saved to DB: {data['city']} Temp={data['temp']}°C")
