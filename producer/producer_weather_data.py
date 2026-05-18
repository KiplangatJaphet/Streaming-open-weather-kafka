import requests 
import time
from kafka import KafkaProducer 
import json 

API_KEY='4d2021edf0149607122ad089dec4fcd8' 
CITIES = [
    # Kenya
    "Nairobi", "Nakuru", "Eldoret", "Mombasa", "Kisumu",
    # East Africa
    "Dodoma", "Kampala", "Kigali", "Dar es Salaam",
    # World
    "London", "New York", "Tokyo", "Dubai"
]

CONFLUENT_BOOTSTRAP ="pkc-619z3.us-east1.gcp.confluent.cloud:9092"  # yours
CONFLUENT_API_KEY ="GNCXNU26FBCZSH5Y"
CONFLUENT_API_SECRET = "cfltoyvgACIIrChamF7NI0ADK54oA3GvtXZoUw4nQEffxUrsYQXFg3hbDH9DacIw"

producer = KafkaProducer(
    bootstrap_servers=CONFLUENT_BOOTSTRAP,
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_plain_username=CONFLUENT_API_KEY,
    sasl_plain_password=CONFLUENT_API_SECRET,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

topic = "weather_data"

while True: 
    for city in CITIES:
        try:
            url=f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
            response = requests.get(url)
            data = response.json()
            payload = {
                "city": city,
                "temp": data["main"]["temp"],
                "humidity": data["main"]["humidity"],
                "condition": data["weather"][0]["main"],
                "timestamp": data["dt"]
       	    }
            producer.send(topic, value = payload) 
            print(f"sent: {payload}")
        except Exception as e:
            print("Error!: ", e)


        time.sleep(10)
    

