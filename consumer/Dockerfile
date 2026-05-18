FROM python:3.12-slim
WORKDIR /app
COPY consumer_weather_data.py .
RUN pip install kafka-python psycopg2-binary
CMD ["python3", "consumer_weather_data.py"]
