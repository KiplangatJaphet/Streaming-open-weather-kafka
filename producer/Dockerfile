FROM python:3.12-slim
WORKDIR /app
COPY producer_weather_data.py .
RUN pip install kafka-python requests
CMD ["python3", "producer_weather_data.py"]
