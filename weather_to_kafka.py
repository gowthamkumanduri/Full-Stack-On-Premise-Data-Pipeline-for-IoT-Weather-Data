import requests, json
from kafka import KafkaProducer
from datetime import datetime

API_KEY = '458052ea63a452bdb80efbd4817dac9c'
CITY = 'Vijayawada'
URL = f"http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}"

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

response = requests.get(URL).json()
response['ingest_time'] = datetime.utcnow().isoformat()  # Add unique timestamp
producer.send('weather-topic', response)
producer.flush()
