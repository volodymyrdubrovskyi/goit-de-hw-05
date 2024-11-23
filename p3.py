import json
import time

from copy import deepcopy
from confluent_kafka import Consumer, Producer
from configs import kafka_config


# Створення споживача (Consumer) Kafka
consumer_kafka_config = deepcopy(kafka_config)
consumer_kafka_config["group.id"] = "sensor-group"
consumer_kafka_config["auto.offset.reset"] = "earliest"

consumer = Consumer(consumer_kafka_config)
consumer.subscribe(['vvd_building_sensors'])

# Створення продюсера Kafka 
producer = Producer(kafka_config)

# Функція зворотнього виклику для підтвердження доставки
def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for message {msg.key()}: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Обробка повідомлень
try:
    while True:
        msg = consumer.poll(1.0)  # Очікування повідомлення протягом 1 секунди
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        data = json.loads(msg.value().decode('utf-8'))
        sensor_id = data['sensor_id']
        timestamp = data['timestamp']
        temperature = data['temperature']
        humidity = data['humidity']

        # Генерація сповіщень
        if temperature > 40:
            alert = {
                "sensor_id": sensor_id,
                "timestamp": timestamp,
                "temperature": temperature,
                "message": "Temperature exceeds threshold"
            }
            producer.produce('vvd_temperature_alerts', value=json.dumps(alert).encode('utf-8'), callback=delivery_report)
            producer.poll(1)

        if humidity > 80 or humidity < 20:
            alert = {
                "sensor_id": sensor_id,
                "timestamp": timestamp,
                "humidity": humidity,
                "message": "Humidity out of range"
            }
            producer.produce('vvd_humidity_alerts', value=json.dumps(alert).encode('utf-8'), callback=delivery_report)
            producer.poll(1)

        print(f"Processed data: {data}")
except KeyboardInterrupt:
    print("Stopped by user")
finally:
    consumer.close()
    producer.flush()  # Очікування завершення відправлення всіх повідомлень
