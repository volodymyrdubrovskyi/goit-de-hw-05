import json

from confluent_kafka import Consumer
from configs import kafka_config
from copy import deepcopy


consumer_kafka_config = deepcopy(kafka_config)
consumer_kafka_config["group.id"] = "sensor-group"
consumer_kafka_config["auto.offset.reset"] = "earliest"

# Створення споживача (Consumer) Kafka для температурних сповіщень
temperature_consumer = Consumer(consumer_kafka_config)
temperature_consumer.subscribe(['vvd_temperature_alerts'])

# Створення споживача (Consumer) Kafka для сповіщень про вологість
humidity_consumer = Consumer(consumer_kafka_config)
humidity_consumer.subscribe(['vvd_humidity_alerts'])

try:
    while True:
        # Читання сповіщень про температуру
        temp_msg = temperature_consumer.poll(1.0)
        if temp_msg is not None:
            if temp_msg.error():
                print(f"Temperature consumer error: {temp_msg.error()}")
            else:
                temp_alert = json.loads(temp_msg.value().decode('utf-8'))
                print(f"Temperature Alert: {temp_alert}")

        # Читання сповіщень про вологість
        humid_msg = humidity_consumer.poll(1.0)
        if humid_msg is not None:
            if humid_msg.error():
                print(f"Humidity consumer error: {humid_msg.error()}")
            else:
                humid_alert = json.loads(humid_msg.value().decode('utf-8'))
                print(f"Humidity Alert: {humid_alert}")

except KeyboardInterrupt:
    print("Stopped by user")
finally:
    temperature_consumer.close()
    humidity_consumer.close()
