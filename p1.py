from confluent_kafka.admin import AdminClient, NewTopic
from configs import kafka_config

admin_client = AdminClient(kafka_config)

# Перевірка конекту
topics = admin_client.list_topics().topics
print(f"Connected to Kafka. \nAvailable topics #: {len(topics)}")

# Визначення нових топіків 
topics = [ 
        NewTopic("vvd_building_sensors", num_partitions=1, replication_factor=1), 
        NewTopic("vvd_temperature_alerts", num_partitions=1, replication_factor=1), 
        NewTopic("vvd_humidity_alerts", num_partitions=1, replication_factor=1) 
        ] 

# Створення нових топіків 
try: 
    admin_client.create_topics(topics, validate_only=False) 
    print("Топіки створені успішно:") 
    for topic in topics: 
        print(f"- {topic.topic}") 
except Exception as e: 
    print(f"An error occurred: {e}") 
    

print("Перевірка створення топиків:")
metadata = admin_client.list_topics(timeout=10)
topics = metadata.topics

[print(topic) for topic in topics if "vvd_" in topic]