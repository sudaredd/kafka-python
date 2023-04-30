import json
from kafka import KafkaConsumer
import logging
topic_name = "test"

consumer = KafkaConsumer(topic_name, bootstrap_servers=['localhost:9092'],
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')), group_id='demo',
                         auto_offset_reset='earliest')
for message in consumer:
    print(message)