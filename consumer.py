from kafka import KafkaConsumer
import logging

topic_name = "test"
import json

consumer = KafkaConsumer(topic_name, bootstrap_servers=['localhost:9092'],
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')), group_id='demo',
                         auto_offset_reset='earliest')
for message in consumer:
    msg = message.value
    msg_dict = json.loads(msg)
    print(msg)
    print("userId {}".format(msg_dict['user_id']))
    print("user name {}".format(msg_dict['user_name']))
