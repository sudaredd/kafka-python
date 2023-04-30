from json import dumps
from  time import  sleep
from kafka import KafkaProducer
from faker import Faker
import random

topic_name = "test"
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda x: dumps(x).encode('utf-8'))
fake = Faker()

for i in range (100):
    data = {
        'user_id': fake.random_int(min=20000, max=100000),
        'user_name': fake.name(),
        'user_address': fake.street_address() + ' | ' + fake.city() + ' | ' + fake.country_code(),
        'platform': random.choice(['Mobile', 'Laptop', 'Tablet']),
        'signup_at': str(fake.date_time_this_month())
    }
    msg = dumps(data)
    print (msg)
    producer.send(topic_name, value=msg)
    sleep(1)