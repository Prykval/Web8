import faker
import json
from datetime import datetime

import pika

credentials = pika.PlainCredentials('guest', 'guest')
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', port=5672, credentials=credentials))
channel = connection.channel()

channel.exchange_declare(exchange='Web_hm8 exchange', exchange_type='direct')
channel.queue_declare(queue='web_hm8_queue', durable=True)
channel.queue_bind(exchange='Web_hm8 exchange', queue='web_hm8_queue')


fake = faker.Faker()

def create_tasks(nums: int):
    for i in range(nums):
        message = {
            'id': i,
            'name' : fake.name(),
            'email' : fake.email(),
            'payload': f"Date: {datetime.now().isoformat()}",
        }

        channel.basic_publish(exchange='Web_hm8 exchange', routing_key='web_hm8_queue', body=json.dumps(message).encode())

    connection.close()


if __name__ == '__main__':
    create_tasks(100)
