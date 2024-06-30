#!/usr/bin/env python
import os
import time
from glob import glob

import pika
from tqdm import tqdm

host = os.getenv("RABBITMQ_HOST", "localhost")
port = os.getenv("RABBITMQ_PORT", 5672)

connection = pika.BlockingConnection(pika.ConnectionParameters(host=host, port=port))
channel = connection.channel()

files = glob(f"{os.path.dirname(__file__)}/../data/ztf_alerts/**.avro")

alerts = [open(file, "rb").read() for file in files]

# copy alerts (which is of length 313) * 1000 to have 300k alerts like we would in one night, just to see how quickly 2 works read that with rabbitmq

print(f"Nb alerts: {len(alerts)}")

channel.queue_declare(queue="alerts", durable=True)

start = time.time()

for a in tqdm(alerts):
    channel.basic_publish(
        exchange="",
        routing_key="alerts",
        body=a,
        properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent),
    )
print(f" [x] Sent {len(alerts)} alerts")
connection.close()
