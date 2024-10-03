import os
import time
import random
from datetime import datetime
from confluent_kafka import Producer # type: ignore
import json
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

def delivery_report(err, msg):
    """ Callback for delivery reports from the producer """
    if err is not None:
        logging.error(f'Message delivery failed: {err}')
    else:
        logging.info(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def create_producer():
    conf = {
        'bootstrap.servers': os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    }
    producer = Producer(conf)
    return producer

producer = create_producer()

try:
    while True:
        # Generate a normal value
        value = random.uniform(40, 60)

        # Introduce an outlier with a 10% probability
        if random.random() < 0.1:
            value = random.uniform(80, 100)

        data = {
            'timestamp': datetime.utcnow().isoformat(),
            'value': value
        }

        # Serialize data to JSON string
        data_json = json.dumps(data)

        # Produce the message to Kafka
        producer.produce(
            topic='time_series',
            value=data_json,
            callback=delivery_report
        )

        # Poll for delivery reports
        producer.poll(0)

        logging.info(f'Produced: {data}')

        time.sleep(1)

except KeyboardInterrupt:
    pass
finally:
    # Wait for any outstanding messages to be delivered and delivery report callbacks to be triggered
    producer.flush()
