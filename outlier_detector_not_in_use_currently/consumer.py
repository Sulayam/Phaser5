import os
import json
from confluent_kafka import Consumer, KafkaError # type: ignore
from statistics import mean, stdev
from collections import deque
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

WINDOW_SIZE = 20
THRESHOLD = 2

conf = {
    'bootstrap.servers': os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
    'group.id': 'outlier_detector_group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe(['time_series'])

data_window = deque(maxlen=WINDOW_SIZE)

logging.info("Starting to consume messages...")

try:
    while True:
        msg = consumer.poll(1.0)  # Timeout of 1 second

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                continue
            else:
                logging.error(f"Consumer error: {msg.error()}")
                continue

        # Deserialize the message value from JSON
        data = json.loads(msg.value().decode('utf-8'))
        data_point = data['value']
        data_window.append(data_point)

        if len(data_window) == WINDOW_SIZE:
            avg = mean(data_window)
            std_dev = stdev(data_window)
            if abs(data_point - avg) > THRESHOLD * std_dev:
                logging.info(f"Outlier detected: {data_point}")
            else:
                logging.info(f"Data: {data_point}")
        else:
            logging.info(f"Data: {data_point}")

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
