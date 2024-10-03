import os
import time
import random
from datetime import datetime
from confluent_kafka import Producer  # type: ignore
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

# List of fields and operations
onshore_fields = ['Bab', 'Bu Hasa', 'Asab', 'Sahil', 'Shah']
offshore_fields = ['Upper Zakum', 'Lower Zakum', 'Umm Shaif', 'Nasr', 'Al Yasat']
specialized_fields = ['Shah Gas Field', 'Al Yasat Petroleum', 'Al Dhafra Petroleum']

# Producer instance
producer = create_producer()

def generate_data():
    # Randomly choose operation type and field
    operation_type = random.choice(['Onshore', 'Offshore', 'Specialized'])
    if operation_type == 'Onshore':
        field_name = random.choice(onshore_fields)
    elif operation_type == 'Offshore':
        field_name = random.choice(offshore_fields)
    else:
        field_name = random.choice(specialized_fields)

    data = {
        'timestamp': datetime.utcnow().isoformat(),
        'field_name': field_name,
        'operation_type': operation_type,
        'well_id': f"Well-{random.randint(1, 100)}",
        'pressure': random.uniform(30, 50),  # Normal pressure range (psi)
        'temperature': random.uniform(10, 40),  # Normal temperature range (°C)
        'flow_rate': random.uniform(500, 1500),  # Normal flow rate (m³/day)
        'vibration': random.uniform(0.1, 0.3),  # Normal vibration (mm/s)
        'equipment_status': random.choice(['operational', 'maintenance', 'failed'])
    }

    # Add onshore-specific fields
    if operation_type == 'Onshore':
        data.update({
            'reservoir_pressure': random.uniform(2000, 4000),  # psi
            'wellhead_temperature': random.uniform(50, 90),  # °C
            'oil_cut': random.uniform(70, 90),  # %
            'sand_production': random.uniform(0, 20),  # ppm
            'water_injection_rate': random.uniform(200, 500)  # m³/day
        })

    # Add offshore-specific fields
    elif operation_type == 'Offshore':
        data.update({
            'platform_id': f"Platform-{random.randint(1, 50)}",
            'sea_surface_temperature': random.uniform(20, 35),  # °C
            'wave_height': random.uniform(0.5, 5),  # m
            'pipeline_pressure': random.uniform(500, 2000),  # psi
            'gas_lift_rate': random.uniform(1000, 5000)  # m³/day
        })

    # Add specialized field-specific data
    elif operation_type == 'Specialized':
        data.update({
            'h2s_concentration': random.uniform(0, 5000),  # ppm
            'sulfur_recovery_efficiency': random.uniform(85, 99),  # %
            'condensate_yield': random.uniform(10, 50),  # bbl/mmscf
            'gas_production_rate': random.uniform(500, 1500)  # mmscf/day
        })

    # 10% chance to inject an outlier
    if random.random() < 0.1:
        outlier_type = random.choice(['pressure_spike', 'temperature_drop', 'flow_spike', 'vibration_spike'])
        if outlier_type == 'pressure_spike':
            data['pressure'] = random.uniform(80, 100)  # Outlier pressure
        elif outlier_type == 'temperature_drop':
            data['temperature'] = random.uniform(-10, 0)  # Unusual low temperature
        elif outlier_type == 'flow_spike':
            data['flow_rate'] = random.uniform(2000, 3000)  # Abnormal flow rate
        elif outlier_type == 'vibration_spike':
            data['vibration'] = random.uniform(1, 3)  # High vibration indicating equipment malfunction

    return data

try:
    while True:
        # Generate the data
        data = generate_data()

        # Serialize data to JSON string
        data_json = json.dumps(data)

        # Produce the message to Kafka
        producer.produce(
            topic='time_series',  # Ensure the topic name matches the one used by the consumer
            value=data_json,
            callback=delivery_report
        )

        # Poll for delivery reports
        producer.poll(0)

        logging.info(f'Produced: {data}')

        # Introduce a delay between data points to simulate real-time production
        time.sleep(1)

except KeyboardInterrupt:
    pass
finally:
    # Wait for any outstanding messages to be delivered and delivery report callbacks to be triggered
    producer.flush()