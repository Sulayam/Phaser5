import os
import json
from confluent_kafka import Consumer, KafkaError
from threading import Thread
import queue
import pandas as pd
import time
import logging
import threading

import dash
from dash.dependencies import Output, Input
from dash import dcc, html
import plotly.graph_objs as go

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

# Kafka Consumer Configuration
conf = {
    'bootstrap.servers': os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
    'group.id': 'outlier_detector_group',
    'auto.offset.reset': 'latest'  # Start from latest messages
}

# Initialize data storage
data_queue = queue.Queue()

# Thread-safe data storage
data_lock = threading.Lock()
timestamps = []
values = []
outlier_timestamps = []
outlier_values = []
WINDOW_SIZE = 20
THRESHOLD = 2
data_window_for_detection = []

# Initialize Dash app
app = dash.Dash(__name__)

app.layout = html.Div([
    html.H1("Real-Time Outlier Detection"),
    dcc.Graph(id='live-graph', animate=False),
    dcc.Interval(
        id='graph-update',
        interval=1*1000,  # Update every second
        n_intervals=0
    )
])

def consume_kafka_data():
    consumer = Consumer(conf)
    consumer.subscribe(['time_series'])
    logging.info("Kafka Consumer started")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logging.error(f"Consumer error: {msg.error()}")
                    continue
            data = json.loads(msg.value().decode('utf-8'))
            data_point = data['value']
            timestamp = pd.Timestamp.now()
            with data_lock:
                data_queue.put({'timestamp': timestamp, 'value': data_point})
    except Exception as e:
        logging.error(f"Error in Kafka consumer: {e}")
    finally:
        consumer.close()

@app.callback(Output('live-graph', 'figure'),
              [Input('graph-update', 'n_intervals')])
def update_graph_scatter(n):
    global timestamps, values, outlier_timestamps, outlier_values, data_window_for_detection

    with data_lock:
        # Fetch data from the queue
        while not data_queue.empty():
            data = data_queue.get()
            timestamp = data['timestamp']
            value = data['value']
            timestamps.append(timestamp)
            values.append(value)
            data_window_for_detection.append(value)

            # Outlier detection
            if len(data_window_for_detection) >= WINDOW_SIZE:
                window = data_window_for_detection[-WINDOW_SIZE:]
                avg = sum(window) / len(window)
                std_dev = (sum((x - avg) ** 2 for x in window) / (len(window) - 1)) ** 0.5
                if abs(value - avg) > THRESHOLD * std_dev:
                    logging.info(f"Outlier detected: {value}")
                    outlier_timestamps.append(timestamp)
                    outlier_values.append(value)

    # Limit data length to prevent memory issues
    max_length = 1000
    timestamps = timestamps[-max_length:]
    values = values[-max_length:]
    outlier_timestamps = outlier_timestamps[-max_length:]
    outlier_values = outlier_values[-max_length:]

    # Create the figure
    data = [
        go.Scatter(
            x=timestamps,
            y=values,
            mode='lines+markers',
            name='Data',
            line=dict(color='blue')
        ),
        go.Scatter(
            x=outlier_timestamps,
            y=outlier_values,
            mode='markers',
            marker=dict(color='red', size=10, symbol='x'),
            name='Outliers'
        )
    ]

    layout = go.Layout(
        xaxis=dict(
            title='Time',
            range=[min(timestamps) if timestamps else None, max(timestamps) if timestamps else None],
            showgrid=True
        ),
        yaxis=dict(
            title='Value',
            range=[min(values) - 1 if values else None, max(values) + 1 if values else None],
            showgrid=True
        ),
        title='Real-Time Data Stream with Outliers',
        legend=dict(x=0, y=1.0),
        margin=dict(l=40, r=0, t=40, b=30)
    )

    return {'data': data, 'layout': layout}

if __name__ == '__main__':
    # Start Kafka consumer thread inside the main block
    consumer_thread = threading.Thread(target=consume_kafka_data, daemon=True)
    consumer_thread.start()
    app.run_server(debug=True, use_reloader=False)
