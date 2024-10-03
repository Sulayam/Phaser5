import os
import json
from confluent_kafka import Consumer, KafkaError
from threading import Thread
import queue
import pandas as pd
import logging
import threading

import dash
from dash.dependencies import Output, Input
from dash import dcc, html
import dash_bootstrap_components as dbc
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

# Initialize Dash app with a Bootstrap theme
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.FLATLY])

app.layout = dbc.Container(
    [
        dbc.Row(
            dbc.Col(
                html.H1("Real-Time Outlier Detection", className="text-center text-primary mb-4"),
                width=12
            )
        ),
        dbc.Row(
            dbc.Col(
                dcc.Graph(id='live-graph', animate=False),
                width=12
            )
        ),
        dcc.Interval(
            id='graph-update',
            interval=1*1000,  # Update every second
            n_intervals=0
        )
    ],
    fluid=True
)

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
    data_traces = [
        go.Scatter(
            x=timestamps,
            y=values,
            mode='lines',
            name='Data',
            line=dict(color='#007bff', width=2)  # Bootstrap primary color
        ),
        go.Scatter(
            x=outlier_timestamps,
            y=outlier_values,
            mode='markers',
            marker=dict(color='red', size=8, symbol='circle'),
            name='Outliers'
        )
    ]

    xaxis_range = [timestamps[0], timestamps[-1]] if timestamps else [None, None]
    y_min = min(values + outlier_values) if values or outlier_values else None
    y_max = max(values + outlier_values) if values or outlier_values else None
    yaxis_range = [y_min - 1, y_max + 1] if y_min is not None and y_max is not None else [None, None]

    layout = go.Layout(
        xaxis=dict(
            title='Time',
            showgrid=True,
            zeroline=False,
            showticklabels=True,
            automargin=True,
            linecolor='black',
            linewidth=1,
            range=xaxis_range
        ),
        yaxis=dict(
            title='Value',
            showgrid=True,
            zeroline=False,
            showticklabels=True,
            automargin=True,
            linecolor='black',
            linewidth=1,
            range=yaxis_range
        ),
        margin=dict(l=40, r=40, t=50, b=50),
        showlegend=True,
        legend=dict(
            x=0,
            y=1.0,
            bgcolor='rgba(255, 255, 255, 0)',
            bordercolor='rgba(255, 255, 255, 0)'
        ),
        plot_bgcolor='white',
        paper_bgcolor='white',
        title='Real-Time Data Stream with Outliers'
    )

    return {'data': data_traces, 'layout': layout}

if __name__ == '__main__':
    # Start Kafka consumer thread inside the main block
    consumer_thread = threading.Thread(target=consume_kafka_data, daemon=True)
    consumer_thread.start()
    app.run_server(debug=True, use_reloader=False)