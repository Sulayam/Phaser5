import os
import json
from confluent_kafka import Consumer, KafkaError
import threading
import queue
import pandas as pd
import logging

import dash
from dash.dependencies import Output, Input
from dash import dcc, html
import dash_bootstrap_components as dbc
import plotly.graph_objs as go
from sklearn.ensemble import IsolationForest  # ML model for anomaly detection
import numpy as np

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

# Data structures for each operation type
operation_types = ['Onshore', 'Offshore', 'Specialized']

data_store = {
    'Onshore': {
        'timestamps': [],
        'values': [],
        'outlier_timestamps': [],
        'outlier_values': [],
        'data_window': [],
        'model': IsolationForest(n_estimators=50, contamination=0.1, random_state=42),
        'model_ready': False,
        'field_name': 'reservoir_pressure'  # High-risk field for Onshore
    },
    'Offshore': {
        'timestamps': [],
        'values': [],
        'outlier_timestamps': [],
        'outlier_values': [],
        'data_window': [],
        'model': IsolationForest(n_estimators=50, contamination=0.1, random_state=42),
        'model_ready': False,
        'field_name': 'pipeline_pressure'  # High-risk field for Offshore
    },
    'Specialized': {
        'timestamps': [],
        'values': [],
        'outlier_timestamps': [],
        'outlier_values': [],
        'data_window': [],
        'model': IsolationForest(n_estimators=50, contamination=0.1, random_state=42),
        'model_ready': False,
        'field_name': 'h2s_concentration'  # High-risk field for Specialized
    }
}

def update_model(operation_type):
    """
    Update the ML model with the latest data window for the given operation type.
    """
    data_entry = data_store[operation_type]
    data_window = data_entry['data_window']
    model = data_entry['model']

    # Reshape the data for the model training
    X = np.array(data_window).reshape(-1, 1)
    model.fit(X)
    data_entry['model_ready'] = True
    logging.info(f"ML Model updated for {operation_type} with the latest data window")

def consume_kafka_data():
    consumer = Consumer(conf)
    consumer.subscribe(['time_series'])  # Ensure this matches the producer's topic
    logging.info("Kafka Consumer started")
    try:
        while True:
            msg = consumer.poll(5.0)  # Increase poll timeout to 5 seconds
            if msg is None:
                logging.info("No new message received")
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logging.error(f"Consumer error: {msg.error()}")
                    continue
            data = json.loads(msg.value().decode('utf-8'))
            logging.info(f"Message received: {data}")  # Log received message

            operation_type = data.get('operation_type')
            if operation_type not in operation_types:
                continue  # Ignore if operation_type is not recognized

            field_name = data_store[operation_type]['field_name']
            if field_name not in data:
                continue  # Skip if the required field is not present

            data_point = data[field_name]
            timestamp = pd.Timestamp.now()

            with data_lock:
                data_queue.put({
                    'timestamp': timestamp,
                    'value': data_point,
                    'operation_type': operation_type
                })
                logging.info(f"Data added to queue for {operation_type}")
        consumer.close()
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
                html.H1("Real-Time Outlier Detection for Oil & Gas Operations", className="text-center text-primary mb-4"),
                width=12
            )
        ),
        dbc.Row(
            [
                dbc.Col(
                    dcc.Graph(id='onshore-graph', animate=False),
                    width=12
                ),
                dbc.Col(
                    dcc.Graph(id='offshore-graph', animate=False),
                    width=12
                ),
                dbc.Col(
                    dcc.Graph(id='specialized-graph', animate=False),
                    width=12
                )
            ]
        ),
        dcc.Interval(
            id='graph-update',
            interval=1*1000,  # Update every second
            n_intervals=0
        )
    ],
    fluid=True
)

@app.callback(
    [Output('onshore-graph', 'figure'),
     Output('offshore-graph', 'figure'),
     Output('specialized-graph', 'figure')],
    [Input('graph-update', 'n_intervals')]
)
def update_graphs(n):
    with data_lock:
        # Fetch data from the queue
        while not data_queue.empty():
            data = data_queue.get()
            timestamp = data['timestamp']
            value = data['value']
            operation_type = data['operation_type']

            data_entry = data_store[operation_type]
            data_entry['timestamps'].append(timestamp)
            data_entry['values'].append(value)
            data_entry['data_window'].append(value)

            # Update the ML model if we have enough data points
            if len(data_entry['data_window']) >= 50 and not data_entry['model_ready']:
                update_model(operation_type)

            # Anomaly detection using the ML model
            if data_entry['model_ready']:
                prediction = data_entry['model'].predict([[value]])
                if prediction == -1:  # -1 indicates an outlier in Isolation Forest
                    logging.info(f"Outlier detected in {operation_type}: {value}")
                    data_entry['outlier_timestamps'].append(timestamp)
                    data_entry['outlier_values'].append(value)

    figures = []
    for operation_type in operation_types:
        data_entry = data_store[operation_type]

        # Limit data length to prevent memory issues
        max_length = 1000
        data_entry['timestamps'] = data_entry['timestamps'][-max_length:]
        data_entry['values'] = data_entry['values'][-max_length:]
        data_entry['outlier_timestamps'] = data_entry['outlier_timestamps'][-max_length:]
        data_entry['outlier_values'] = data_entry['outlier_values'][-max_length:]

        # Create the figure
        data_traces = [
            go.Scatter(
                x=data_entry['timestamps'],
                y=data_entry['values'],
                mode='lines',
                name=f'{operation_type} Data',
                line=dict(width=2)
            ),
            go.Scatter(
                x=data_entry['outlier_timestamps'],
                y=data_entry['outlier_values'],
                mode='markers',
                marker=dict(color='red', size=8, symbol='circle'),
                name='Outliers'
            )
        ]

        xaxis_range = [data_entry['timestamps'][0], data_entry['timestamps'][-1]] if data_entry['timestamps'] else [None, None]
        y_min = min(data_entry['values'] + data_entry['outlier_values']) if data_entry['values'] or data_entry['outlier_values'] else None
        y_max = max(data_entry['values'] + data_entry['outlier_values']) if data_entry['values'] or data_entry['outlier_values'] else None
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
                title=f"{data_entry['field_name'].replace('_', ' ').title()}",
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
            title=f"Real-Time {operation_type} Data Stream with Outliers"
        )

        figures.append({'data': data_traces, 'layout': layout})

    return figures

if __name__ == '__main__':
    # Start Kafka consumer thread inside the main block
    consumer_thread = threading.Thread(target=consume_kafka_data, daemon=True)
    consumer_thread.start()
    app.run_server(debug=True, use_reloader=False)