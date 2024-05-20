
import paho.mqtt.client as mqtt
import time
import numpy as np
from collections import defaultdict, deque
import pandas as pd  # Add this import
import threading

BROKER = 'localhost'
REQUEST_TOPICS = ['request/qos', 'request/delay', 'request/instancecount']
DELAY_VALUES = [0, 1, 2, 4]
# DELAY_VALUES = [0]    # For testing bonus 3
QOS_VALUES = [0, 1, 2]
INSTANCE_COUNT_VALUES = [1, 2, 3, 4, 5]
DURATION = 60
lock = threading.Lock() # Lock to synchronize access to shared data structures

# Data structure to hold message data
messages = deque()
# Data structure to hold system metrics
sys_metrics = {
    '$SYS/broker/clients/connected': [],
    '$SYS/broker/load/connections/1min': [],
    '$SYS/broker/load/messages/received/1min': [],
    '$SYS/broker/load/messages/sent/1min': [],
    '$SYS/broker/load/publish/dropped/1min': [],
    '$SYS/broker/load/sockets/1min': [],
    '$SYS/broker/messages/inflight': [],
    '$SYS/broker/heap/current size': [],
    '$SYS/broker/heap/maximum size': [],
    '$SYS/broker/messages/received': [],
    '$SYS/broker/messages/sent': []
}

# Data structure to hold results
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        client.subscribe('$SYS/#')

# Callback function to process incoming messages
def on_message(client, userdata, msg):
    with lock:
        if msg.topic.startswith('$SYS'):
            process_sys_message(msg)
        else:
            process_message(msg)

def process_message(msg):
    # Extract publisher ID from topic, assuming topic format: 'counter/publisher_id/...'
    topic_parts = msg.topic.split('/')
    publisher_id = topic_parts[1]  # Keep publisher ID as a string
    payload = int(msg.payload.decode())
    timestamp = time.time()
    messages.append((payload, timestamp, publisher_id))

# Callback function to process system messages
def process_sys_message(msg):
    topic = msg.topic
    payload = msg.payload.decode()
    timestamp = time.time()
    
    if topic in sys_metrics:
        sys_metrics[topic].append((timestamp, payload))

# Function to send requests to the publishers
def send_requests(client, qos, delay, instance_count):
    with lock:
        client.publish('request/qos', qos)
        client.publish('request/delay', delay)
        client.publish('request/instancecount', instance_count)

# Function to analyze the results
def analyze_results():
    received_counter = len(messages) # Number of received messages
    print(f"Received {received_counter} messages")

    if received_counter == 0: # No messages received
        return {
            "total_rate": 0,
            "loss_rate": 100,
            "out_of_order_rate": 0,
            "median_gap": None,
            "sys_metrics": sys_metrics
        }

    # Convert messages to a structured NumPy array for easier processing
    messages_np = np.array(messages, dtype=[('counter', int), ('timestamp', float), ('publisher', 'U20')])
    
    # Extract message counters, timestamps, and publishers
    counters = messages_np['counter']
    timestamps = messages_np['timestamp']
    publishers = messages_np['publisher']
        
    # Calculate the first and last message, and the number of lost messages
    first_msg = np.min(counters)
    first_timestamp = timestamps[np.argmin(counters)]
    last_msg = np.max(counters)
    last_timestamp = timestamps[np.argmax(counters)]

    print(f"First message: {first_msg} at {first_timestamp}")
    print(f"Last message: {last_msg} at {last_timestamp}")
    
    # Calculate the number of lost messages, loss rate, and out-of-order rate
    expected_counter = last_msg - first_msg + 1
    lost_messages = expected_counter - received_counter
    loss_rate = (lost_messages / expected_counter) * 100
    out_of_order_count = np.sum(counters[1:] < counters[:-1])
    out_of_order_rate = (out_of_order_count / received_counter) * 100

    print(f"Lost messages: {lost_messages}", f"Expected messages: {expected_counter}",  f"Received messages: {received_counter}")
    print(f"Loss rate: {loss_rate}%")
    print(f"Out of order messages: {out_of_order_count}")
    print(f"Out of order rate: {out_of_order_rate}%")

    # Calculate the gaps where the counter is incremented by 1, per publisher
    publisher_gaps = defaultdict(list)
    for pub_id in np.unique(publishers):
        # Extract counters and timestamps for the current publisher
        pub_indices = np.where(publishers == pub_id)[0]
        pub_counters = counters[pub_indices]
        pub_timestamps = timestamps[pub_indices]
        # Find the valid gaps where the counter is incremented by 1
        valid_gaps = np.where(pub_counters[1:] == pub_counters[:-1] + 1)[0]
        if valid_gaps.size > 0:
            gaps = (pub_timestamps[valid_gaps + 1] - pub_timestamps[valid_gaps]) * 1000  # Convert to milliseconds
            publisher_gaps[pub_id].extend(gaps) # Add gaps to the list for the current publisher
    
    # Calculate the median gap and the first-to-last duration
    all_gaps = [gap for gaps in publisher_gaps.values() for gap in gaps]
    median_gap = np.median(all_gaps) if all_gaps else None
    
    first_to_last_duration = last_timestamp - first_timestamp if first_timestamp and last_timestamp else None

    print(f"Median gap: {median_gap} ms")        
    print(f"First to last duration: {first_to_last_duration}")
    print(f"System metrics: {sys_metrics}")
    
    return {
        "total_rate": received_counter / DURATION,
        "loss_rate": loss_rate,
        "out_of_order_rate": out_of_order_rate,
        "median_gap": median_gap,
        "first_to_last_duration": first_to_last_duration,
        "sys_metrics": sys_metrics
    }

# Function to run the test
def run_test(client, pub_qos, sub_qos, delay, instance_count):
    global messages, sys_metrics # Use global variables
    # Clear the data structures
    messages = deque()
    sys_metrics = {
        '$SYS/broker/clients/connected': [],
        '$SYS/broker/load/connections/1min': [],
        '$SYS/broker/load/messages/received/1min': [],
        '$SYS/broker/load/messages/sent/1min': [],
        '$SYS/broker/load/publish/dropped/1min': [],
        '$SYS/broker/load/sockets/1min': [],
        '$SYS/broker/messages/inflight': [],
        '$SYS/broker/heap/current size': [],
        '$SYS/broker/heap/maximum size': [],
        '$SYS/broker/messages/received': [],
        '$SYS/broker/messages/sent': []
    }
    
    # Send requests to the publishers
    send_requests(client, pub_qos, delay, instance_count)
    # Wait for the test duration
    time.sleep(DURATION)
    
    return analyze_results()

def save_results(results, filename='results.xlsx'):
    # Convert results to a pandas DataFrame
    df = pd.DataFrame(results)
    # Save the DataFrame to an Excel file
    filename = f"results_{time.strftime('%Y%m%d-%H%M%S')}.xlsx"
    df.to_excel(filename, index=False)

# Function to start the analyzer
def start_analyzer():
    # Create an MQTT client
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    # Connect to the broker and start the client loop
    client.connect(BROKER, 1883, 60)
    client.loop_start()

    results = [] # List to hold the results

    # Run the test for all combinations of QoS, delay, and instance count
    for pub_qos in QOS_VALUES:
        for sub_qos in QOS_VALUES:
            for delay in DELAY_VALUES:
                for instance_count in INSTANCE_COUNT_VALUES:
                    print(f"Subscribing with QoS {sub_qos},", f"Publishing with QoS {pub_qos},", f"Delay {delay},", f"Instance count {instance_count}")
                    client.subscribe(f'counter/+/+/+', qos=sub_qos) # Subscribe to all counter topics
                    send_requests(client, pub_qos, delay, instance_count)

                    # Run the test and store the results
                    result = run_test(client, pub_qos, sub_qos, delay, instance_count)
                    result.update({"pub_qos": pub_qos, "sub_qos": sub_qos, "delay": delay, "instance_count": instance_count})
                    results.append(result)
                    print(f"Results: {result}")
                    print(f"Unsubscribing from counter topics")
                    print("===============================================")
                    client.unsubscribe(f'counter/+/+/+') # Unsubscribe from all counter topics

    # Stop the MQTT client
    client.loop_stop()
    client.disconnect()

    # Print all results
    for result in results:
        print(result)
    save_results(results)

if __name__ == "__main__":
    start_analyzer()
