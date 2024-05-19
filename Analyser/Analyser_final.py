
import paho.mqtt.client as mqtt
import time
import numpy as np
from collections import defaultdict, deque
import pandas as pd  # Add this import
import threading

BROKER = 'localhost'
REQUEST_TOPICS = ['request/qos', 'request/delay', 'request/instancecount']
DELAY_VALUES = [0, 1, 2, 4]
QOS_VALUES = [0, 1, 2]
INSTANCE_COUNT_VALUES = [1, 2, 3, 4, 5]
DURATION = 60
lock = threading.Lock()

# Data structure to hold message data
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

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        client.subscribe('$SYS/#')

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

def process_sys_message(msg):
    topic = msg.topic
    payload = msg.payload.decode()
    timestamp = time.time()
    
    if topic in sys_metrics:
        sys_metrics[topic].append((timestamp, payload))

def send_requests(client, qos, delay, instance_count):
    with lock:
        client.publish('request/qos', qos)
        client.publish('request/delay', delay)
        client.publish('request/instancecount', instance_count)

def analyze_results():
    received_counter = len(messages)
    print(f"Received {received_counter} messages")
    if received_counter == 0:
        return {
            "total_rate": 0,
            "loss_rate": 100,
            "out_of_order_rate": 0,
            "median_gap": None,
            "sys_metrics": sys_metrics
        }

    # Convert deque to numpy array to avoid mutation during iteration and for efficient calculations
    messages_np = np.array(messages, dtype=[('counter', int), ('timestamp', float), ('publisher', 'U20')])
    
    # Extract message counters, timestamps, and publishers
    counters = messages_np['counter']
    timestamps = messages_np['timestamp']
    publishers = messages_np['publisher']

    first_msg = np.min(counters)
    first_timestamp = timestamps[np.argmin(counters)]
    last_msg = np.max(counters)
    last_timestamp = timestamps[np.argmax(counters)]

    print(f"First message: {first_msg} at {first_timestamp}")
    print(f"Last message: {last_msg} at {last_timestamp}")
    
    expected_counter = last_msg - first_msg + 1

    lost_messages = expected_counter - received_counter
    print(f"Lost messages: {lost_messages}", f"Expected messages: {expected_counter}",  f"Received messages: {received_counter}")
    loss_rate = (lost_messages / expected_counter) * 100
    print(f"Loss rate: {loss_rate}%")
    
    out_of_order_count = np.sum(counters[1:] < counters[:-1])
    print(f"Out of order messages: {out_of_order_count}")
    out_of_order_rate = (out_of_order_count / received_counter) * 100
    print(f"Out of order rate: {out_of_order_rate}%")

    # Calculate the gaps where the counter is incremented by 1, per publisher
    publisher_gaps = defaultdict(list)
    for pub_id in np.unique(publishers):
        pub_indices = np.where(publishers == pub_id)[0]
        pub_counters = counters[pub_indices]
        pub_timestamps = timestamps[pub_indices]

        valid_gaps = np.where(pub_counters[1:] == pub_counters[:-1] + 1)[0]
        if valid_gaps.size > 0:
            gaps = (pub_timestamps[valid_gaps + 1] - pub_timestamps[valid_gaps]) * 1000  # Convert to milliseconds
            publisher_gaps[pub_id].extend(gaps)
    
    all_gaps = [gap for gaps in publisher_gaps.values() for gap in gaps]
    median_gap = np.median(all_gaps) if all_gaps else None
    print(f"Median gap: {median_gap} ms")
    first_to_last_duration = last_timestamp - first_timestamp if first_timestamp and last_timestamp else None
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

def run_test(client, pub_qos, sub_qos, delay, instance_count):
    global messages, sys_metrics
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
    
    send_requests(client, pub_qos, delay, instance_count)
    time.sleep(DURATION)  # Extra time to account for setup and teardown
    
    return analyze_results()

def save_results(results, filename='results.xlsx'):
    # Convert results to a pandas DataFrame
    df = pd.DataFrame(results)
    # Save the DataFrame to an Excel file
    filename = f"results_{time.strftime('%Y%m%d-%H%M%S')}.xlsx"
    df.to_excel(filename, index=False)

def start_analyzer():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(BROKER, 1883, 60)
    client.loop_start()

    results = []

    for pub_qos in QOS_VALUES:
        for sub_qos in QOS_VALUES:
            for delay in DELAY_VALUES:
                for instance_count in INSTANCE_COUNT_VALUES:
                    print(f"Subscribing with QoS {sub_qos},", f"Publishing with QoS {pub_qos},", f"Delay {delay},", f"Instance count {instance_count}")
                    client.subscribe(f'counter/+/+/+', qos=sub_qos)
                    send_requests(client, pub_qos, delay, instance_count)
                    # time.sleep(1)  # Allow publishers to process the requests

                    # Synchronization step
                    # client.publish('request/sync', 'start')
                    # time.sleep(1)
                    result = run_test(client, pub_qos, sub_qos, delay, instance_count)
                    result.update({"pub_qos": pub_qos, "sub_qos": sub_qos, "delay": delay, "instance_count": instance_count})
                    results.append(result)
                    print(f"Results: {result}")
                    print(f"Unsubscribing from counter topics")
                    print("===============================================")
                    client.unsubscribe(f'counter/+/+/+')

    client.loop_stop()
    client.disconnect()

    # Print all results
    for result in results:
        print(result)
    save_results(results)

if __name__ == "__main__":
    start_analyzer()