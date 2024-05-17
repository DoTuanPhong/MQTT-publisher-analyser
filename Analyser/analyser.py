import paho.mqtt.client as mqtt
import time
import threading
from collections import defaultdict
import numpy as np



class Analyser:
    def __init__(self):
        self.client = mqtt.Client()
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.connect("127.0.0.1", 1883, 60)
        self.client.loop_start()
        self.messages = defaultdict(list)
        self.start_time = None
        self.end_time = None
        self.qos = 0

    def on_connect(self, client, userdata, flags, rc):
        print(f"Analyser connected with result code {rc}")
        self.client.subscribe("counter/#")

    def on_message(self, client, userdata, msg):
        if self.start_time is None:
            self.start_time = time.time()
        self.end_time = time.time()
        topic_parts = msg.topic.split('/')
        instance_id = int(topic_parts[1])
        qos = int(topic_parts[2])
        delay = int(topic_parts[3])
        counter = int(msg.payload.decode())
        timestamp = time.time()
        self.messages[(instance_id, qos, delay)].append((counter, timestamp))

    def compute_statistics(self):
        for key, msgs in self.messages.items():
            instance_id, qos, delay = key
            if not msgs:
                continue

            counters, timestamps = zip(*msgs)
            received_count = len(counters)
            expected_count = (self.end_time - self.start_time) / (delay / 1000.0) if delay > 0 else float('inf')
            loss_rate = (expected_count - received_count) / expected_count * 100 if expected_count > 0 else 0
            out_of_order_count = sum(1 for i in range(1, received_count) if counters[i] < counters[i - 1])
            out_of_order_rate = out_of_order_count / received_count * 100 if received_count > 0 else 0
            inter_message_gaps = [timestamps[i] - timestamps[i - 1] for i in range(1, received_count) if counters[i] == counters[i - 1] + 1]
            median_gap = np.median(inter_message_gaps) * 1000 if inter_message_gaps else None

            print(f"Instance ID: {instance_id}, Pub QoS: {qos}, Analyser QoS: {self.qos}, Delay: {delay} ms")
            print(f"Total Average Rate: {received_count / 60:.2f} messages/second")
            print(f"Message Loss Rate: {loss_rate:.2f}%")
            print(f"Out-of-Order Rate: {out_of_order_rate:.2f}%")
            print(f"Median Inter-Message Gap: {median_gap:.2f} ms compared to requested {delay} ms\n")

    def run_tests(self):
        qos_values_p = [0, 1, 2]  # QoS values for the Publisher
        qos_values_a = [0, 1, 2]  # QoS values for the Analyser
        delay_values = [0, 1, 2, 4]
        instance_counts = range(1, 6)

        for qos_a in qos_values_a:
            self.qos = qos_a
            for qos_p in qos_values_p:
                for delay in delay_values:
                    for instance_count in instance_counts:
                        self.messages.clear()
                        self.start_time = None
                        self.end_time = None
                        print(f"Starting test with Pub QoS={qos_p}, Analyser QoS={qos_a}, Delay={delay}, InstanceCount={instance_count}")
                        self.client.publish("request/qos", qos_p)
                        self.client.publish("request/delay", delay)
                        self.client.publish("request/instancecount", instance_count)
                        time.sleep(60)  # Wait for 60 seconds
                        self.compute_statistics()

        self.client.loop_stop()

analyser = Analyser()
analyser.run_tests()
