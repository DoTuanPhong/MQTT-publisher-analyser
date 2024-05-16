import paho.mqtt.client as mqtt
import time
import json

class Analyser:
    def __init__(self):
        self.client = mqtt.Client()
        self.client.on_message = self.on_message
        self.client.connect("localhost", 1883, 60)
        self.client.subscribe("counter/#")
        self.client.loop_start()

    def on_message(self, client, userdata, msg):
        # Process the message here
        print(f"Received message: {msg.payload.decode()} on topic {msg.topic}")

    def run_tests(self):
        qos_values_p = [0, 1, 2]  # Replace with your QoS values for the Publisher
        qos_values_a = [0, 1, 2]  # Replace with your QoS values for the Analyser
        delay_values = [0, 1, 2, 4]
        instance_counts = range(1, 6)

        for qos_p in qos_values_p:
            for qos_a in qos_values_a:
                for delay in delay_values:
                    for instance_count in instance_counts:
                        self.client.publish("request/qos_p", qos_p)
                        self.client.publish("request/qos_a", qos_a)
                        self.client.publish("request/delay", delay)
                        self.client.publish("request/instancecount", instance_count)
                        time.sleep(60)  # Wait for 60 seconds

        self.client.loop_stop()
analyser = Analyser()
analyser.run_tests()
