import paho.mqtt.client as mqtt
import time
import threading
import logging

BROKER = 'localhost' # Broker address
REQUEST_TOPICS = ['request/qos', 'request/delay', 'request/instancecount']
ACK_TOPIC = 'acknowledgment/#'
counter = 0 # Global counter variable
logging.basicConfig(level=logging.INFO)
counter_lock = threading.Lock() # Lock to protect the counter variable

class Publisher:
    # Initialize the publisher with the instance ID
    def __init__(self, instance_id):
        self.instance_id = instance_id
        self.qos = 0
        self.delay = 0
        self.instance_count = 0
        self.active = False
        self.client = mqtt.Client(client_id=instance_id, protocol=mqtt.MQTTv31)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.lock = threading.Lock()
        self.ack_received = False

    # Callback function for when the client receives a CONNACK response from the server
    def on_connect(self, client, userdata, flags, rc):
        for topic in REQUEST_TOPICS:
            client.subscribe(topic)
        client.subscribe(ACK_TOPIC)

    def on_message(self, client, userdata, msg):
        # Update the QoS, delay, and instance count based on the received message
        if msg.topic == 'request/qos':
            self.qos = int(msg.payload.decode())
        elif msg.topic == 'request/delay':
            self.delay = int(msg.payload.decode())
        elif msg.topic == 'request/instancecount':
            requested_instance_count = int(msg.payload.decode())
            self.active = (int(self.instance_id.split('-')[1]) <= requested_instance_count)
        elif msg.topic.startswith('acknowledgment'):
            self.ack_received = True

    def publish_messages(self): # Publish messages for 60 seconds
        global counter
        start_time = time.time()

        # Publish messages for 60 seconds
        while time.time() - start_time < 60:
            # Check if the publisher is active
            if not self.active:
                break
            # Publish messages with a delay
            with counter_lock:  # Lock the counter variable
                topic = f'counter/{self.instance_id}/{self.qos}/{self.delay}'
                self.client.publish(topic, f"{counter}", qos=self.qos)
                counter += 1
            self.ack_received = False
            time.sleep(self.delay / 1000.0)  # Convert milliseconds to seconds

            # Wait for acknowledgment
            start_ack_time = time.time()
            while not self.ack_received:
                if time.time() - start_ack_time > 1:  # Timeout for acknowledgment
                    logging.warning(f"Acknowledgment not received for message {counter - 1}")
                    break
                time.sleep(0.01)

    def start(self):
        self.client.connect(BROKER, 1883, 60)   # Connect to the broker
        self.client.loop_start()

        # Publish messages indefinitely
        while True:
            self.publish_messages()  # Reset ready state after publishing burst
            time.sleep(1)
        # Disconnect the client
        self.client.loop_stop()
        self.client.disconnect()

if __name__ == "__main__":
    publishers = ['pub-1', 'pub-2', 'pub-3', 'pub-4', 'pub-5']  # List of publishers
    threads = []

    # Start the publishers
    for pub_id in publishers:
        publisher = Publisher(pub_id)
        publisher_thread = threading.Thread(target=publisher.start)
        threads.append(publisher_thread)
        publisher_thread.start()

    # Wait for all threads to finish
    for thread in threads:
        thread.join()
