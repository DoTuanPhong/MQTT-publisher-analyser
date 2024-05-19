import paho.mqtt.client as mqtt
import time
import threading
import logging

BROKER = 'localhost'
REQUEST_TOPICS = ['request/qos', 'request/delay', 'request/instancecount']
counter = 0
logging.basicConfig(level=logging.INFO)

class Publisher:
    def __init__(self, instance_id):
        self.instance_id = instance_id
        self.qos = 0
        self.delay = 0
        self.instance_count = 0
        self.active = False
        self.client = mqtt.Client()
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.lock = threading.Lock()

    def on_connect(self, client, userdata, flags, rc):
        for topic in REQUEST_TOPICS:
            client.subscribe(topic)

    def on_message(self, client, userdata, msg):
        if msg.topic == 'request/qos':
            self.qos = int(msg.payload.decode())
        elif msg.topic == 'request/delay':
            self.delay = int(msg.payload.decode())
        elif msg.topic == 'request/instancecount':
            requested_instance_count = int(msg.payload.decode())
            self.active = (int(self.instance_id.split('-')[1]) <= requested_instance_count)

    def publish_messages(self):
        global counter
        start_time = time.time()

        while time.time() - start_time < 60:
            if not self.active:
                break
            with self.lock:
                topic = f'counter/{self.instance_id}/{self.qos}/{self.delay}'
                self.client.publish(topic, f"{counter}", qos=self.qos)
                counter += 1
            time.sleep(self.delay / 1000.0)  # Convert milliseconds to seconds

    def start(self):
        self.client.connect(BROKER, 1883, 60)
        self.client.loop_start()

        while True:
            self.publish_messages()  # Reset ready state after publishing burst
            time.sleep(1)

        self.client.loop_stop()
        self.client.disconnect()

if __name__ == "__main__":
    publishers = ['pub-1', 'pub-2', 'pub-3', 'pub-4', 'pub-5']
    threads = []

    for pub_id in publishers:
        publisher = Publisher(pub_id)
        publisher_thread = threading.Thread(target=publisher.start)
        threads.append(publisher_thread)
        publisher_thread.start()

    for thread in threads:
        thread.join()
