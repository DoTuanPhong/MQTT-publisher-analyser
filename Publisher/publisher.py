import paho.mqtt.client as mqtt
import time
import threading

class Publisher:
    def __init__(self, instance_id):
        self.instance_id = instance_id
        self.active = False
        self.qos = 0
        self.delay = 0
        self.client = mqtt.Client(f"pub-{instance_id}")
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

    def on_connect(self, client, userdata, flags, rc):
        print(f"Publisher {self.instance_id} connected with result code {rc}")
        self.client.subscribe("request/qos")
        self.client.subscribe("request/delay")
        self.client.subscribe("request/instancecount")

    def on_message(self, client, userdata, msg):
        if msg.topic == "request/qos":
            self.qos = int(msg.payload.decode())
        elif msg.topic == "request/delay":
            self.delay = int(msg.payload.decode())
        elif msg.topic == "request/instancecount":
            instance_count = int(msg.payload.decode())
            self.active = (self.instance_id <= instance_count)

    def publish_messages(self):
        counter = 0
        while self.active:
            topic = f"counter/{self.instance_id}/{self.qos}/{self.delay}"
            message = str(counter)
            self.client.publish(topic, message, qos=self.qos)
            counter += 1
            time.sleep(self.delay / 1000.0)  # Convert delay to seconds
        self.client.loop_stop()

    def run(self):
        self.client.connect("localhost", 1883, 60)
        self.client.loop_start()
        self.publish_messages()


# Creating and running 5 instances of Publisher
publishers = []
for i in range(1, 6):
    publisher = Publisher(i)
    publishers.append(publisher)
    threading.Thread(target=publisher.run).start()
