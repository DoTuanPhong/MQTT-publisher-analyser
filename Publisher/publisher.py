import paho.mqtt.client as mqtt
import time
import threading

class Publisher:
    def __init__(self, instance_id):
        self.instance_id = instance_id
        self.active = False
        self.qos = 0
        self.delay = 0
        self.client = mqtt.Client(client_id=f"pub-{instance_id}", clean_session=True, protocol=mqtt.MQTTv311)
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
        start_time = time.time()
        while self.active and time.time() - start_time < 60:  # Publish for 60 seconds
            topic = f"counter/{self.instance_id}/{self.qos}/{self.delay}"
            message = str(counter)
            self.client.publish(topic, message, qos=self.qos)
            counter += 1
            time.sleep(self.delay / 1000.0)  # Convert delay to seconds
        self.client.loop_stop()

    def run(self):
        self.client.connect("127.0.0.1", 1883, 60)
        self.client.loop_start()
        threading.Thread(target=self.publish_messages).start()  # Start publishing in a new thread


publishers = [Publisher(i) for i in range(1, 6)]  # Create 5 publishers
for publisher in publishers:
    publisher.run()  # Start each publisher
