import json
import random
from paho.mqtt import client as mqtt_client


broker = '116.118.95.187'
port = 1883
car_id = "12345" 
topic = f"car/{car_id}/telemetry"
username = 'ducchien0612'
password = '123456'


def connect_mqtt() -> mqtt_client:
    def on_connect(client, userdata, flags, rc, properties):
        if flags.session_present:
            print("Session is present")
        if rc == 0:
            print("Connected to MQTT Broker!")
        if rc > 0:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION2)

    client.username_pw_set(username, password)
    client.on_connect = on_connect

    client.connect(broker, port)
    return client


def subscribe(client):
    def on_message(client, userdata, msg):
        print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
        try:
            data = json.loads(msg.payload.decode())
            print("Parsed data:", data)
        except json.JSONDecodeError:
            print("Failed to decode JSON")

    client.subscribe(topic)
    client.on_message = on_message


def run():
    client = connect_mqtt()
    subscribe(client)
    client.loop_forever()


if __name__ == '__main__':
    run()
