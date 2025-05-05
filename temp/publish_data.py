import json
from paho.mqtt import client as mqtt_client
import random
import time

broker = '116.118.95.187'
port = 1883
username = 'ducchien0612'
password = '123456'
car_id = "12345" 
telemetry_topic = f"car/{car_id}/telemetry"
assign_topic = f"car/{car_id}/assign"

def connect_mqtt():
    assign_data = {
        "status": "moving",
        "battery": 85.4,
        "lat": 37.7749,
        "lng": -122.4194,
        "speed": 45.2
    }
    def on_connect(client, userdata, flags, rc, properties):
        if flags.session_present:
            print("Session is present")
        if rc == 0:
            print("Connected to MQTT Broker!")
            # Publish assign data to the assign topic
            #client.sub(assign_topic, json.dumps(assign_data))
            client.subscribe(assign_topic)
        if rc > 0:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION2)

    # client.username_pw_set(username, password)    
    #client.connect(broker, port)
    client.username_pw_set(username, password)
    client.on_connect = on_connect

    client.connect(broker, port)
    return client

def sub(client):
    def on_message(client, userdata, msg):
        print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
        try:
            data = json.loads(msg.payload.decode())
            print("Parsed data:", data)
        except json.JSONDecodeError:
            print("Failed to decode JSON")
            
def publish(client):
    telemetry_data = {
        "status": "moving",
        "battery": 85.4,
        "lat": 37.7749,
        "lng": -122.4194,
        "speed": 45.2
    }
    while True:
        time.sleep(1)
        msg = json.dumps(telemetry_data)
        result = client.publish(telemetry_topic, msg)
        status = result[0]
        if status == 0:
            print(f"Sent `{msg}` to topic `{telemetry_topic}`")
        else:
            print(f"Failed to send message to topic {telemetry_topic}")
        telemetry_data["battery"] -= 0.1
        telemetry_data["speed"] += 0.5
        if telemetry_data["battery"] <= 0:
            print("Battery depleted. Stopping telemetry.")
            break


def run():
    client = connect_mqtt()
    sub(client)
    client.loop_start()
    publish(client)
    client.loop_stop()


if __name__ == '__main__':
    run()
