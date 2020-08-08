#!/usr/bin/env python3
import paho.mqtt.client as mqtt
from kafka import KafkaProducer
import json
import time

# Kafka Producer
def publish_message(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()

    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))

def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=['localhost:9094'], api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer

# MQTT Subscriber
def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    client.subscribe("topic/topic-3")

def on_message(client, userdata, msg):
  """print(msg.payload.decode())"""
  """client.disconnect()"""
  kafka_producer = connect_kafka_producer()
  publish_message(kafka_producer, 'topic-3', 'sensor-3', json.dumps({ "topic": "topic-3", "value": msg.payload.decode(), "timestamp": str(time.strftime("%H:%M:%S", time.localtime())) }))
  if kafka_producer is not None:
      kafka_producer.close()

client = mqtt.Client()
client.connect("localhost", 1883, 60)

client.on_connect = on_connect
client.on_message = on_message

client.loop_forever()
