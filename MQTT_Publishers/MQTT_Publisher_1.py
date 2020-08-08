#!/usr/bin/env python3
from random import seed, random
import paho.mqtt.client as mqtt
import time, threading

# Publisher
seed(1)
client = mqtt.Client()
client.connect("localhost", 1883, 60)
ticker = threading.Event()

while not ticker.wait(0.1):
    r = random()
    client.publish("topic/topic-1", r);

client.disconnect();
