from kafka import KafkaConsumer
from multiprocessing import Process
import json

consumer = KafkaConsumer("topic-1", bootstrap_servers=['localhost:9092'], api_version=(0, 10), fetch_max_wait_ms=0)

def consumeData():
    for msg in consumer:
        print(str(json.loads(msg.value)["value"]) + " " + str(json.loads(msg.value)["timestamp"]) + "\n")

t1 = Process(target=consumeData)
t1.start()

# if consumer is not None:
#     consumer.close()
