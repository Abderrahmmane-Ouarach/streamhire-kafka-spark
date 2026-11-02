import time
from kafka import KafkaProducer   # pip install kafka-python

producer = KafkaProducer(bootstrap_servers="localhost:9092")

with open("jobs.txt", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if line:
            producer.send("jobs", line.encode("utf-8"))
            print("envoyé :", line)
            time.sleep(1)   # simule un flux en temps réel

producer.flush()


