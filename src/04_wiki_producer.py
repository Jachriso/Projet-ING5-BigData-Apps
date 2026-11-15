# 04_wiki_producer.py - Reads Wikimedia recent changes and sends them to Kafka.
# Run : python src/04_wiki_producer.py
import json
import requests
from kafka import KafkaProducer

URL = "https://stream.wikimedia.org/v2/stream/recentchange"
TOPIC = "wiki-changes"

producer = KafkaProducer(bootstrap_servers="localhost:9092",
                         value_serializer=lambda v: json.dumps(v).encode("utf-8"))
headers = {"User-Agent": "ece-bigdata-student-project/1.0", "Accept": "text/event-stream"}
with requests.get(URL, stream=True, headers=headers, timeout=60) as r:
    for line in r.iter_lines(decode_unicode=True):
        if line and line.startswith("data: "):
            event = json.loads(line[6:])
            if event.get("wiki") == "enwiki" and event.get("namespace") == 0:
                producer.send(TOPIC, event)