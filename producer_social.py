import re
import time
import requests
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers="localhost:9092")
API = "https://hn.algolia.com/api/v1"
seen = set()

def latest_hiring_thread():
    r = requests.get(f"{API}/search_by_date",
                     params={"query": "Ask HN: Who is hiring?",
                             "tags": "story,author_whoishiring"}, timeout=10)
    return r.json()["hits"][0]["objectID"]

thread = latest_hiring_thread()
print("Fil utilisé :", thread)

while True:
    r = requests.get(f"{API}/search_by_date",
                     params={"tags": f"comment,story_{thread}",
                             "hitsPerPage": 20}, timeout=10)
    for c in r.json()["hits"]:
        if c["objectID"] in seen or not c.get("comment_text"):
            continue
        seen.add(c["objectID"])
        text = re.sub(r"<[^>]+>", " ", c["comment_text"])   # retire le HTML
        text = " ".join(text.split())[:200]                  # une ligne, 200 car.
        producer.send("jobs", text.encode("utf-8"))
        print(text)
    producer.flush()
    time.sleep(20)