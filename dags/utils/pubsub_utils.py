import requests
import csv
import time
from google.cloud import pubsub_v1
from io import StringIO

def simulate_streaming_from_csv(url, topic_path, batch_size=100, interval_secs=60):
    publisher = pubsub_v1.PublisherClient()

    response = requests.get(url)
    response.raise_for_status()

    csv_data = response.content.decode("utf-8")
    reader = csv.DictReader(StringIO(csv_data))
    rows = list(reader)

    for i in range(0, len(rows), batch_size):
        batch = rows[i:i+batch_size]
        for row in batch:
            publisher.publish(topic_path, str(row).encode("utf-8"))
        time.sleep(interval_secs)
