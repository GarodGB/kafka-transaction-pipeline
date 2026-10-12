import json
import sys
from kafka import KafkaProducer

TOPIC = "transactions"


txn_id = int(sys.argv[1]) if len(sys.argv) > 1 else 1


producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


producer.send(TOPIC, {"id": txn_id})
producer.flush()
print(f"Sent id={txn_id} to topic '{TOPIC}'")

producer.close()