import json
import sys
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "football-events-json",
    bootstrap_servers="localhost:9092",
    group_id="live-ticker",
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

consumer_name = sys.argv[1]

print(f"{consumer_name} wartet auf Nachrichten...")

for message in consumer:
    event = message.value

    print(
        f"{consumer_name}: "
        f"Partition {message.partition}, "
        f"Offset {message.offset} -> "
        f"{event['minute']}' {event['player']} "
        f"({event['match']})"
    )