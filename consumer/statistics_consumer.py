import json
from collections import defaultdict
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "football-events-json",
    bootstrap_servers="localhost:9092",
    group_id="statistics",
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

goals_by_team = defaultdict(int)

print("Statistics consumer wartet auf Nachrichten...")

for message in consumer:
    event = message.value

    if event["event"] == "goal":
        team = message.key.decode("utf-8")
        goals_by_team[team] += 1

        print(dict(goals_by_team))