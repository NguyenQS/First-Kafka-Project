import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

events = [
    ("Bayern", {
        "match": "Bayern - Dortmund",
        "minute": 67,
        "event": "goal",
        "player": "Musiala"
    }),
    ("Dortmund", {
        "match": "Bayern - Dortmund",
        "minute": 72,
        "event": "goal",
        "player": "Brandt"
    }),
    ("Leverkusen", {
        "match": "Leverkusen - Mainz",
        "minute": 80,
        "event": "goal",
        "player": "Wirtz"
    })
]

for team, event in events:
    future = producer.send(
        "football-events",
        key=team.encode("utf-8"),
        value=event
    )

    metadata = future.get(timeout=10)

    print(
        f"{team} -> Partition {metadata.partition}, "
        f"Offset {metadata.offset}"
    )

producer.flush()

print("JSON-Nachrichten gesendet.")