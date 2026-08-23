import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

events = [
    ("Bayern", {"match": "Bayern - Dortmund", "minute": 12, "event": "goal", "player": "Musiala"}),
    ("Dortmund", {"match": "Bayern - Dortmund", "minute": 18, "event": "yellow_card", "player": "Schlotterbeck"}),
    ("Leverkusen", {"match": "Leverkusen - Mainz", "minute": 21, "event": "goal", "player": "Wirtz"}),
    ("Bayern", {"match": "Bayern - Dortmund", "minute": 27, "event": "yellow_card", "player": "Kimmich"}),
    ("Mainz", {"match": "Leverkusen - Mainz", "minute": 31, "event": "substitution", "player": "Burkardt"}),
    ("Dortmund", {"match": "Bayern - Dortmund", "minute": 35, "event": "goal", "player": "Brandt"}),
    ("Frankfurt", {"match": "Frankfurt - Freiburg", "minute": 39, "event": "goal", "player": "Marmoush"}),
    ("Freiburg", {"match": "Frankfurt - Freiburg", "minute": 42, "event": "yellow_card", "player": "Ginter"}),
    ("Bayern", {"match": "Bayern - Dortmund", "minute": 45, "event": "halftime", "player": "-"}),
    ("Leverkusen", {"match": "Leverkusen - Mainz", "minute": 49, "event": "goal", "player": "Boniface"}),
    ("Mainz", {"match": "Leverkusen - Mainz", "minute": 53, "event": "yellow_card", "player": "Kohr"}),
    ("Dortmund", {"match": "Bayern - Dortmund", "minute": 58, "event": "substitution", "player": "Adeyemi"}),
    ("Bayern", {"match": "Bayern - Dortmund", "minute": 63, "event": "goal", "player": "Kane"}),
    ("Frankfurt", {"match": "Frankfurt - Freiburg", "minute": 67, "event": "goal", "player": "Ekitike"}),
    ("Freiburg", {"match": "Frankfurt - Freiburg", "minute": 71, "event": "substitution", "player": "Grifo"}),
    ("Leverkusen", {"match": "Leverkusen - Mainz", "minute": 76, "event": "yellow_card", "player": "Xhaka"}),
    ("Mainz", {"match": "Leverkusen - Mainz", "minute": 81, "event": "goal", "player": "Lee"}),
    ("Dortmund", {"match": "Bayern - Dortmund", "minute": 84, "event": "goal", "player": "Guirassy"}),
    ("Bayern", {"match": "Bayern - Dortmund", "minute": 88, "event": "substitution", "player": "Sané"}),
    ("Bayern", {"match": "Bayern - Dortmund", "minute": 90, "event": "fulltime", "player": "-"})
]

for team, event in events:
    future = producer.send(
        "football-events-json",
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