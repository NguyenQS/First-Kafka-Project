from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "football-events",
    bootstrap_servers="localhost:9092",
    group_id="statistics",
    auto_offset_reset="earliest"
)

print("Warte auf Nachrichten...")

for message in consumer:
    print(message.value.decode())