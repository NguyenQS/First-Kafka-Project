from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092"
)

producer.send(
    "football-events",
    b"Tor Musiala"
)

producer.flush()

print("Nachricht gesendet.")