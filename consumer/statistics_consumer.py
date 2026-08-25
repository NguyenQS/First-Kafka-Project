import json
import psycopg
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "football-events-json",
    bootstrap_servers="localhost:9092",
    group_id="statistics-db",
    auto_offset_reset="latest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

connection = psycopg.connect(
    host="localhost",
    port=5432,
    dbname="football",
    user="football",
    password="football"
)

print("Statistics consumer wartet auf Nachrichten...")

for message in consumer:
    event = message.value

    if event["event"] != "goal":
        continue

    event_id = event["event_id"]
    team = message.key.decode("utf-8")

    try:
        with connection.cursor() as cursor:
            # Wurde dieses Event schon verarbeitet?
            cursor.execute(
                "SELECT 1 FROM processed_events WHERE event_id = %s",
                (event_id,)
            )

            already_processed = cursor.fetchone()

            if already_processed:
                print(f"{event_id} wurde bereits verarbeitet -> übersprungen.")
                continue

            # Statistik aktualisieren
            cursor.execute(
                """
                INSERT INTO team_statistics (team, goals)
                VALUES (%s, 1)
                ON CONFLICT (team)
                DO UPDATE SET goals = team_statistics.goals + 1
                """,
                (team,)
            )

            # Event als verarbeitet markieren
            cursor.execute(
                """
                INSERT INTO processed_events (event_id)
                VALUES (%s)
                """,
                (event_id,)
            )

        # Beide DB-Änderungen zusammen dauerhaft speichern
        connection.commit()

        print(f"{event_id}: Tor für {team} gespeichert.")

    except Exception:
        connection.rollback()
        raise