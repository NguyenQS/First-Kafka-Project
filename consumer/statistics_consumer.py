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

    # Transform
    event["event"] = event["event"].strip().lower()
    event["player"] = event["player"].strip()

    team = message.key.decode("utf-8").strip().title()
    event_id = event["event_id"]

    minute = event["minute"]

    if minute <= 45:
        match_phase = "first_half"
    elif minute <= 90:
        match_phase = "second_half"
    else:
        match_phase = "stoppage_time"

    print(
        f"Transformiert: {team}, Minute {minute} "
        f"-> {match_phase}"
    )

    if event["event"] != "goal":
        continue

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

            cursor.execute(
                """
                INSERT INTO goal_events (event_id, team, minute, match_phase)
                VALUES (%s, %s, %s, %s)
                """,
                (event_id, team, minute, match_phase)
            )

            # Event als verarbeitet markieren
            cursor.execute(
                """
                INSERT INTO processed_events (event_id)
                VALUES (%s)
                """,
                (event_id,)
            )

        connection.commit()

        print(f"{event_id}: Tor für {team} gespeichert.")

    except Exception:
        connection.rollback()
        raise