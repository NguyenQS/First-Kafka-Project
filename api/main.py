import json
import psycopg

from fastapi import FastAPI
from pydantic import BaseModel
from kafka import KafkaProducer

app = FastAPI()

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

db_connection = psycopg.connect(
    host="localhost",
    port=5432,
    dbname="football",
    user="football",
    password="football"
)


class FootballEvent(BaseModel):
    event_id: str
    team: str
    match: str
    minute: int
    event: str
    player: str


@app.post("/events")
def create_event(event: FootballEvent):
    kafka_event = {
        "event_id": event.event_id,
        "match": event.match,
        "minute": event.minute,
        "event": event.event,
        "player": event.player
    }

    future = producer.send(
        "football-events-json",
        key=event.team.encode("utf-8"),
        value=kafka_event
    )

    metadata = future.get(timeout=10)

    return {
        "status": "sent",
        "partition": metadata.partition,
        "offset": metadata.offset
    }

@app.get("/statistics")
def get_statistics():
    with db_connection.cursor() as cursor:
        cursor.execute("""
            SELECT team, goals
            FROM team_statistics
            ORDER BY goals DESC, team
        """)

        rows = cursor.fetchall()

    return [
        {
            "team": team,
            "goals": goals
        }
        for team, goals in rows
    ]