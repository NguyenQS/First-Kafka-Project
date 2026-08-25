import psycopg

connection = psycopg.connect(
    host="localhost",
    port=5432,
    dbname="football",
    user="football",
    password="football"
)

with connection.cursor() as cursor:
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS team_statistics (
            team TEXT PRIMARY KEY,
            goals INTEGER NOT NULL DEFAULT 0
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS processed_events (
            event_id TEXT PRIMARY KEY
        )
    """)

connection.commit()
connection.close()

print("Tabelle team_statistics ist bereit.")