from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("FootballSparkBasics")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

events = [
    ("Bayern", 15, "goal"),
    ("Dortmund", 74, "goal"),
    ("Bayern", 93, "goal"),
    ("Leverkusen", 30, "yellow_card"),
    ("Bayern", 55, "goal"),
    ("Dortmund", 20, "yellow_card"),
]

df = spark.createDataFrame(
    events,
    ["team", "minute", "event"]
)

print("Alle Events:")
df.show()

print("Nur Tore:")
df.filter(df.event == "goal").show()

print("Tore pro Team:")
(
    df
    .filter(df.event == "goal")
    .groupBy("team")
    .count()
    .show()
)

goals_per_team = (
    df
    .filter(df.event == "goal")
    .groupBy("team")
    .count()
)

print("Spark-Ausführungsplan:")
goals_per_team.explain()

print("Anzahl Spark-Partitionen:")
print(df.rdd.getNumPartitions())

from pyspark.sql.functions import col, when

print("Ausgewählte Spalten:")
df.select("team", "minute").show()

transformed_df = (
    df
    .withColumn(
        "match_phase",
        when(col("minute") <= 45, "first_half")
        .when(col("minute") <= 90, "second_half")
        .otherwise("stoppage_time")
    )
)

print("Mit berechneter match_phase:")
transformed_df.show()

print("Events pro Spielphase:")
(
    transformed_df
    .groupBy("match_phase")
    .count()
    .show()
)

transformed_df.createOrReplaceTempView("football_events")

print("Spark SQL - Events pro Spielphase:")

spark.sql("""
    SELECT
        match_phase,
        COUNT(*) AS event_count
    FROM football_events
    GROUP BY match_phase
    ORDER BY event_count DESC
""").show()

teams = [
    ("Bayern", "Munich"),
    ("Dortmund", "Dortmund"),
    ("Leverkusen", "Leverkusen"),
]

teams_df = spark.createDataFrame(
    teams,
    ["team", "city"]
)

print("Teams:")
teams_df.show()

joined_df = transformed_df.join(
    teams_df,
    on="team",
    how="left"
)

print("Events mit Team-Stadt:")
joined_df.show()

spark.stop()