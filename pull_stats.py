import requests
import psycopg2
from datetime import date
import os
from urllib.parse import urlparse


# Get DATABASE_URL from environment
DATABASE_URL = os.environ.get("DATABASE_URL")

if not DATABASE_URL:
    raise Exception("DATABASE_URL environment variable not set")

# Parse the URL and convert to DSN format
result = urlparse(DATABASE_URL)
username = result.username
password = result.password
database = result.path[1:]  # remove leading '/'
hostname = result.hostname
port = result.port

# Connect with SSL enabled
conn = psycopg2.connect(
    dbname=database,
    user=username,
    password=password,
    host=hostname,
    port=port,
    sslmode="require"
)

cur = conn.cursor()

# =======================
# 1. Insert Teams
# =======================
teams_url = "https://api.nhle.com/stats/rest/en/team"
teams_response = requests.get(teams_url).json()
teams_data = teams_response.get("data", [])

for team in teams_data:
    cur.execute("""
        INSERT INTO teams (id, name, abbreviation)
        VALUES (%s, %s, %s)
        ON CONFLICT (id) DO NOTHING
    """, (
        team.get('teamId'),
        team.get('name'),
        team.get('abbreviation')
    ))
conn.commit()
print("Teams inserted successfully.")

# =======================
# 2. Insert Players
# =======================
players_url = "https://api.nhle.com/stats/rest/en/players"
players_response = requests.get(players_url).json()
players_data = players_response.get("data", [])

for player in players_data:
    cur.execute("""
        INSERT INTO players (id, team_id, full_name, position)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING
    """, (
        player.get('playerId'),
        player.get('teamId'),
        player.get('fullName'),
        player.get('positionCode', 'N/A')
    ))
conn.commit()
print("Players inserted successfully.")

# =======================
# 3. Insert Today's Games
# =======================
schedule_url = "https://api-web.nhle.com/v1/schedule/now"
schedule_data = requests.get(schedule_url).json()
games_to_update_stats = []

for game in schedule_data.get("dates", []):
    for g in game.get("games", []):
        game_id = g.get("gamePk")
        game_date = g.get("gameDate")
        home_team = g["teams"]["home"]["team"].get("id")
        away_team = g["teams"]["away"]["team"].get("id")

        cur.execute("""
            INSERT INTO games (game_id, game_date, home_team_id, away_team_id)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (game_id) DO NOTHING
        """, (game_id, game_date, home_team, away_team))

        # Track games that are finished to pull stats
        if g["status"]["detailedState"] == "Final":
            games_to_update_stats.append(game_id)

conn.commit()
print("Today's games inserted successfully.")

# =======================
# 4. Insert Player Stats for Finished Games
# =======================
for game_id in games_to_update_stats:
    stats_url = f"https://api.nhle.com/stats/rest/en/game/boxscore?gameId={game_id}"
    stats_response = requests.get(stats_url).json()
    
    for player in stats_response.get("data", []):
        # Ensure it's a skater
        if player.get("statsType") == "skater":
            cur.execute("""
                INSERT INTO player_stats (player_id, game_id, team_id, goals, assists, points, shots, hits, time_on_ice)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (player_id, game_id) DO UPDATE
                SET goals=%s, assists=%s, points=%s, shots=%s, hits=%s, time_on_ice=%s
            """, (
                player.get("playerId"),
                game_id,
                player.get("teamId"),
                player.get("goals", 0),
                player.get("assists", 0),
                player.get("points", 0),
                player.get("shots", 0),
                player.get("hits", 0),
                player.get("timeOnIce", "00:00"),
                player.get("goals", 0),
                player.get("assists", 0),
                player.get("points", 0),
                player.get("shots", 0),
                player.get("hits", 0),
                player.get("timeOnIce", "00:00"),
            ))

conn.commit()
print("Player stats updated successfully.")

# =======================
# Close connection
# =======================
cur.close()
conn.close()
print("Database update complete.")
