from db import get_conn
import requests
from datetime import datetime

BASE_URL = "https://api-web.nhle.com/v1/schedule"
# Replace with the range of dates you want to pull
START_DATE = "2025-10-07"
END_DATE = "2025-10-14"

def upsert_team(cur, name, abbreviation):
    cur.execute("""
        INSERT INTO teams (name, abbreviation)
        VALUES (%s, %s)
        ON CONFLICT (abbreviation) DO UPDATE
        SET name = EXCLUDED.name
        RETURNING id
    """, (name, abbreviation))
    return cur.fetchone()['id']

def ingest_schedule(start_date, end_date):
    conn = get_conn()
    cur = conn.cursor()
    team_cache = {}
    total_inserted = 0

    url = f"{BASE_URL}?startDate={start_date}&endDate={end_date}"
    resp = requests.get(url)
    resp.raise_for_status()
    schedule = resp.json()

    for day in schedule.get("gameWeek", []):
        for game in day.get("games", []):
            nhl_game_id = game["id"]
            status = game["gameState"]

            if status != "Final":
                continue

            # Upsert teams
            for t in [game["homeTeam"], game["awayTeam"]]:
                abbrev = t["abbrev"]
                name = t["commonName"]["default"]
                if abbrev not in team_cache:
                    team_cache[abbrev] = upsert_team(cur, name, abbrev)

            home_team_id = team_cache[game["homeTeam"]["abbrev"]]
            away_team_id = team_cache[game["awayTeam"]["abbrev"]]

            # Skip duplicates
            cur.execute("SELECT 1 FROM games WHERE nhl_game_id=%s", (nhl_game_id,))
            if cur.fetchone():
                print(f"Skipping already ingested game {nhl_game_id}")
                continue

            cur.execute("""
                INSERT INTO games (
                    nhl_game_id, date, home_team_id, away_team_id,
                    home_score, away_score, status, season
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (nhl_game_id) DO NOTHING
            """, (
                nhl_game_id,
                datetime.strptime(game["startTimeUTC"], "%Y-%m-%dT%H:%M:%SZ"),
                home_team_id,
                away_team_id,
                game["homeTeam"]["score"],
                game["awayTeam"]["score"],
                status.lower(),
                game["season"]
            ))

            print(f"Inserted game {nhl_game_id}: {game['homeTeam']['abbrev']} vs {game['awayTeam']['abbrev']}")
            total_inserted += 1

    conn.commit()
    cur.close()
    conn.close()
    print(f"Backfill finished: inserted {total_inserted} new games.")

if __name__ == "__main__":
    ingest_schedule(START_DATE, END_DATE)
