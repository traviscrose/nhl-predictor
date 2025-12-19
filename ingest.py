from db import get_conn
import requests
from datetime import date, datetime

BASE_URL = "https://statsapi.web.nhl.com/api/v1"

SEASON_START = "2025-10-07"
SEASON_END = date.today().isoformat()  # or "2025-04-13" for full season

def get_schedule(start_date, end_date):
    url = f"{BASE_URL}/schedule?startDate={start_date}&endDate={end_date}"
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    return r.json()

def upsert_team(cur, name, abbreviation):
    cur.execute("""
        INSERT INTO teams (name, abbreviation)
        VALUES (%s, %s)
        ON CONFLICT (abbreviation) DO UPDATE
        SET name = EXCLUDED.name
        RETURNING id
    """, (name, abbreviation))
    return cur.fetchone()['id']

def ingest_season(start_date=SEASON_START, end_date=SEASON_END):
    schedule_data = get_schedule(start_date, end_date)
    conn = get_conn()
    cur = conn.cursor()

    team_cache = {}
    inserted_games = 0

    for date_block in schedule_data.get("dates", []):
        for game in date_block.get("games", []):
            if game["status"]["detailedState"] != "Final":
                continue

            nhl_game_id = game["gamePk"]

            # Skip if already ingested
            cur.execute("SELECT 1 FROM games WHERE nhl_game_id=%s", (nhl_game_id,))
            if cur.fetchone():
                continue

            # Teams
            home = game["teams"]["home"]
            away = game["teams"]["away"]

            for team in [home, away]:
                abbrev = team["team"]["abbreviation"]
                if abbrev not in team_cache:
                    team_cache[abbrev] = upsert_team(cur, team["team"]["name"], abbrev)

            home_team_id = team_cache[home["team"]["abbreviation"]]
            away_team_id = team_cache[away["team"]["abbreviation"]]

            # Insert game
            cur.execute("""
                INSERT INTO games (
                    nhl_game_id, date, home_team_id, away_team_id,
                    home_score, away_score, status, season
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (nhl_game_id) DO NOTHING
            """, (
                nhl_game_id,
                datetime.strptime(game["gameDate"], "%Y-%m-%dT%H:%M:%SZ"),
                home_team_id,
                away_team_id,
                home["score"],
                away["score"],
                game["status"]["detailedState"].lower(),
                game["season"]
            ))

            inserted_games += 1

    conn.commit()
    cur.close()
    conn.close()

    print(f"Inserted {inserted_games} new games from {start_date} to {end_date}")

if __name__ == "__main__":
    ingest_season()
