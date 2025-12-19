from db import get_conn
import requests
from datetime import datetime

# Replace with your actual working endpoint
SCHEDULE_URL = "https://api-web.nhle.com/v1/schedule"  

def upsert_team(cur, name, abbreviation):
    """
    Insert or update a team and return its ID.
    """
    cur.execute("""
        INSERT INTO teams (name, abbreviation)
        VALUES (%s, %s)
        ON CONFLICT (abbreviation) DO UPDATE
        SET name = EXCLUDED.name
        RETURNING id
    """, (name, abbreviation))
    return cur.fetchone()['id']

def map_game_state(game_state):
    mapping = {
        "OFF": "scheduled",
        "LIVE": "live",
        "Final": "final"
    }
    return mapping.get(game_state, "scheduled")

def ingest_schedule(start_date, end_date):
    """
    Ingests games from the NHL schedule API that returns `gameWeek`.
    Inserts teams and games into Postgres, including scheduled games.
    """
    conn = get_conn()
    cur = conn.cursor()
    team_cache = {}
    total_inserted = 0
    current_date = start_date

    while current_date <= end_date:
        url = f"{SCHEDULE_URL}/{current_date}"
        resp = requests.get(url)
        if resp.status_code == 404:
            print(f"No data for {current_date}")
            break
        resp.raise_for_status()
        schedule = resp.json()

        game_weeks = schedule.get("gameWeek", [])
        if not game_weeks:
            print(f"No games in gameWeek for {current_date}")
        for day in game_weeks:
            for game in day.get("games", []):
                nhl_game_id = game["id"]
                raw_state = game.get("gameState", "OFF")
                status = map_game_state(raw_state)

                # Determine scores: only if Final
                home_score = game["homeTeam"].get("score") if raw_state == "Final" else None
                away_score = game["awayTeam"].get("score") if raw_state == "Final" else None

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
                    continue

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
                    datetime.strptime(game["startTimeUTC"], "%Y-%m-%dT%H:%M:%SZ"),
                    home_team_id,
                    away_team_id,
                    home_score,
                    away_score,
                    status,
                    game["season"]
                ))

                print(f"Inserted game {nhl_game_id}: {game['homeTeam']['abbrev']} vs {game['awayTeam']['abbrev']} ({game_state})")
                total_inserted += 1

        # Move to nextStartDate for next iteration
        next_date = schedule.get("nextStartDate")
        if not next_date or next_date > end_date:
            break
        current_date = next_date

    conn.commit()
    cur.close()
    conn.close()
    print(f"Finished ingestion: inserted {total_inserted} new games.")

if __name__ == "__main__":
    # Example: ingest from Oct 7, 2025 to Dec 19, 2025
    ingest_schedule("2025-10-07", "2025-12-19")
