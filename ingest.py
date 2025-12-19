from db import get_conn
import requests
from datetime import date, timedelta, datetime

BASE_URL = "https://api-web.nhle.com/v1"
SEASON_START = date(2025, 10, 7)
SEASON_END = date.today()  # or fixed season end

def daterange(start: date, end: date):
    """Yield each date from start to end inclusive."""
    curr = start
    while curr <= end:
        yield curr
        curr += timedelta(days=1)

def get_schedule_for_date(date_str: str):
    """Fetch NHL schedule for a specific YYYY-MM-DD date."""
    url = f"{BASE_URL}/schedule/{date_str}"
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    return r.json()

def upsert_team(cur, name, abbreviation):
    """Upsert a team and return its ID."""
    cur.execute("""
        INSERT INTO teams (name, abbreviation)
        VALUES (%s, %s)
        ON CONFLICT (abbreviation) DO UPDATE
        SET name = EXCLUDED.name
        RETURNING id
    """, (name, abbreviation))
    return cur.fetchone()['id']

def ingest_backfill():
    conn = get_conn()
    cur = conn.cursor()
    team_cache = {}
    total_inserted = 0

    for single_date in daterange(SEASON_START, SEASON_END):
        date_str = single_date.isoformat()
        try:
            schedule = get_schedule_for_date(date_str)
        except requests.exceptions.HTTPError as e:
            print(f"Skipping {date_str}: HTTPError {e}")
            continue

        # Handle both 'dates' and deprecated 'gameWeek'
        games_data = schedule.get("dates", [])
        if not games_data and "gameWeek" in schedule:
            games_data = [{"games": schedule["gameWeek"]}]

        if not games_data:
            print(f"No games found for {date_str}")
            continue

        for day in games_data:
            for game in day.get("games", []):
                if game.get("gameState") != "FINAL":
                    continue  # skip non-final games

                nhl_game_id = game["id"]

                # Skip if already ingested
                cur.execute("SELECT 1 FROM games WHERE nhl_game_id=%s", (nhl_game_id,))
                if cur.fetchone():
                    print(f"Skipping already ingested game {nhl_game_id}")
                    continue

                # Upsert teams
                home = game["homeTeam"]
                away = game["awayTeam"]

                for t in [home, away]:
                    abbr = t["abbrev"]
                    if abbr not in team_cache:
                        team_cache[abbr] = upsert_team(cur, t["name"], abbr)

                home_team_id = team_cache[home["abbrev"]]
                away_team_id = team_cache[away["abbrev"]]

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
                    home["score"],
                    away["score"],
                    game["gameState"].lower(),
                    game["season"],
                ))

                print(f"Inserted game {nhl_game_id}: {home['abbrev']} vs {away['abbrev']}, {home['score']}-{away['score']}")
                total_inserted += 1

        # Commit after each day
        conn.commit()

    cur.close()
    conn.close()
    print(f"Backfill finished: inserted {total_inserted} new games.")

if __name__ == "__main__":
    ingest_backfill()
