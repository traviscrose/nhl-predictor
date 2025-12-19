from db import get_conn
import requests
from datetime import date, timedelta, datetime

BASE_URL = "https://api-web.nhle.com/v1"
SEASON_START = date(2024, 10, 10)
SEASON_END = date.today()  # or fixed season end

def daterange(start: date, end: date):
    curr = start
    while curr <= end:
        yield curr
        curr += timedelta(days=1)

def get_schedule_for_date(date_str: str):
    url = f"{BASE_URL}/schedule/{date_str}"
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

def ingest_season():
    conn = get_conn()
    cur = conn.cursor()
    team_cache = {}
    inserted_games = 0

    for single_date in daterange(SEASON_START, SEASON_END):
        date_str = single_date.isoformat()
        try:
            schedule = get_schedule_for_date(date_str)
        except requests.exceptions.HTTPError as e:
            print(f"Skipping {date_str}: HTTPError {e}")
            continue

        # The games for that date
        dates_list = schedule.get("data", {}).get("schedule", {}).get("dates", [])
        if not dates_list:
            print(f"No games found for {date_str}")
            continue

        for day in dates_list:
            for game in day.get("games", []):
                nhl_game_id = game["id"]
                status = game.get("status", {}).get("detailedState")

                if status != "Final":
                    continue  # only insert final games

                home_team_info = game["teams"]["home"]["team"]
                away_team_info = game["teams"]["away"]["team"]
                home_score = game["teams"]["home"]["score"]
                away_score = game["teams"]["away"]["score"]

                # Upsert teams
                for t in [(home_team_info["name"], home_team_info["abbreviation"]),
                          (away_team_info["name"], away_team_info["abbreviation"])]:
                    name, abbr = t
                    if abbr not in team_cache:
                        team_cache[abbr] = upsert_team(cur, name, abbr)

                home_team_id = team_cache[home_team_info["abbreviation"]]
                away_team_id = team_cache[away_team_info["abbreviation"]]

                # Skip duplicates
                cur.execute("SELECT 1 FROM games WHERE nhl_game_id=%s", (nhl_game_id,))
                if cur.fetchone():
                    print(f"Skipping already ingested game {nhl_game_id}")
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
                    datetime.strptime(game["gameDate"], "%Y-%m-%dT%H:%M:%SZ"),
                    home_team_id,
                    away_team_id,
                    home_score,
                    away_score,
                    status.lower(),
                    game.get("season")
                ))

                print(f"Inserted game {nhl_game_id}: {home_team_info['abbreviation']} vs {away_team_info['abbreviation']}, {home_score}-{away_score}")
                inserted_games += 1

        # Commit per day
        conn.commit()

    cur.close()
    conn.close()
    print(f"Finished ingestion: inserted {inserted_games} new games.")

if __name__ == "__main__":
    ingest_season()
