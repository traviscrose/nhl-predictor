from db import get_conn
from nhl_api import get_schedule
from datetime import date, datetime

SEASON_START = "2024-10-10"  # adjust for the NHL season
SEASON_END = date.today().isoformat()  # or fixed end like "2025-04-13"

def upsert_team(cur, name, abbreviation):
    """
    Upsert a team; returns its ID.
    """
    cur.execute("""
        INSERT INTO teams (name, abbreviation)
        VALUES (%s, %s)
        ON CONFLICT (abbreviation) DO UPDATE
        SET name = EXCLUDED.name
        RETURNING id
    """, (name, abbreviation))
    return cur.fetchone()['id']

def ingest_season(start_date=SEASON_START, end_date=SEASON_END):
    """
    Backfill NHL games from start_date to end_date into Postgres.
    """
    schedule = get_schedule(start_date, end_date)
    conn = get_conn()
    cur = conn.cursor()
    team_cache = {}
    inserted_games = 0

    for day in schedule.get("dates", []):
        for game in day.get("games", []):
            if game.get("gameState") != "FINAL":
                continue

            nhl_game_id = game["id"]

            # Skip already ingested
            cur.execute("SELECT 1 FROM games WHERE nhl_game_id=%s", (nhl_game_id,))
            if cur.fetchone():
                continue

            # Upsert teams
            home = game["homeTeam"]
            away = game["awayTeam"]

            for t in [home, away]:
                abbrev = t["abbrev"]
                if abbrev not in team_cache:
                    team_cache[abbrev] = upsert_team(cur, t["name"], abbrev)

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
                game["season"]
            ))

            inserted_games += 1

    conn.commit()
    cur.close()
    conn.close()
    print(f"Inserted {inserted_games} new games from {start_date} to {end_date}")


if __name__ == "__main__":
    ingest_season()
