from db import get_conn
from nhl_api import get_schedule_for_date
from datetime import date, datetime, timedelta

SEASON_START = date(2025, 10, 7)
SEASON_END = date.today()  # Or the fixed final date for the season

def daterange(start: date, end: date):
    """
    Yield each date from start to end inclusive.
    """
    curr = start
    while curr <= end:
        yield curr
        curr += timedelta(days=1)

def upsert_team(cur, name, abbreviation):
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
            # No schedule for this date? Just skip
            print(f"Skipping {date_str}: {e}")
            continue

        # NHL schedule returns a list of gameWeek objects sometimes;
        # for date-specific endpoints you’ll see a “dates” array instead
        games_data = schedule.get("dates", [])  # usually an array
        if not games_data:
            # Fallback if API returns gameWeek instead
            games_data = [{"games": schedule.get("gameWeek", [])}]

        for day in games_data:
            for game in day.get("games", []):
                if game.get("gameState") != "FINAL":
                    continue

                nhl_game_id = game["id"]

                # Skip duplicates
                cur.execute("SELECT 1 FROM games WHERE nhl_game_id=%s", (nhl_game_id,))
                if cur.fetchone():
                    continue

                # Upsert teams
                home = game["homeTeam"]
                away = game["awayTeam"]

                for t in (home, away):
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

                total_inserted += 1

        # Save after each date to avoid large uncommitted batches
        conn.commit()

    cur.close()
    conn.close()
    print(f"Backfill finished: inserted {total_inserted} new games.")
