from db import get_conn
import requests
from datetime import datetime

# Replace with your actual working endpoint
SCHEDULE_URL = "https://api-web.nhle.com/v1/schedule"

# --------------------------
# Helper Functions
# --------------------------

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

def upsert_season(cur, season_code):
    """
    Insert or fetch a season and return its ID.
    """
    season_str = str(season_code)
    start_year = int(season_str[:4])
    end_year = int(season_str[4:])
    start_date = f"{start_year}-10-01"
    end_date = f"{end_year}-06-30"
    
    cur.execute("""
        INSERT INTO seasons (season_code, start_date, end_date)
        VALUES (%s, %s, %s)
        ON CONFLICT (season_code) DO UPDATE
        SET season_code = EXCLUDED.season_code
        RETURNING id
    """, (season_code, start_date, end_date))
    return cur.fetchone()['id']

def map_game_state(game_state):
    """
    Map NHL API game state to our database status.
    """
    return {
        "OFF": "scheduled",
        "LIVE": "live",
        "Final": "final"
    }.get(game_state, "scheduled")

# --------------------------
# Main Ingestion Function
# --------------------------

def ingest_schedule(start_date, end_date):
    """
    Ingest NHL games from the API into local Postgres database.
    Smart ingestion:
      - Skips games that are already final
      - Updates games only if status or score changed
      - Inserts new games
    """
    conn = get_conn()
    cur = conn.cursor()
    
    team_cache = {}
    season_cache = {}
    total_inserted = 0
    total_updated = 0
    current_date = start_date

    while current_date <= end_date:
        url = f"{SCHEDULE_URL}/{current_date}"
        resp = requests.get(url)

        if resp.status_code == 404:
            print(f"No data for {current_date}")
            break

        resp.raise_for_status()
        schedule = resp.json()

        for day in schedule.get("gameWeek", []):
            for game in day.get("games", []):
                nhl_game_id = game["id"]
                raw_state = game.get("gameState", "OFF")

                home_score = game["homeTeam"].get("score")
                away_score = game["awayTeam"].get("score")

                status = map_game_state(raw_state)
                if home_score is not None and away_score is not None:
                    status = "final"

                game_date = datetime.strptime(
                    game["startTimeUTC"], "%Y-%m-%dT%H:%M:%SZ"
                )
                
                venue_obj = game.get("venue")
                venue = venue_obj.get("default") if isinstance(venue_obj, dict) else None
                
                game_type = game.get("gameType")

                # --- Upsert Teams ---
                for t in (game["homeTeam"], game["awayTeam"]):
                    abbrev = t["abbrev"]
                    name = t["commonName"]["default"]
                    if abbrev not in team_cache:
                        team_cache[abbrev] = upsert_team(cur, name, abbrev)

                home_team_id = team_cache[game["homeTeam"]["abbrev"]]
                away_team_id = team_cache[game["awayTeam"]["abbrev"]]

                # --- Upsert Season ---
                season_code = game.get("season")
                if season_code not in season_cache:
                    season_cache[season_code] = upsert_season(cur, season_code)
                season = season_cache[season_code]

                # --- Smart Upsert Game ---
                cur.execute("""
                    SELECT status, home_score, away_score
                    FROM games
                    WHERE nhl_game_id = %s
                """, (nhl_game_id,))
                existing_game = cur.fetchone()

                if existing_game:
                    if existing_game['status'] == 'final':
                        print(f"Skipping final game {nhl_game_id}")
                        continue

                    # Only update if status or score has changed
                    if status != existing_game['status'] or \
                       (status == 'final' and (home_score != existing_game['home_score'] or away_score != existing_game['away_score'])):
                        cur.execute("""
                            UPDATE games
                            SET
                                status = %s,
                                home_score = CASE WHEN %s = 'final' THEN %s ELSE home_score END,
                                away_score = CASE WHEN %s = 'final' THEN %s ELSE away_score END,
                                season = %s,
                                venue = %s,
                                game_type = %s
                            WHERE nhl_game_id = %s
                        """, (
                            status,
                            status, home_score,
                            status, away_score,
                            season,
                            venue,
                            game_type,
                            nhl_game_id
                        ))
                        total_updated += 1
                        print(f"Updated game {nhl_game_id}: status changed to {status}")
                    else:
                        print(f"No update needed for game {nhl_game_id}")
                    continue  # skip to next game

                # Insert new game if it doesn’t exist
                cur.execute("""
                    INSERT INTO games (
                        nhl_game_id,
                        season,
                        game_date,
                        home_team_id,
                        away_team_id,
                        home_score,
                        away_score,
                        status,
                        venue,
                        game_type
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    nhl_game_id,
                    season,
                    game_date,
                    home_team_id,
                    away_team_id,
                    home_score,
                    away_score,
                    status,
                    venue,
                    game_type
                ))
                total_inserted += 1
                print(
                    f"Inserted new game {nhl_game_id}: "
                    f"{game['homeTeam']['abbrev']} vs {game['awayTeam']['abbrev']} "
                    f"({status}) season={season_code}"
                )

        next_date = schedule.get("nextStartDate")
        if not next_date or next_date > end_date:
            break
        current_date = next_date

    conn.commit()
    cur.close()
    conn.close()
    print(f"Finished ingestion: {total_inserted} inserted, {total_updated} updated.")


# --------------------------
# Entry Point
# --------------------------

if __name__ == "__main__":
    # Example ingestion: Oct 1, 2021 → Dec 19, 2025
    ingest_schedule("2021-10-01", "2025-12-19")
