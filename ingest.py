from nhl_api import get_schedule
from db import get_conn

def upsert_team(cur, name, abbreviation):
    """
    Insert team if it doesn't exist; otherwise update name.
    """
    cur.execute("""
        INSERT INTO teams (name, abbreviation)
        VALUES (%s, %s)
        ON CONFLICT (abbreviation) DO UPDATE
        SET name = EXCLUDED.name
        RETURNING id
    """, (name, abbreviation))
    return cur.fetchone()['id']

def ingest_games():
    schedule = get_schedule()
    conn = get_conn()
    cur = conn.cursor()
    
    # Cache abbreviation -> id mapping to reduce DB calls
    team_cache = {}

    inserted_games = 0

    for day in schedule.get("gameWeek", []):
        for game in day.get("games", []):
            # Only ingest final games
            if game.get("gameState") != "FINAL":
                continue

            nhl_game_id = game["id"]

            # Check if game already exists
            cur.execute("SELECT 1 FROM games WHERE nhl_game_id = %s", (nhl_game_id,))
            if cur.fetchone():
                continue  # already ingested

            # Upsert teams
            home_abbrev = game["homeTeam"]["abbrev"]
            away_abbrev = game["awayTeam"]["abbrev"]

            if home_abbrev in team_cache:
                home_team_id = team_cache[home_abbrev]
            else:
                home_team_id = upsert_team(cur, game["homeTeam"]["name"]["default"], home_abbrev)
                team_cache[home_abbrev] = home_team_id

            if away_abbrev in team_cache:
                away_team_id = team_cache[away_abbrev]
            else:
                away_team_id = upsert_team(cur, game["awayTeam"]["name"]["default"], away_abbrev)
                team_cache[away_abbrev] = away_team_id

            # Insert game
            cur.execute("""
                INSERT INTO games (
                    nhl_game_id, date, home_team_id, away_team_id,
                    home_score, away_score, status, season
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (nhl_game_id) DO NOTHING
            """, (
                nhl_game_id,
                game["startTimeUTC"],
                home_team_id,
                away_team_id,
                game["homeTeam"]["score"],
                game["awayTeam"]["score"],
                game["gameState"].lower(),
                game["season"]
            ))

            inserted_games += 1

    conn.commit()
    cur.close()
    conn.close()

    print(f"Inserted {inserted_games} new games.")


if __name__ == "__main__":
    ingest_games()
