import requests
from datetime import datetime
from db import get_conn

BASE_URL = "https://api-web.nhle.com/v1/schedule"

def ingest_season(start_date, end_date):
    conn = get_conn()
    cur = conn.cursor()
    team_cache = {}
    total_inserted = 0
    current_date = start_date

    while current_date <= end_date:
        url = f"{BASE_URL}?startDate={current_date}"
        resp = requests.get(url)
        if resp.status_code == 404:
            print(f"No data for {current_date}")
            break
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
                        cur.execute("""
                            INSERT INTO teams (name, abbreviation)
                            VALUES (%s,%s)
                            ON CONFLICT (abbreviation) DO UPDATE
                            SET name = EXCLUDED.name
                            RETURNING id
                        """, (name, abbrev))
                        team_cache[abbrev] = cur.fetchone()['id']

                home_team_id = team_cache[game["homeTeam"]["abbrev"]]
                away_team_id = team_cache[game["awayTeam"]["abbrev"]]

                # Skip duplicates
                cur.execute("SELECT 1 FROM games WHERE nhl_game_id=%s", (nhl_game_id,))
                if cur.fetchone():
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
                total_inserted += 1

        conn.commit()

        # Advance to next date returned by the API
        next_date = schedule.get("nextStartDate")
        if not next_date or next_date > end_date:
            break
        current_date = next_date

    cur.close()
    conn.close()
    print(f"Inserted {total_inserted} new games.")

# Example usage
if __name__ == "__main__":
    ingest_season("2025-10-07", "2025-12-19")
