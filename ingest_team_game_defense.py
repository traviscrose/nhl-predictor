import time
import requests
from db import get_conn, engine
import pandas as pd

# -------------------------------
# Config
# -------------------------------

BOXSCORE_URL = "https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore"

# -------------------------------
# Helper functions
# -------------------------------

def ingest_defense(nhl_game_id, season):
    """Ingest defense stats for a single game."""
    try:
        r = requests.get(BOXSCORE_URL.format(game_id=nhl_game_id), timeout=10)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"Failed to fetch game {nhl_game_id}: {e}")
        return False

    if "playerByGameStats" not in data:
        print(f"Game {nhl_game_id} missing playerByGameStats, skipping")
        return False

    for side in ["homeTeam", "awayTeam"]:
        try:
            team_stats = data["playerByGameStats"][side]
            defense_players = team_stats.get("defense", [])
            goalies = team_stats.get("goalies", [])

            # Aggregate team-level defense stats
            goals_against = sum(g.get("goalsAgainst", 0) for g in goalies)
            shots_against = sum(g.get("shotsAgainst", 0) for g in goalies)

            team_id = data[side]["id"]
            opponent_side = "homeTeam" if side == "awayTeam" else "awayTeam"
            opponent_team_id = data[opponent_side]["id"]
            is_home = side == "homeTeam"

            conn = get_conn()
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO team_game_defense (
                    game_id, team_id, opponent_team_id, season, is_home,
                    goals_against, shots_against
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (game_id, team_id)
                DO UPDATE SET
                    goals_against = EXCLUDED.goals_against,
                    shots_against = EXCLUDED.shots_against
            """, (nhl_game_id, team_id, opponent_team_id, season, is_home,
                  goals_against, shots_against))
            conn.commit()
        except Exception as e:
            print(f"Failed to ingest team {side} in game {nhl_game_id}: {e}")
        finally:
            cur.close()
            conn.close()

    time.sleep(0.1)
    return True

def ingest_all_games():
    """Ingest all games from the `games` table."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT nhl_game_id, season FROM games ORDER BY nhl_game_id")
    games = cur.fetchall()
    cur.close()
    conn.close()

    total = len(games)
    print(f"Found {total} games to ingest")

    success_count = 0
    fail_count = 0

    for idx, (nhl_game_id, season) in enumerate(games, start=1):
        try:
            if ingest_defense(nhl_game_id, season):
                success_count += 1
            else:
                fail_count += 1
        except KeyboardInterrupt:
            print("\nIngestion interrupted by user")
            break
        except Exception as e:
            print(f"Unexpected error for game {nhl_game_id}: {e}")
            fail_count += 1

        if idx % 50 == 0:
            print(f"Ingested {idx}/{total} games so far...")

    print(f"\nIngestion complete. Success: {success_count}, Failed: {fail_count}")


if __name__ == "__main__":
    ingest_all_games()
 