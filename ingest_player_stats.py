from db import get_conn
import requests

BOXSCORE_URL = "https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore"

# --------------------------
# Helper Functions
# --------------------------

def upsert_player(cur, name, team_id, position):
    """
    Insert a player if not exists. Update position if empty.
    """
    cur.execute("""
        INSERT INTO players (name, team_id, position)
        VALUES (%s, %s, %s)
        ON CONFLICT (name, team_id) DO UPDATE
        SET position = COALESCE(players.position, EXCLUDED.position)
        RETURNING id
    """, (name, team_id, position))
    return cur.fetchone()["id"]

# --------------------------
# Main Ingestion Function
# --------------------------

def ingest_player_stats():
    conn = get_conn()
    cur = conn.cursor()

    # Cache teams
    cur.execute("SELECT id, abbreviation FROM teams")
    team_cache = {row["abbreviation"]: row["id"] for row in cur.fetchall()}

    # Get all final games
    cur.execute("""
        SELECT id, nhl_game_id
        FROM games
        WHERE status = 'final'
    """)
    games = cur.fetchall()

    total_rows = 0

    for g in games:
        game_db_id = g["id"]
        nhl_game_id = g["nhl_game_id"]

        # Skip game if already has player stats
        cur.execute("SELECT 1 FROM player_stats WHERE game_id=%s LIMIT 1", (game_db_id,))
        if cur.fetchone():
            print(f"Skipping game {nhl_game_id}, already processed")
            continue

        try:
            url = BOXSCORE_URL.format(game_id=nhl_game_id)
            resp = requests.get(url)
            resp.raise_for_status()
            data = resp.json()

            pbg = data.get("playerByGameStats", {})
            if not pbg:
                print(f"Game {nhl_game_id} has no playerByGameStats, skipping")
                continue

            for side in ("homeTeam", "awayTeam"):
                team_stats = pbg.get(side)
                if not team_stats:
                    continue

                team_abbrev = data[side]["abbrev"]
                team_id = team_cache.get(team_abbrev)
                if not team_id:
                    print(f"Warning: team {team_abbrev} not found in DB")
                    continue

                # ---------- SKATERS ----------
                skaters = team_stats.get("forwards", []) + team_stats.get("defense", [])
                for p in skaters:
                    name = p["name"]["default"]
                    position = p.get("position")
                    player_id = upsert_player(cur, name, team_id, position)

                    cur.execute("""
                        INSERT INTO player_stats (
                            player_id, game_id, team_id,
                            goals, assists, points,
                            shots, hits, time_on_ice
                        )
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (player_id, game_id) DO UPDATE
                        SET
                            goals = EXCLUDED.goals,
                            assists = EXCLUDED.assists,
                            points = EXCLUDED.points,
                            shots = EXCLUDED.shots,
                            hits = EXCLUDED.hits,
                            time_on_ice = EXCLUDED.time_on_ice
                    """, (
                        player_id,
                        game_db_id,
                        team_id,
                        p.get("goals"),
                        p.get("assists"),
                        p.get("points"),
                        p.get("sog"),
                        p.get("hits"),
                        p.get("toi"),
                    ))
                    total_rows += 1

                # ---------- GOALIES ----------
                goalies = team_stats.get("goalies", [])
                for gk in goalies:
                    name = gk["name"]["default"]
                    position = "G"
                    player_id = upsert_player(cur, name, team_id, position)

                    cur.execute("""
                        INSERT INTO player_stats (
                            player_id, game_id, team_id,
                            goals, assists, points,
                            shots, hits, time_on_ice
                        )
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (player_id, game_id) DO UPDATE
                        SET
                            goals = EXCLUDED.goals,
                            shots = EXCLUDED.shots,
                            time_on_ice = EXCLUDED.time_on_ice
                    """, (
                        player_id,
                        game_db_id,
                        team_id,
                        gk.get("goalsAgainst"),
                        0,          # assists
                        0,          # points
                        gk.get("shotsAgainst"),
                        0,          # hits
                        gk.get("toi"),
                    ))
                    total_rows += 1

            # Commit after each game
            conn.commit()
            print(f"Ingested player stats for game {nhl_game_id}")

        except Exception as e:
            print(f"Error processing game {nhl_game_id}: {e}")
            conn.rollback()  # rollback only the current game

    cur.close()
    conn.close()
    print(f"Finished player stats ingestion: {total_rows} rows")

# --------------------------
# Entry Point
# --------------------------

if __name__ == "__main__":
    ingest_player_stats()
