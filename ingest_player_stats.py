from db import get_conn
import requests

BOXSCORE_URL = "https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore"

def upsert_player(cur, name, team_id, position):
    cur.execute("""
        INSERT INTO players (name, team_id, position)
        VALUES (%s, %s, %s)
        ON CONFLICT (name, team_id) DO UPDATE
        SET position = COALESCE(players.position, EXCLUDED.position)
        RETURNING id
    """, (name, team_id, position))
    return cur.fetchone()["id"]

def ingest_player_stats():
    conn = get_conn()
    cur = conn.cursor()

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

        url = f"https://api-web.nhle.com/v1/gamecenter/{nhl_game_id}/boxscore"
        resp = requests.get(url)
        resp.raise_for_status()
        data = resp.json()

        pbg = data.get("playerByGameStats", {})

        for side in ("homeTeam", "awayTeam"):
            team_stats = pbg.get(side)
            if not team_stats:
                continue

            team_abbrev = data[side]["abbrev"]

            # Lookup team_id
            cur.execute(
                "SELECT id FROM teams WHERE abbreviation=%s",
                (team_abbrev,)
            )
            team_id = cur.fetchone()["id"]

        # ---------- SKATERS ----------
        skaters = (
            team_stats.get("forwards", [])
            + team_stats.get("defense", [])
        )

        for p in skaters:
            name = p["name"]["default"]
            position = p.get("position")  # C, L, R, D
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

        for g in goalies:
            name = g["name"]["default"]
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
                g.get("goalsAgainst"),
                0,          # assists
                0,          # points
                g.get("shotsAgainst"),
                0,          # hits
                g.get("toi"),
            ))

            total_rows += 1


        print(f"Ingested player stats for game {nhl_game_id}")

    conn.commit()
    cur.close()
    conn.close()

    print(f"Finished player stats ingestion: {total_rows} rows")



if __name__ == "__main__":
    ingest_player_stats()
