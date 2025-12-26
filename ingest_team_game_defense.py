import os
import requests
import json
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# -------------------------------------------------
# Load environment variables
# -------------------------------------------------
load_dotenv()
DB_URI = os.getenv("DB_URI")
if not DB_URI:
    raise RuntimeError("DB_URI not set in .env")

engine = create_engine(DB_URI)

BOXSCORE_URL = "https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore"

# -------------------------------------------------
# Fetch all games from the database
# -------------------------------------------------
def get_all_games():
    query = "SELECT nhl_game_id, season FROM games ORDER BY season, id"
    with engine.connect() as conn:
        return conn.execute(text(query)).fetchall()

# -------------------------------------------------
# Ingest defense stats for a single game
# -------------------------------------------------
def ingest_defense(game_id, season):
    try:
        r = requests.get(BOXSCORE_URL.format(game_id=game_id), timeout=10)
        r.raise_for_status()
        data = r.json()
    except (requests.RequestException, json.JSONDecodeError) as e:
        print(f"Failed to fetch game {game_id}: {e}")
        return

    teams_data = []
    for side in ["awayTeam", "homeTeam"]:
        team_info = data.get(side)
        if not team_info:
            print(f"Game {game_id} missing {side} data")
            continue
        team_id = team_info["id"]

        defense_players = team_info.get("playerByGameStats", {}).get("defense", [])
        if not defense_players:
            print(f"No defense players found for game {game_id}, team {team_id}")
            continue

        for p in defense_players:
            player_stats = {
                "game_id": game_id,
                "season": season,
                "team_id": team_id,
                "player_id": p["playerId"],
                "name": p["name"]["default"],
                "position": p.get("position"),
                "goals": p.get("goals", 0),
                "assists": p.get("assists", 0),
                "points": p.get("points", 0),
                "plus_minus": p.get("plusMinus", 0),
                "pim": p.get("pim", 0),
                "hits": p.get("hits", 0),
                "blocked_shots": p.get("blockedShots", 0),
                "shifts": p.get("shifts", 0),
                "giveaways": p.get("giveaways", 0),
                "takeaways": p.get("takeaways", 0),
                "toi": p.get("toi", "0:00"),
            }
            teams_data.append(player_stats)

    if not teams_data:
        print(f"No defense stats to insert for game {game_id}")
        return

    # -------------------------------------------------
    # Insert/update into database
    # -------------------------------------------------
    insert_sql = text("""
        INSERT INTO team_game_defense (
            game_id, season, team_id, player_id, name, position,
            goals, assists, points, plus_minus, pim,
            hits, blocked_shots, shifts, giveaways, takeaways, toi
        )
        VALUES (
            :game_id, :season, :team_id, :player_id, :name, :position,
            :goals, :assists, :points, :plus_minus, :pim,
            :hits, :blocked_shots, :shifts, :giveaways, :takeaways, :toi
        )
        ON CONFLICT (game_id, player_id) DO UPDATE SET
            goals = EXCLUDED.goals,
            assists = EXCLUDED.assists,
            points = EXCLUDED.points,
            plus_minus = EXCLUDED.plus_minus,
            pim = EXCLUDED.pim,
            hits = EXCLUDED.hits,
            blocked_shots = EXCLUDED.blocked_shots,
            shifts = EXCLUDED.shifts,
            giveaways = EXCLUDED.giveaways,
            takeaways = EXCLUDED.takeaways,
            toi = EXCLUDED.toi
    """)

    with engine.begin() as conn:  # begin() ensures transaction is committed
        for row in teams_data:
            conn.execute(insert_sql, **row)

    print(f"Successfully ingested game {game_id}")

# -------------------------------------------------
# Main ingestion loop
# -------------------------------------------------
def ingest_all_games():
    games = get_all_games()
    print(f"Found {len(games)} games to ingest")

    for game_id, season in games:
        ingest_defense(game_id, season)

if __name__ == "__main__":
    ingest_all_games()
