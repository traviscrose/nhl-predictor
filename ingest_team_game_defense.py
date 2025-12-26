import os
import requests
import logging
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# -----------------------------
# Load environment variables
# -----------------------------
load_dotenv()
DB_URI = os.getenv("DB_URI")
if not DB_URI:
    raise RuntimeError("DB_URI not set in .env")

engine = create_engine(DB_URI)

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# -----------------------------
# Config
# -----------------------------
BOXSCORE_URL = "https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore"

# -----------------------------
# Helper Functions
# -----------------------------
def ingest_defense(game_id: int, season: int):
    """
    Fetch defense stats for a single game and insert into DB.
    """
    try:
        r = requests.get(BOXSCORE_URL.format(game_id=game_id), timeout=10)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        logging.warning(f"Failed to fetch game {game_id}: {e}")
        return

    rows_to_insert = []

    for team_key in ["awayTeam", "homeTeam"]:
        team_data = data.get(team_key)
        if not team_data:
            continue
        team_id = team_data["id"]

        defense_players = team_data.get("defense", [])
        if not defense_players:
            logging.info(f"No defense players found for game {game_id}, team {team_id}")
            continue

        for p in defense_players:
            row = {
                "game_id": game_id,
                "season": season,
                "team_id": team_id,
                "player_id": p["playerId"],
                "name": p["name"]["default"],
                "position": p["position"],
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
                "toi": p.get("toi", None),
            }
            rows_to_insert.append(row)

    if not rows_to_insert:
        logging.info(f"No defense stats to insert for game {game_id}")
        return

    insert_sql = text("""
        INSERT INTO team_game_defense (
            game_id, season, team_id, player_id, name, position,
            goals, assists, points, plus_minus, pim,
            hits, blocked_shots, shifts, giveaways, takeaways, toi
        ) VALUES (
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
