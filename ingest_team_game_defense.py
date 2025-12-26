import os
import requests
import json
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# ----------------------------
# Load environment variables
# ----------------------------
load_dotenv()
DB_URI = os.getenv("DB_URI")
if not DB_URI:
    raise RuntimeError("DB_URI not found in .env")

engine = create_engine(DB_URI)

# ----------------------------
# Config
# ----------------------------
BOXSCORE_URL = "https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore"

# ----------------------------
# Helper Functions
# ----------------------------
def upsert_defense(cur, game_id, season, team_id, player):
    """
    Insert or update a defense player's stats for a game.
    """
    stmt = text("""
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
            blocked_shots = EXCLUDED.blocked_shots,
            shifts = EXCLUDED.shifts,
            giveaways = EXCLUDED.giveaways,
            takeaways = EXCLUDED.takeaways,
            toi = EXCLUDED.toi
    """)

    cur.execute(stmt, {
        "game_id": game_id,
        "season": season,
        "team_id": tea_
