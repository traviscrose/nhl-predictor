import os
import time
import requests
import logging
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
# Logging configuration
# ----------------------------
LOG_FILE = "ingest_defense.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ----------------------------
# Config
# ----------------------------
BOXSCORE_URL = "https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore"
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds

# ----------------------------
# Helper Functions
# ----------------------------
def upsert_defense(cur, game_id, season, team_id, player):
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
        "team_id": team_id,
        "player_id": player.get("playerId"),
        "name": player.get("name", {}).get("default"),
        "position": player.get("position"),
        "goals": player.get("goals", 0),
        "assists": player.get("assists", 0),
        "points": player.get("points", 0),
        "plus_minus": player.get("plusMinus", 0),
        "pim": player.get("pim", 0),
        "hits": player.get("hits", 0),
        "blocked_shots": player.get("blockedShots", 0),
        "shifts": player.get("shifts", 0),
        "giveaways": player.get("giveaways", 0),
        "takeaways": player.get("takeaways", 0),
        "toi": player.get("toi"),
    })

def fetch_game_json(game_id):
    """
    Fetch game JSON from NHL API with retries.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(BOXSCORE_URL.format(game_id=game_id))
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            logger.warning(f"[Attempt {attempt}] Failed to fetch game {game_id}: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
            else:
                logger.error(f"Skipping game {game_id} after {MAX_RETRIES} failed attempts.")
                return None

def ingest_defense(game_id, season):
    """
    Fetch a game boxscore and ingest defense stats for both teams.
    """
    data = fetch_game_json(game_id)
    if not data or "playerByGameStats" not in data:
        logger.warning(f"No player stats found for game {game_id}")
        return

    defense_players = []

    # Away team
    away_def = data.get("playerByGameStats", {}).get("awayTeam", {}).get("defense", [])
    away_team_id = data.get("awayTeam", {}).get("id")
    for player in away_def:
        defense_players.append((player, away_team_id))

    # Home team
    home_def = data.get("playerByGameStats", {}).get("homeTeam", {}).get("defense", [])
    home_team_id = data.get("homeTeam", {}).get("id")
    for player in home_def:
        defense_players.append((player, home_team_id))

    if not defense_players:
        logger.info(f"No defense stats to insert for game {game_id}")
        return

    try:
        with engine.begin() as conn:
            for player, team_id in defense_players:
                upsert_defense(conn, game_id, season, team_id, player)
        logger.info(f"Successfully ingested defense stats for game {game_id}")
    except Exception as e:
        logger.error(f"Failed to ingest defense stats for game {game_id}: {e}")

def ingest_all_games():
    """
    Loop through all games in the games table and ingest defense stats.
    Skips games that already have defense stats.
    """
    with engine.connect() as conn:
        games = conn.execute(text("""
            SELECT nhl_game_id, season
            FROM games g
            LEFT JOIN team_game_defense d
            ON g.nhl_game_id = d.game_id
            GROUP BY g.nhl_game_id, g.season
            HAVING COUNT(d.game_id) = 0
            ORDER BY g.nhl_game_id
        """)).fetchall()

    logger.info(f"Found {len(games)} games to ingest")

    for idx, game in enumerate(games, start=1):
        game_id = game["nhl_game_id"]
        season = game["season"]
        logger.info(f"[{idx}/{len(games)}] Ingesting game {game_id} (season {season})")
        ingest_defense(game_id, season)

if __name__ == "__main__":
    ingest_all_games()
    logger.info("Done.")
