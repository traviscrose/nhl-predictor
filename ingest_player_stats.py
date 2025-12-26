import os
import requests
import logging
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

# Load .env variables
load_dotenv()
DB_URI = os.getenv("DB_URI")

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# API URLs
SCHEDULE_URL = "https://api-web.nhle.com/v1/schedule"
BOXSCORE_URL = "https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore"

# Database engine
engine = create_engine(DB_URI, echo=False)

def get_games_to_ingest():
    """Fetch all games from NHL schedule that are FINAL and not already in the database."""
    try:
        response = requests.get(SCHEDULE_URL)
        response.raise_for_status()
        schedule = response.json()
    except requests.RequestException as e:
        logging.error(f"Failed to fetch schedule: {e}")
        return []

    games_to_ingest = []

    with engine.connect() as conn:
        for date_info in schedule.get("dates", []):
            for game in date_info.get("games", []):
                nhl_game_id = game["id"]
                season = game["season"]
                # Check if this game already exists in team_game_defense
                existing = conn.execute(
                    text("SELECT 1 FROM team_game_defense WHERE game_id = :game_id LIMIT 1"),
                    {"game_id": nhl_game_id}
                ).first()
                if not existing:
                    games_to_ingest.append({
                        "nhl_game_id": nhl_game_id,
                        "season": season
                    })

    logging.info(f"Found {len(games_to_ingest)} games to ingest")
    return games_to_ingest

def fetch_boxscore(game_id):
    """Fetch boxscore data for a specific game."""
    try:
        response = requests.get(BOXSCORE_URL.format(game_id=game_id))
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logging.error(f"Failed to fetch boxscore for game {game_id}: {e}")
        return None

def insert_defense_stats(conn, game_id, season, team_id, defense_players):
    """Insert or update defense player stats for a team in a game."""
    if not defense_players:
        logging.info(f"No defense players found for game {game_id}, team {team_id}")
        return

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

    for player in defense_players:
        params = {
            "game_id": game_id,
            "season": season,
            "team_id": team_id,
            "player_id": player["playerId"],
            "name": player["name"]["default"],
            "position": player["position"],
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
            "toi": player.get("toi", None)
        }
        conn.execute(stmt, params)

def ingest_all_games():
    games = get_games_to_ingest()
    if not games:
        return

    for game in games:
        game_id = game["nhl_game_id"]
        season = game["season"]
        logging.info(f"Ingesting game {game_id}")

        boxscore = fetch_boxscore(game_id)
        if not boxscore:
            logging.warning(f"Skipping game {game_id} due to fetch failure")
            continue

        try:
            with engine.begin() as conn:  # transaction per game
                for team_key in ["homeTeam", "awayTeam"]:
                    team_info = boxscore.get(team_key)
                    if not team_info:
                        continue
                    team_id = team_info["id"]
                    defense_players = boxscore.get("playerByGameStats", {}).get(team_key, {}).get("defense", [])
                    if not defense_players:
                        logging.info(f"No defense stats to insert for game {game_id}, team {team_id}")
                        continue
                    insert_defense_stats(conn, game_id, season, team_id, defense_players)
        except SQLAlchemyError as e:
            logging.error(f"Failed to ingest game {game_id}: {e}")

if __name__ == "__main__":
    ingest_all_games()
