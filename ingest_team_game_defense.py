import os
import requests
import logging
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime, timedelta

# -----------------------------
# Load environment variables
# -----------------------------
load_dotenv()
DB_URI = os.getenv("DB_URI")
if not DB_URI:
    raise ValueError("DB_URI must be set in the .env file")

engine = create_engine(DB_URI, future=True)

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# -----------------------------
# NHL API URLs
# -----------------------------
SCHEDULE_URL = "https://api-web.nhle.com/v1/schedule/{date}"
BOXSCORE_URL = "https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore"

# -----------------------------
# Helper functions
# -----------------------------
def fetch_schedule(date_str):
    url = SCHEDULE_URL.format(date=date_str)
    r = requests.get(url)
    r.raise_for_status()
    return r.json()

def fetch_boxscore(game_id):
    url = BOXSCORE_URL.format(game_id=game_id)
    r = requests.get(url)
    r.raise_for_status()
    return r.json()

def upsert_defense_stats(conn, game_id, season, team_id, defense_players):
    sql = text("""
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
        conn.execute(sql, {
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
            "toi": player.get("toi")
        })

# -----------------------------
# Main ingestion loop
# -----------------------------
def ingest_all_games(start_date="2025-10-01", end_date=None):
    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")

    current_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date_dt = datetime.strptime(end_date, "%Y-%m-%d")

    games_to_ingest = []

    # Collect all games between start and end date
    while current_date <= end_date_dt:
        date_str = current_date.strftime("%Y-%m-%d")
        try:
            schedule_json = fetch_schedule(date_str)
            daily_games = schedule_json.get("dates", [])
            for day in daily_games:
                for game in day.get("games", []):
                    games_to_ingest.append({
                        "game_id": game.get("gamePk"),
                        "season": game.get("season")
                    })
        except requests.HTTPError as e:
            logging.warning(f"Failed to fetch schedule for {date_str}: {e}")
        current_date += timedelta(days=1)

    logging.info(f"Found {len(games_to_ingest)} games to ingest")

    # Process each game
    for game in games_to_ingest:
        game_id = game["game_id"]
        season = game["season"]

        try:
            boxscore = fetch_boxscore(game_id)
        except requests.HTTPError as e:
            logging.warning(f"Failed to fetch boxscore for game {game_id}: {e}")
            continue

        player_stats = boxscore.get("playerByGameStats", {})

        teams = [
            ("awayTeam", boxscore.get("awayTeam", {}).get("id")),
            ("homeTeam", boxscore.get("homeTeam", {}).get("id"))
        ]

        with engine.begin() as conn:
            any_defense_found = False
            for side, team_id in teams:
                if not team_id:
                    continue
                defense_players = player_stats.get(side, {}).get("defense", [])
                if not defense_players:
                    logging.info(f"No defense players found for game {game_id}, team {team_id}")
                    continue
                any_defense_found = True
                upsert_defense_stats(conn, game_id, season, team_id, defense_players)

            if not any_defense_found:
                logging.info(f"No defense stats to insert for game {game_id}")

if __name__ == "__main__":
    ingest_all_games()
