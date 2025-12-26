import os
import requests
import logging
from datetime import date, timedelta
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# -----------------------
# Load environment variables
# -----------------------
load_dotenv()
DB_URI = os.getenv("DB_URI")

if not DB_URI:
    raise ValueError("DB_URI not set in .env file")

# -----------------------
# Logging
# -----------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# -----------------------
# Database setup
# -----------------------
engine = create_engine(DB_URI, future=True)

# -----------------------
# NHL API URLs
# -----------------------
BOXSCORE_URL = "https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore"

# -----------------------
# Helper functions
# -----------------------
def fetch_schedule_for_date(schedule_date):
    url = f"https://api-web.nhle.com/v1/schedule/{schedule_date}"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    return data.get("dates", [])

def fetch_boxscore(game_id):
    url = BOXSCORE_URL.format(game_id=game_id)
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def upsert_defense_stats(cur, game_id, season, team_id, players):
    if not players:
        logging.info(f"No defense stats to insert for game {game_id}, team {team_id}")
        return
    for p in players:
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
            "toi": p.get("toi", None)
        })

# -----------------------
# Main ingestion loop
# -----------------------
def ingest_all_games(start_date: date, end_date: date):
    delta = timedelta(days=1)
    current_date = start_date

    while current_date <= end_date:
        logging.info(f"Fetching schedule for {current_date}")
        try:
            days = fetch_schedule_for_date(current_date.isoformat())
        except requests.HTTPError as e:
            logging.error(f"Failed to fetch schedule for {current_date}: {e}")
            current_date += delta
            continue

        for day in days:
            for game in day.get("games", []):
                game_id = game["gamePk"]
                season = game["season"]
                logging.info(f"Processing game {game_id} ({season})")

                try:
                    boxscore = fetch_boxscore(game_id)
                except requests.HTTPError as e:
                    logging.error(f"Failed to fetch boxscore for game {game_id}: {e}")
                    continue

                player_stats = boxscore.get("playerByGameStats", {})
                teams = [("awayTeam", game["teams"]["away"]["id"] if "teams" in game else None),
                         ("homeTeam", game["teams"]["home"]["id"] if "teams" in game else None)]

                with engine.begin() as conn:
                    for side, team_id in teams:
                        if not team_id:
                            continue
                        defense_players = player_stats.get(side, {}).get("defense", [])
                        if not defense_players:
                            logging.info(f"No defense players found for game {game_id}, team {team_id}")
                        upsert_defense_stats(conn, game_id, season, team_id, defense_players)

        current_date += delta

# -----------------------
# Run ingestion
# -----------------------
if __name__ == "__main__":
    # Example: ingest from Oct 1, 2025 to Dec 26, 2025
    ingest_all_games(date(2021, 10, 1), date(2025, 12, 26))
