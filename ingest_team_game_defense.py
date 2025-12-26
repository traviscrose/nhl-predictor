import os
import logging
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# --------------------------
# Load environment variables
# --------------------------
load_dotenv()
DB_URI = os.getenv("DB_URI")

if not DB_URI:
    raise RuntimeError("DB_URI not found in .env")

# --------------------------
# Setup logging
# --------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# --------------------------
# Setup database connection
# --------------------------
engine = create_engine(DB_URI, future=True)

# --------------------------
# NHL API URLs
# --------------------------
SCHEDULE_URL = "https://statsapi.web.nhl.com/api/v1/schedule"
BOXSCORE_URL = "https://statsapi.web.nhl.com/api/v1/game/{game_id}/boxscore"

# --------------------------
# Helper Functions
# --------------------------

def fetch_schedule():
    """Fetch all games from NHL API."""
    resp = requests.get(SCHEDULE_URL)
    resp.raise_for_status()
    data = resp.json()
    games = []
    for date_info in data.get("dates", []):
        for game in date_info.get("games", []):
            games.append({
                "nhl_game_id": game["gamePk"],
                "season": game["season"]
            })
    return games

def fetch_game_boxscore(game_id):
    """Fetch boxscore JSON for a given game."""
    url = BOXSCORE_URL.format(game_id=game_id)
    resp = requests.get(url)
    resp.raise_for_status()
    return resp.json()

def insert_defense_stats(cur, game_id, season, team_id, defense_players):
    """Insert defense player stats into the database."""
    if not defense_players:
        logging.info(f"No defense stats to insert for game {game_id}, team {team_id}")
        return

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

    for player in defense_players:
        cur.execute(insert_sql, {
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

# --------------------------
# Main ingestion loop
# --------------------------

def ingest_all_games():
    games = fetch_schedule()
    logging.info(f"Found {len(games)} games to ingest")

    with engine.begin() as conn:
        for game in games:
            try:
                game_id = game["nhl_game_id"]
                season = game["season"]

                boxscore = fetch_game_boxscore(game_id)
                player_stats = boxscore.get("teams", {})

                # Correctly access playerByGameStats
                pbgs = boxscore.get("playerByGameStats", {})

                for side in ["awayTeam", "homeTeam"]:
                    team_info = boxscore.get(side)
                    if not team_info:
                        continue

                    team_id = team_info.get("id")
                    defense_players = pbgs.get(side, {}).get("defense", [])

                    if not defense_players:
                        logging.info(f"No defense players found for game {game_id}, team {team_id}")
                        continue

                    insert_defense_stats(conn, game_id, season, team_id, defense_players)

                conn.commit()  # Commit after each game
                logging.info(f"Successfully ingested defense stats for game {game_id}")

            except requests.HTTPError as e:
                logging.error(f"Failed to fetch game {game_id}: {e}")
            except SQLAlchemyError as e:
                logging.error(f"Failed to ingest game {game_id}: {e}")
            except Exception as e:
                logging.error(f"Unexpected error for game {game.get('nhl_game_id')}: {e}")

# --------------------------
# Run ingestion
# --------------------------
if __name__ == "__main__":
    ingest_all_games()
