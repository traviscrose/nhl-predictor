import os
import requests
import logging
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# ------------------------
# Setup
# ------------------------
load_dotenv()
DB_URI = os.getenv("DB_URI")  # e.g., postgresql+psycopg2://user:pass@localhost/nhl_database

engine = create_engine(DB_URI)
Session = sessionmaker(bind=engine)

BOXSCORE_URL = "https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ------------------------
# Helper Functions
# ------------------------

def fetch_boxscore(game_id):
    url = BOXSCORE_URL.format(game_id=game_id)
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        logging.error(f"Failed to fetch boxscore for game {game_id}: {e}")
        return None

def insert_defense_stats(session, game_id, season, team_id, defense_players):
    if not defense_players:
        logging.info(f"No defense players found for game {game_id}, team {team_id}")
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
            blocked_shots = EXCLUDED.blocked_shots,
            shifts = EXCLUDED.shifts,
            giveaways = EXCLUDED.giveaways,
            takeaways = EXCLUDED.takeaways,
            toi = EXCLUDED.toi
    """)

    for player in defense_players:
        session.execute(insert_sql, {
            "game_id": game_id,
            "season": season,
            "team_id": team_id,
            "player_id": player.get("playerId"),
            "name": player["name"]["default"],
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
            "toi": player.get("toi", "0:00")
        })

# ------------------------
# Main Ingestion Loop
# ------------------------

def ingest_all_games():
    session = Session()
    try:
        # Only select games that don't already have defense stats
        games = session.execute(text("""
            SELECT g.nhl_game_id, g.season, g.home_team_id, g.away_team_id
            FROM games g
            LEFT JOIN team_game_defense d
              ON g.nhl_game_id = d.game_id
            WHERE d.game_id IS NULL
            ORDER BY g.nhl_game_id
        """)).all()

        logging.info(f"Found {len(games)} games to ingest")

        for game_row in games:
            game_id = game_row[0]
            season = game_row[1]
            home_team_id = game_row[2]
            away_team_id = game_row[3]

            boxscore = fetch_boxscore(game_id)
            if not boxscore:
                continue

            player_stats = boxscore.get("playerByGameStats", {})
            inserted_any = False

            # Away defense
            away_defense = player_stats.get("awayTeam", {}).get("defense", [])
            if away_defense:
                insert_defense_stats(session, game_id, season, away_team_id, away_defense)
                inserted_any = True

            # Home defense
            home_defense = player_stats.get("homeTeam", {}).get("defense", [])
            if home_defense:
                insert_defense_stats(session, game_id, season, home_team_id, home_defense)
                inserted_any = True

            if inserted_any:
                session.commit()
                logging.info(f"Inserted/Updated defense stats for game {game_id}")
            else:
                logging.info(f"No defense stats to insert for game {game_id}")

    except Exception as e:
        logging.error(f"Error in ingest_all_games: {e}")
        session.rollback()
    finally:
        session.close()

if __name__ == "__main__":
    ingest_all_games()
