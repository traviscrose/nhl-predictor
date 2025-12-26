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

DB_URI = os.getenv("DB_URI")
engine = create_engine(DB_URI)
Session = sessionmaker(bind=engine)

BOXSCORE_URL = "https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ------------------------
# Helpers
# ------------------------

def fetch_boxscore(game_id):
    try:
        r = requests.get(
            BOXSCORE_URL.format(game_id=game_id),
            timeout=10
        )
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logging.error(f"Boxscore fetch failed for game {game_id}: {e}")
        return None


def rebuild_team_game_defense(session):
    logging.info("Rebuilding team_game_defense table (TRUNCATE)")
    session.execute(text("TRUNCATE TABLE team_game_defense"))
    session.commit()


def insert_defense_stats(session, game_id, season, team_id, defense_players):

    sql = text("""
        INSERT INTO team_game_defense (
            game_id,
            season,
            team_id,
            player_id,
            name,
            position,
            goals,
            assists,
            points,
            plus_minus,
            pim,
            hits,
            blocked_shots,
            shifts,
            giveaways,
            takeaways,
            toi
        ) VALUES (
            :game_id,
            :season,
            :team_id,
            :player_id,
            :name,
            :position,
            :goals,
            :assists,
            :points,
            :plus_minus,
            :pim,
            :hits,
            :blocked_shots,
            :shifts,
            :giveaways,
            :takeaways,
            :toi
        )
        ON CONFLICT (game_id, player_id) DO UPDATE SET
            goals          = EXCLUDED.goals,
            assists        = EXCLUDED.assists,
            points         = EXCLUDED.points,
            plus_minus     = EXCLUDED.plus_minus,
            pim             = EXCLUDED.pim,
            hits            = EXCLUDED.hits,
            blocked_shots   = EXCLUDED.blocked_shots,
            shifts          = EXCLUDED.shifts,
            giveaways       = EXCLUDED.giveaways,
            takeaways       = EXCLUDED.takeaways,
            toi             = EXCLUDED.toi
    """)

    for p in defense_players:
        session.execute(sql, {
            "game_id": game_id,
            "season": season,
            "team_id": team_id,
            "player_id": p.get("playerId"),
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
            "toi": p.get("toi", "0:00")
        })


# ------------------------
# Main
# ------------------------

def ingest_all_games(rebuild=False):
    session = Session()

    try:
        if rebuild:
            rebuild_team_game_defense(session)

        games = session.execute(text("""
            SELECT
                nhl_game_id,
                season,
                home_team_id,
                away_team_id
            FROM games
            ORDER BY nhl_game_id
        """)).all()

        logging.info(f"Ingesting defense stats for {len(games)} games")

        for g in games:
            game_id, season, home_id, away_id = g

            box = fetch_boxscore(game_id)
            if not box:
                continue

            stats = box.get("playerByGameStats", {})

            away_def = stats.get("awayTeam", {}).get("defense", [])
            home_def = stats.get("homeTeam", {}).get("defense", [])

            if away_def:
                insert_defense_stats(session, game_id, season, away_id, away_def)

            if home_def:
                insert_defense_stats(session, game_id, season, home_id, home_def)

            session.commit()
            logging.info(f"Processed defense stats for game {game_id}")

    except Exception as e:
        session.rollback()
        logging.error(f"Defense ingestion failed: {e}")
        raise
    finally:
        session.close()


if __name__ == "__main__":
    ingest_all_games(rebuild=True)
