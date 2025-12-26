import requests
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
import os

# -------------------------------
# Load environment variables
# -------------------------------
load_dotenv()  # reads .env in current directory
DB_URI = os.getenv("DB_URI")
if not DB_URI:
    raise RuntimeError("DB_URI not set in .env file")

BOXSCORE_URL = "https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore"

engine = create_engine(DB_URI, future=True)

# -------------------------------
# Helper Functions
# -------------------------------

def ingest_defense(game_id, season):
    try:
        r = requests.get(BOXSCORE_URL.format(game_id=game_id))
        r.raise_for_status()
        data = r.json()
    except (requests.RequestException, ValueError) as e:
        print(f"Failed to fetch game {game_id}: {e}")
        return

    try:
        with engine.begin() as conn:
            for team_key in ["homeTeam", "awayTeam"]:
                team_data = data.get(team_key)
                if not team_data:
                    print(f"Game {game_id} missing {team_key}")
                    continue

                team_id = team_data.get("id")
                if not team_id:
                    print(f"Game {game_id}, {team_key} missing id")
                    continue

                players = team_data.get("playerByGameStats", {}).get("defense", [])
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
                    conn.execute(stmt, {
                        "game_id": game_id,
                        "season": season,
                        "team_id": team_id,
                        "player_id": p.get("playerId"),
                        "name": p.get("name", {}).get("default"),
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
                        "toi": p.get("toi"),
                    })
    except SQLAlchemyError as e:
        print(f"Failed to ingest game {game_id}: {e}")


def ingest_all_games():
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT nhl_game_id, season FROM games ORDER BY game_date"))
            games = result.fetchall()
    except SQLAlchemyError as e:
        print(f"Failed to fetch games from DB: {e}")
        return

    print(f"Found {len(games)} games to ingest")

    for game in games:
        game_id, season = game
        try:
            ingest_defense(game_id, season)
        except Exception as e:
            print(f"Unexpected error ingesting game {game_id}: {e}")


# -------------------------------
# Main
# -------------------------------

if __name__ == "__main__":
    ingest_all_games()
