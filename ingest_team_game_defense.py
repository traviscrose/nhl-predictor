import pandas as pd
import requests
from db import get_conn

# -------------------------------
# Config
# -------------------------------

BOXSCORE_URL = "https://statsapi.web.nhl.com/api/v1/game/{game_id}/boxscore"

# -------------------------------
# Helper functions
# -------------------------------

def upsert_team_defense(cur, game_id, team_id, opponent_id, season, is_home,
                        goals_against, shots_against):
    """
    Insert a row into team_game_defense or update if it exists.
    """
    cur.execute("""
        INSERT INTO team_game_defense (
            game_id, team_id, opponent_team_id, season, is_home,
            goals_against, shots_against
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (game_id, team_id)
        DO UPDATE SET
            goals_against = EXCLUDED.goals_against,
            shots_against = EXCLUDED.shots_against
    """, (game_id, team_id, opponent_id, season, is_home, goals_against, shots_against))


def ingest_defense(game_id, season):
    """
    Ingest defense stats for both teams in a game.
    """
    r = requests.get(BOXSCORE_URL.format(game_id=game_id))
    data = r.json()

    home = data["teams"]["home"]["team"]
    away = data["teams"]["away"]["team"]

    home_stats = data["teams"]["home"]["teamStats"]["teamSkaterStats"]
    away_stats = data["teams"]["away"]["teamStats"]["teamSkaterStats"]

    home_goals_against = away_stats["goals"]
    away_goals_against = home_stats["goals"]

    home_shots_against = away_stats.get("shots", None)
    away_shots_against = home_stats.get("shots", None)

    is_home_flag = True

    conn = get_conn()
    cur = conn.cursor()
    try:
        # Home team defense
        upsert_team_defense(
            cur,
            game_id,
            home["id"],
            away["id"],
            season,
            is_home_flag,
            home_goals_against,
            home_shots_against,
        )

        # Away team defense
        upsert_team_defense(
            cur,
            game_id,
            away["id"],
            home["id"],
            season,
            not is_home_flag,
            away_goals_against,
            away_shots_against,
        )

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
