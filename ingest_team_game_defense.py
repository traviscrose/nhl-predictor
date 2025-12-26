import requests
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from db import engine  # assumes you have a SQLAlchemy engine in db.py

# -----------------------------
# Config
# -----------------------------
BOXSCORE_URL = "https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore"

# -----------------------------
# Helper: insert defense data
# -----------------------------
def ingest_defense(game_id, season):
    """
    Fetches boxscore for a game and inserts defensive stats into the database.
    """
    r = requests.get(BOXSCORE_URL.format(game_id=game_id))
    r.raise_for_status()
    data = r.json()

    # Check structure
    teams = data.get("playerByGameStats")
    if not teams:
        raise ValueError("'playerByGameStats' missing in API response")

    with engine.begin() as conn:
        for side in ["homeTeam", "awayTeam"]:
            team_data = data["homeTeam"] if side == "homeTeam" else data["awayTeam"]
            team_id = team_data["id"]
            players = teams[side]["defense"]
            goalies = teams[side]["goalies"]

            # Insert defense players
            for p in players:
                conn.execute(
                    """
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
                    """,
                    [
                        {
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
                            "toi": p.get("toi"),
                        }
                    ],
                )

            # Insert goalies
            for g in goalies:
                conn.execute(
                    """
                    INSERT INTO team_game_goalies (
                        game_id, season, team_id, player_id, name, position,
                        goals_against, saves, shots_against, save_pctg, toi, decision
                    ) VALUES (
                        :game_id, :season, :team_id, :player_id, :name, :position,
                        :goals_against, :saves, :shots_against, :save_pctg, :toi, :decision
                    )
                    ON CONFLICT (game_id, player_id) DO UPDATE SET
                        goals_against = EXCLUDED.goals_against,
                        saves = EXCLUDED.saves,
                        shots_against = EXCLUDED.shots_against,
                        save_pctg = EXCLUDED.save_pctg,
                        toi = EXCLUDED.toi,
                        decision = EXCLUDED.decision
                    """,
                    [
                        {
                            "game_id": game_id,
                            "season": season,
                            "team_id": team_id,
                            "player_id": g["playerId"],
                            "name": g["name"]["default"],
                            "position": g["position"],
                            "goals_against": g.get("goalsAgainst", 0),
                            "saves": g.get("saves", 0),
                            "shots_against": g.get("shotsAgainst", 0),
                            "save_pctg": g.get("savePctg"),
                            "toi": g.get("toi"),
                            "decision": g.get("decision"),
                        }
                    ],
                )


# -----------------------------
# Main loop: ingest all games
# -----------------------------
def ingest_all_games():
    games_df = pd.read_sql("SELECT nhl_game_id, season FROM games ORDER BY season, nhl_game_id", engine)
    total_games = len(games_df)
    print(f"Found {total_games} games to ingest")

    failed_games = []

    for idx, row in games_df.iterrows():
        nhl_game_id = row["nhl_game_id"]
        season = row["season"]

        try:
            ingest_defense(nhl_game_id, season)
        except Exception as e:
            print(f"Failed to ingest game {nhl_game_id}: {e}")
            failed_games.append(nhl_game_id)

        # Progress indicator every 50 games
        if (idx + 1) % 50 == 0:
            print(f"Ingested {idx + 1}/{total_games} games so far")

    print("\nIngestion complete.")
    if failed_games:
        print(f"Failed to ingest {len(failed_games)} games:")
        print(failed_games)


if __name__ == "__main__":
    ingest_all_games()
