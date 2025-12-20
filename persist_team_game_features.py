import pandas as pd
from sqlalchemy import text
from db import engine

def persist_team_game_features(df: pd.DataFrame):
    df = df.copy()

    # Fill NaNs in numeric columns
    numeric_cols = [
        "goals", "goals_against", "shots", "hits", "points",
        "opp_goals", "opp_shots", "opp_hits", "opp_points",
        "goals_last5", "goals_against_last5", "shots_last5",
        "hits_last5", "points_last5"
    ]
    df.loc[:, numeric_cols] = df[numeric_cols].fillna(0)

    # Ensure table exists
    create_sql = """
    CREATE TABLE IF NOT EXISTS public.team_vs_opponent (
        game_id BIGINT NOT NULL,
        team_id INT NOT NULL,
        team_abbrev TEXT NOT NULL,
        home_away TEXT NOT NULL,
        opp_team_id INT NOT NULL,
        goals INT,
        goals_against INT,
        shots INT,
        hits INT,
        points INT,
        opp_goals INT,
        opp_shots INT,
        opp_hits INT,
        opp_points INT,
        goals_last5 DOUBLE PRECISION,
        goals_against_last5 DOUBLE PRECISION,
        shots_last5 DOUBLE PRECISION,
        hits_last5 DOUBLE PRECISION,
        points_last5 DOUBLE PRECISION,
        PRIMARY KEY (game_id, team_id)
    );
    """
    with engine.begin() as conn:
        conn.execute(text(create_sql))

        # Upsert rows
        for _, row in df.iterrows():
            upsert_sql = """
            INSERT INTO public.team_vs_opponent (
                game_id, team_id, team_abbrev, home_away, opp_team_id,
                goals, goals_against, shots, hits, points,
                opp_goals, opp_shots, opp_hits, opp_points,
                goals_last5, goals_against_last5, shots_last5,
                hits_last5, points_last5
            )
            VALUES (
                :game_id, :team_id, :team_abbrev, :home_away, :opp_team_id,
                :goals, :goals_against, :shots, :hits, :points,
                :opp_goals, :opp_shots, :opp_hits, :opp_points,
                :goals_last5, :goals_against_last5, :shots_last5,
                :hits_last5, :points_last5
            )
            ON CONFLICT (game_id, team_id) DO UPDATE
            SET
                goals = EXCLUDED.goals,
                goals_against = EXCLUDED.goals_against,
                shots = EXCLUDED.shots,
                hits = EXCLUDED.hits,
                points = EXCLUDED.points,
                opp_goals = EXCLUDED.opp_goals,
                opp_shots = EXCLUDED.opp_shots,
                opp_hits = EXCLUDED.opp_hits,
                opp_points = EXCLUDED.opp_points,
                goals_last5 = EXCLUDED.goals_last5,
                goals_against_last5 = EXCLUDED.goals_against_last5,
                shots_last5 = EXCLUDED.shots_last5,
                hits_last5 = EXCLUDED.hits_last5,
                points_last5 = EXCLUDED.points_last5;
            """
            conn.execute(text(upsert_sql), **row.to_dict())

    print(f"Persisted {len(df)} rows into public.team_vs_opponent")
