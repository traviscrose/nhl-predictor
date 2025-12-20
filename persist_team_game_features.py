import pandas as pd
from sqlalchemy import text
from db import engine

def persist_team_game_features(df):
    from db import engine
    import pandas as pd

    # Convert all NaNs to None for proper NULL insertion
    df_clean = df.where(pd.notna(df), None)

    # Ensure integer columns are integers or None
    int_cols = ["game_id", "team_id", "opp_team_id", "goals", "goals_against",
                "shots", "hits", "points", "opp_goals", "opp_shots", "opp_hits", "opp_points"]
    
    for col in int_cols:
        df_clean[col] = df_clean[col].astype("Int64")  # Pandas nullable integer type

    # Now persist to Postgres
    with engine.begin() as conn:
        # Optional: create permanent table if not exists
        conn.execute("""
            CREATE TABLE IF NOT EXISTS public.team_vs_opponent (
                game_id INT,
                team_id INT,
                team_abbrev TEXT,
                home_away TEXT,
                opp_team_id INT,
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
            )
        """)

        # Insert or update
        for _, row in df_clean.iterrows():
            conn.execute("""
                INSERT INTO public.team_vs_opponent (
                    game_id, team_id, team_abbrev, home_away, opp_team_id,
                    goals, goals_against, shots, hits, points,
                    opp_goals, opp_shots, opp_hits, opp_points,
                    goals_last5, goals_against_last5, shots_last5, hits_last5, points_last5
                ) VALUES (
                    %(game_id)s, %(team_id)s, %(team_abbrev)s, %(home_away)s, %(opp_team_id)s,
                    %(goals)s, %(goals_against)s, %(shots)s, %(hits)s, %(points)s,
                    %(opp_goals)s, %(opp_shots)s, %(opp_hits)s, %(opp_points)s,
                    %(goals_last5)s, %(goals_against_last5)s, %(shots_last5)s, %(hits_last5)s, %(points_last5)s
                )
                ON CONFLICT (game_id, team_id)
                DO UPDATE SET
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
                    points_last5 = EXCLUDED.points_last5
            """, row.to_dict())


    print(f"Persisted {len(final_df)} rows into team_game_features")
