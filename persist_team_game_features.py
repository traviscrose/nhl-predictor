from sqlalchemy import text
import numpy as np

def persist_team_game_features(df):
    """
    Persist the team vs opponent features into a permanent table.
    Handles NaNs and uses SQLAlchemy 2.x compatible syntax.
    """
    # Ensure NaNs in numeric columns are replaced with 0
    numeric_cols = [
        "goals", "goals_against", "shots", "hits", "points",
        "opp_goals", "opp_shots", "opp_hits", "opp_points",
        "goals_last5", "goals_against_last5", "shots_last5", "hits_last5", "points_last5"
    ]
    df[numeric_cols] = df[numeric_cols].fillna(0)

    # Table should already exist; if not, create separately with proper types
    df_clean = df.copy()

    with engine.begin() as conn:  # begins a transaction
        for _, row in df_clean.iterrows():
            conn.execute(
                text("""
                    INSERT INTO public.team_vs_opponent (
                        game_id, team_id, team_abbrev, home_away, opp_team_id,
                        goals, goals_against, shots, hits, points,
                        opp_goals, opp_shots, opp_hits, opp_points,
                        goals_last5, goals_against_last5, shots_last5, hits_last5, points_last5
                    ) VALUES (
                        :game_id, :team_id, :team_abbrev, :home_away, :opp_team_id,
                        :goals, :goals_against, :shots, :hits, :points,
                        :opp_goals, :opp_shots, :opp_hits, :opp_points,
                        :goals_last5, :goals_against_last5, :shots_last5, :hits_last5, :points_last5
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
                """),
                row.to_dict()
            )

    print(f"Persisted {len(df_clean)} rows into public.team_vs_opponent")
