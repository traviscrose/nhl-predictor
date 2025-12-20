import pandas as pd
from sqlalchemy import text
from db import engine

def persist_team_game_features(final_df: pd.DataFrame):
    """
    Inserts or updates team_game_features from a pandas DataFrame.
    """
    with engine.begin() as conn:
        for _, row in final_df.iterrows():
            conn.execute(
                text("""
                    INSERT INTO public.team_game_features (
                        game_id,
                        team_id,
                        team_abbrev,
                        home_away,
                        opp_team_id,
                        goals,
                        goals_against,
                        shots,
                        hits,
                        points,
                        opp_goals,
                        opp_shots,
                        opp_hits,
                        opp_points,
                        goals_last5,
                        goals_against_last5,
                        shots_last5,
                        hits_last5,
                        points_last5
                    )
                    VALUES (
                        :game_id,
                        :team_id,
                        :team_abbrev,
                        :home_away,
                        :opp_team_id,
                        :goals,
                        :goals_against,
                        :shots,
                        :hits,
                        :points,
                        :opp_goals,
                        :opp_shots,
                        :opp_hits,
                        :opp_points,
                        :goals_last5,
                        :goals_against_last5,
                        :shots_last5,
                        :hits_last5,
                        :points_last5
                    )
                    ON CONFLICT (game_id, team_id) DO UPDATE SET
                        team_abbrev = EXCLUDED.team_abbrev,
                        home_away = EXCLUDED.home_away,
                        opp_team_id = EXCLUDED.opp_team_id,
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

    print(f"Persisted {len(final_df)} rows into team_game_features")
