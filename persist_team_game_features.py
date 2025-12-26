from sqlalchemy import text
from db import engine

def persist_team_game_features(df):
    df = df.copy()  # avoid SettingWithCopyWarning

    numeric_cols = df.select_dtypes(include="number").columns
    df[numeric_cols] = df[numeric_cols].fillna(0)

    upsert_sql = """
    INSERT INTO public.team_vs_opponent (
        game_id,
        team_id,
        team_abbrev,
        home_away,
        opp_team_id,
        opp_abbrev,

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
        :opp_abbrev,

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
        points_last5 = EXCLUDED.points_last5,
        opp_team_id = EXCLUDED.opp_team_id
    """

    with engine.begin() as conn:
        conn.execute(text(upsert_sql), df.to_dict(orient="records"))

    print(f"Persisted {len(df)} rows into public.team_vs_opponent")
