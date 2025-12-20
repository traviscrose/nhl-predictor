import pandas as pd
from db import engine

# ---------------------------
# 1. Load base tables
# ---------------------------

player_stats = pd.read_sql("""
    SELECT
        ps.game_id,
        ps.team_id,
        t.abbreviation AS team_abbrev,
        p.position,
        ps.goals,
        ps.assists,
        ps.points,
        ps.shots,
        ps.hits,
        ps.time_on_ice
    FROM player_stats ps
    JOIN players p ON ps.player_id = p.id
    JOIN teams t ON ps.team_id = t.id
""", engine)

games = pd.read_sql("""
    SELECT
        id AS game_id,
        date,
        home_team_id,
        away_team_id
    FROM public.games
    WHERE status = 'final'
""", engine)

required = {"game_id", "home_team_id", "away_team_id"}
missing = required - set(games.columns)
assert not missing, f"Missing columns in games DF: {missing}"


# ---------------------------
# 2. Split skaters & goalies
# ---------------------------

skaters = player_stats[player_stats["position"] != "G"].copy()
goalies = player_stats[player_stats["position"] == "G"].copy()

def toi_to_minutes(toi):
    if pd.isna(toi):
        return 0.0
    m, s = toi.split(":")
    return int(m) + int(s) / 60

skaters["toi_minutes"] = skaters["time_on_ice"].apply(toi_to_minutes)
goalies["toi_minutes"] = goalies["time_on_ice"].apply(toi_to_minutes)

# ---------------------------
# 3. Aggregate TEAM GAME stats
# ---------------------------

team_game_stats = (
    skaters
    .groupby(["game_id", "team_abbrev"])
    .agg(
        goals=("goals", "sum"),
        assists=("assists", "sum"),
        points=("points", "sum"),
        shots=("shots", "sum"),
        hits=("hits", "sum"),
        toi_minutes=("toi_minutes", "sum"),
    )
    .reset_index()
)

goalie_game_stats = (
    goalies
    .groupby(["game_id", "team_abbrev"])
    .agg(
        goals_against=("goals", "sum"),
        shots_against=("shots", "sum"),
        goalie_toi=("toi_minutes", "sum"),
    )
    .reset_index()
)

team_game_stats = team_game_stats.merge(
    goalie_game_stats,
    on=["game_id", "team_abbrev"],
    how="left"
)

# ---------------------------
# 4. Attach opponent info
# ---------------------------

games_long = pd.concat([
    games.assign(
        team_id=games.home_team_id,
        home_away="home",
        opp_team_id=games.away_team_id
    ),
    games.assign(
        team_id=games.away_team_id,
        home_away="away",
        opp_team_id=games.home_team_id
    )
], ignore_index=True)

df = team_game_stats.merge(
    games_long,
    on=["game_id", "team_id"],
    how="inner"
)

# ---------------------------
# 5. Add opponent stats
# ---------------------------

opp_stats = team_game_stats.rename(columns={
    "team_abbrev": "opp_abbrev",
    "goals": "opp_goals",
    "shots": "opp_shots",
    "hits": "opp_hits",
    "points": "opp_points",
})

df = df.merge(
    opp_stats[[
        "game_id",
        "opp_abbrev",
        "opp_goals",
        "opp_shots",
        "opp_hits",
        "opp_points"
    ]],
    on=["game_id", "opp_abbrev"],
    how="left"
)

# ---------------------------
# 6. Rolling last-5 averages
# ---------------------------

df = df.sort_values(["team_abbrev", "date"])

for col in ["goals", "goals_against", "shots", "hits", "points"]:
    df[f"{col}_last5"] = (
        df
        .groupby("team_abbrev")[col]
        .rolling(5, min_periods=1)
        .mean()
        .reset_index(level=0, drop=True)
    )

assert "team_id" in df.columns, "team_id missing before final selection"

# ---------------------------
# 7. Final dataset
# ---------------------------

final_cols = [
    "game_id",
    "team_id",
    "team_abbrev",
    "home_away",
    "opp_abbrev",
    "goals",
    "goals_against",
    "shots",
    "hits",
    "points",
    "opp_goals",
    "opp_shots",
    "opp_hits",
    "opp_points",
    "goals_last5",
    "goals_against_last5",
    "shots_last5",
    "hits_last5",
    "points_last5",
]

final_df = df[final_cols]

print(final_df.head())
print(f"Final rows: {len(final_df)}")

from persist_team_game_features import persist_team_game_features

persist_team_game_features(final_df)
