import pandas as pd
from db import engine
from persist_team_game_features import persist_team_game_features

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
    FROM public.player_stats ps
    JOIN public.players p ON ps.player_id = p.id
    JOIN public.teams t ON ps.team_id = t.id
""", engine)

games = pd.read_sql("""
    SELECT
        g.id AS game_id,
        g.date,
        g.home_team_id,
        g.away_team_id,
        ht.abbreviation AS home_abbrev,
        at.abbreviation AS away_abbrev
    FROM public.games g
    JOIN public.teams ht ON g.home_team_id = ht.id
    JOIN public.teams at ON g.away_team_id = at.id
    WHERE g.status = 'final'
""", engine)

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
    .groupby(["game_id", "team_id", "team_abbrev"], as_index=False)
    .agg(
        goals=("goals", "sum"),
        assists=("assists", "sum"),
        points=("points", "sum"),
        shots=("shots", "sum"),
        hits=("hits", "sum"),
        toi_minutes=("toi_minutes", "sum"),
    )
)

goalie_game_stats = (
    goalies
    .groupby(["game_id", "team_id"], as_index=False)
    .agg(
        goals_against=("goals", "sum"),
        shots_against=("shots", "sum"),
        goalie_toi=("toi_minutes", "sum"),
    )
)

team_game_stats = team_game_stats.merge(
    goalie_game_stats,
    on=["game_id", "team_id"],
    how="left"
)

# ---------------------------
# 4. Attach opponent info
# ---------------------------

# Create long format games table for home and away
games_long = pd.concat([
    games.assign(
        team_id=games.home_team_id,
        team_abbrev=games.home_abbrev,
        home_away="home",
        opp_team_id=games.away_team_id,
        opp_abbrev=games.away_abbrev
    ),
    games.assign(
        team_id=games.away_team_id,
        team_abbrev=games.away_abbrev,
        home_away="away",
        opp_team_id=games.home_team_id,
        opp_abbrev=games.home_abbrev
    )
], ignore_index=True)

# Merge team stats with game info, use only game_id and team_id as keys
df = team_game_stats.merge(
    games_long[[
        "game_id", "team_id", "team_abbrev", "home_away", "opp_team_id", "opp_abbrev", "date"
    ]],
    on=["game_id", "team_id"],
    how="inner"
)

# ---------------------------
# 5. Add opponent stats
# ---------------------------

# Prepare opponent stats
opp_stats = team_game_stats.rename(columns={
    "team_id": "opp_team_id",
    "team_abbrev": "opp_abbrev",
    "goals": "opp_goals",
    "shots": "opp_shots",
    "hits": "opp_hits",
    "points": "opp_points",
})[["game_id", "opp_team_id", "opp_abbrev", "opp_goals", "opp_shots", "opp_hits", "opp_points"]]

df = df.merge(
    opp_stats,
    on=["game_id", "opp_team_id", "opp_abbrev"],
    how="left"
)

# ---------------------------
# 6. Rolling last-5 averages
# ---------------------------

df = df.sort_values(["team_id", "date"])

for col in ["goals", "goals_against", "shots", "hits", "points"]:
    df[f"{col}_last5"] = (
        df.groupby("team_id")[col]
          .rolling(5, min_periods=1)
          .mean()
          .reset_index(level=0, drop=True)
    )

# ---------------------------
# 7. Final dataset
# ---------------------------

# Ensure all final columns exist in df
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

# Fill missing columns with NaN if they somehow don't exist
for col in final_cols:
    if col not in df.columns:
        df[col] = float("nan")

final_df = df[final_cols]

print(final_df.head())
print(f"Final rows: {len(final_df)}")

# ---------------------------
# 8. Persist to DB
# ---------------------------

persist_team_game_features(final_df)
