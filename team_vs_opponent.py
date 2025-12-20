import pandas as pd
from db import engine
from persist_team_game_features import persist_team_game_features

# -------------------------------------------------
# 1. Load FINAL games (single source of truth)
# -------------------------------------------------

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

# -------------------------------------------------
# 2. Load player stats ONLY for final games
# -------------------------------------------------

player_stats = pd.read_sql("""
    SELECT
        ps.game_id,
        ps.team_id,
        p.position,
        ps.goals,
        ps.assists,
        ps.points,
        ps.shots,
        ps.hits,
        ps.time_on_ice
    FROM public.player_stats ps
    JOIN public.games g ON ps.game_id = g.id
    JOIN public.players p ON ps.player_id = p.id
    WHERE g.status = 'final'
""", engine)

# -------------------------------------------------
# 3. TOI helper
# -------------------------------------------------

def toi_to_minutes(toi):
    if pd.isna(toi):
        return 0.0
    m, s = toi.split(":")
    return int(m) + int(s) / 60

player_stats["toi_minutes"] = player_stats["time_on_ice"].apply(toi_to_minutes)

# -------------------------------------------------
# 4. Split skaters / goalies
# -------------------------------------------------

skaters = player_stats[player_stats["position"] != "G"]
goalies = player_stats[player_stats["position"] == "G"]

# -------------------------------------------------
# 5. Team-game aggregation (NO abbrevs here)
# -------------------------------------------------

team_game_stats = (
    skaters
    .groupby(["game_id", "team_id"], as_index=False)
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
    how="left",
)

# -------------------------------------------------
# 6. Build game-team perspective (games_long)
# -------------------------------------------------

games_long = pd.concat(
    [
        games.assign(
            team_id=games.home_team_id,
            opp_team_id=games.away_team_id,
            team_abbrev=games.home_abbrev,
            opp_abbrev=games.away_abbrev,
            home_away="home",
        ),
        games.assign(
            team_id=games.away_team_id,
            opp_team_id=games.home_team_id,
            team_abbrev=games.away_abbrev,
            opp_abbrev=games.home_abbrev,
            home_away="away",
        ),
    ],
    ignore_index=True,
)

# -------------------------------------------------
# 7. Merge stats with game context (ID-safe)
# -------------------------------------------------

df = team_game_stats.merge(
    games_long[
        [
            "game_id",
            "team_id",
            "opp_team_id",
            "team_abbrev",
            "opp_abbrev",
            "home_away",
            "date",
        ]
    ],
    on=["game_id", "team_id"],
    how="inner",
    validate="one_to_one",
)

# 🔒 Invariants (fail fast)
assert df["opp_team_id"].notna().all()
assert df["team_abbrev"].notna().all()
assert df["opp_abbrev"].notna().all()
assert df["team_id"].ne(df["opp_team_id"]).all()

# -------------------------------------------------
# 8. Opponent stats (NO collisions)
# -------------------------------------------------

opp_stats = (
    team_game_stats
    .rename(columns={
        "team_id": "opp_team_id",
        "goals": "opp_goals",
        "shots": "opp_shots",
        "hits": "opp_hits",
        "points": "opp_points",
    })
    [["game_id", "opp_team_id", "opp_goals", "opp_shots", "opp_hits", "opp_points"]]
)

df = df.merge(
    opp_stats,
    on=["game_id", "opp_team_id"],
    how="left",
    validate="many_to_one",
)

# -------------------------------------------------
# 9. Rolling last-5 averages
# -------------------------------------------------

df = df.sort_values(["team_id", "date"])

for col in ["goals", "goals_against", "shots", "hits", "points"]:
    df[f"{col}_last5"] = (
        df.groupby("team_id")[col]
        .rolling(5, min_periods=1)
        .mean()
        .reset_index(level=0, drop=True)
    )

# -------------------------------------------------
# 10. Safe numeric fill (NEVER IDs or text)
# -------------------------------------------------

numeric_cols = [
    c for c in df.columns
    if df[c].dtype.kind in "fi" and not c.endswith("_id")
]

df[numeric_cols] = df[numeric_cols].fillna(0)

# -------------------------------------------------
# 11. Final dataset
# -------------------------------------------------

final_cols = [
    "game_id",
    "team_id",
    "team_abbrev",
    "home_away",
    "opp_team_id",
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

# -------------------------------------------------
# 12. Persist
# -------------------------------------------------

persist_team_game_features(final_df)
