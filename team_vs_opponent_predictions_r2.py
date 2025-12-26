import pandas as pd
from sklearn.linear_model import PoissonRegressor
from sklearn.metrics import mean_absolute_error
from db import engine

# -------------------------------------------------
# Config
# -------------------------------------------------
FEATURES = [
    "shots_last5",
    "hits_last5",
    "points_last5",
    "def_blocked_shots_last5",
    "def_plus_minus_last5",
    "opp_shots_last5",
    "opp_hits_last5",
    "opp_points_last5",
    "opp_def_blocked_shots_last5",
    "opp_def_plus_minus_last5",
    "home_away",
]

TARGET = "goals"

# -------------------------------------------------
# 1. Load raw data
# -------------------------------------------------
# Load offensive stats from team_vs_opponent
off_query = """
SELECT
    t.game_id,
    t.team_id,
    t.team_abbrev,
    t.home_away,
    t.opp_team_id,
    t.opp_abbrev,
    t.goals,
    t.goals_against,
    t.shots AS shots_raw,
    t.hits AS hits_raw,
    t.points AS points_raw,
    g.game_date AS date,
    g.season
FROM team_vs_opponent t
JOIN games g
  ON t.game_id = g.id
ORDER BY g.game_date;
"""

df_off = pd.read_sql(off_query, engine)
df_off["date"] = pd.to_datetime(df_off["date"])

# Load defense stats from team_game_defense
def_query = """
SELECT
    game_id,
    team_id,
    SUM(blocked_shots) AS blocked_shots,
    SUM(plus_minus) AS plus_minus
FROM team_game_defense
GROUP BY game_id, team_id
ORDER BY game_id;
"""

df_def = pd.read_sql(def_query, engine)

# -------------------------------------------------
# 2. Merge and compute last-5 rolling stats
# -------------------------------------------------
# Merge offense and defense
df = pd.merge(df_off, df_def, how="left", on=["game_id", "team_id"])

# Sort for rolling calculations
df = df.sort_values(["team_id", "date"]).reset_index(drop=True)

# Compute rolling last-5 features per team
roll_cols = ["shots_raw", "hits_raw", "points_raw", "blocked_shots", "plus_minus"]
for col in roll_cols:
    df[f"{col}_last5"] = df.groupby("team_id")[col].shift().rolling(5, min_periods=1).mean()

# Compute opponent rolling stats
df = pd.merge(
    df,
    df.groupby(["game_id", "team_id"])[["blocked_shots_last5", "plus_minus_last5"]].sum().reset_index(),
    how="left",
    left_on=["game_id", "opp_team_id"],
    right_on=["game_id", "team_id"],
    suffixes=("", "_opp")
)
df["home_away"] = df["home_away"].map({"home": 1, "away": 0})

# Fill NaNs with 0 for new rolling stats
df[[
    "shots_raw_last5", "hits_raw_last5", "points_raw_last5",
    "blocked_shots_last5", "plus_minus_last5",
    "blocked_shots_last5_opp", "plus_minus_last5_opp"
]] = df[[
    "shots_raw_last5", "hits_raw_last5", "points_raw_last5",
    "blocked_shots_last5", "plus_minus_last5",
    "blocked_shots_last5_opp", "plus_minus_last5_opp"
]].fillna(0)

# Rename for model FEATURES
df = df.rename(columns={
    "shots_raw_last5": "shots_last5",
    "hits_raw_last5": "hits_last5",
    "points_raw_last5": "points_last5",
    "blocked_shots_last5": "def_blocked_shots_last5",
    "plus_minus_last5": "def_plus_minus_last5",
    "blocked_shots_last5_opp": "opp_def_blocked_shots_last5",
    "plus_minus_last5_opp": "opp_def_plus_minus_last5",
})

# -------------------------------------------------
# 3. Rolling season backtest
# -------------------------------------------------
seasons = sorted(df["season"].unique())
results = []

for i in range(1, len(seasons)):
    train_seasons = seasons[:i]
    test_season = seasons[i]

    train = df[df["season"].isin(train_seasons)]
    test = df[df["season"] == test_season]

    if train.empty or test.empty:
        print(f"Skipping season {test_season} (no data)")
        continue

    X_train = train[FEATURES]
    y_train = train[TARGET]
    X_test = test[FEATURES]
    y_test = test[TARGET]

    model = PoissonRegressor(alpha=0.001, max_iter=1000)
    model.fit(X_train, y_train)

    test = test.copy()
    test["pred_goals"] = model.predict(X_test)

    mae = mean_absolute_error(y_test, test["pred_goals"])

    print(
        f"Train seasons {train_seasons} → "
        f"Test season {test_season} | MAE = {mae:.3f}"
    )

    results.append(
        test.assign(
            test_season=test_season,
            train_seasons=",".join(map(str, train_seasons)),
            mae=mae,
        )
    )

# -------------------------------------------------
# 4. Results
# -------------------------------------------------
if not results:
    raise RuntimeError("No backtest results generated")

all_results = pd.concat(results, ignore_index=True)

print("\nBacktest summary:")
print(
    all_results
    .groupby("test_season")["mae"]
    .mean()
    .round(3)
)

print("\nDone.")
