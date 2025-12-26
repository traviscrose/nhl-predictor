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
# Team offense
offense_query = """
SELECT
    t.game_id,
    t.team_id,
    t.team_abbrev,
    t.home_away,
    t.opp_team_id,
    t.opp_abbrev,
    t.goals,
    t.goals_against,
    t.shots,
    t.hits,
    t.points,
    g.game_date AS date,
    g.season
FROM team_vs_opponent t
JOIN games g ON t.game_id = g.id
ORDER BY g.game_date;
"""
offense_df = pd.read_sql(offense_query, engine)
offense_df["date"] = pd.to_datetime(offense_df["date"])

# Team defense
defense_query = """
SELECT
    game_id,
    team_id,
    SUM(blocked_shots) AS blocked_shots,
    SUM(plus_minus) AS plus_minus
FROM team_game_defense
GROUP BY game_id, team_id
ORDER BY game_id;
"""
defense_df = pd.read_sql(defense_query, engine)

# -------------------------------------------------
# 2. Merge offense + defense
# -------------------------------------------------
df = offense_df.merge(
    defense_df,
    left_on=["game_id", "team_id"],
    right_on=["game_id", "team_id"],
    how="left"
)

# Fill missing defense stats with 0
df[["blocked_shots", "plus_minus"]] = df[["blocked_shots", "plus_minus"]].fillna(0)

# -------------------------------------------------
# 3. Compute rolling last-5 features
# -------------------------------------------------
def compute_rolling(df, col_name, group_col, new_name):
    return df.groupby(group_col)[col_name].shift().rolling(5, min_periods=1).mean()

# Team offense rolling
df["shots_last5"] = compute_rolling(df, "shots", "team_id", "shots_last5")
df["hits_last5"] = compute_rolling(df, "hits", "team_id", "hits_last5")
df["points_last5"] = compute_rolling(df, "points", "team_id", "points_last5")

# Team defense rolling
df["def_blocked_shots_last5"] = compute_rolling(df, "blocked_shots", "team_id", "def_blocked_shots_last5")
df["def_plus_minus_last5"] = compute_rolling(df, "plus_minus", "team_id", "def_plus_minus_last5")

# Opponent offense rolling
df["opp_shots_last5"] = compute_rolling(df, "shots", "opp_team_id", "opp_shots_last5")
df["opp_hits_last5"] = compute_rolling(df, "hits", "opp_team_id", "opp_hits_last5")
df["opp_points_last5"] = compute_rolling(df, "points", "opp_team_id", "opp_points_last5")

# Opponent defense rolling
df["opp_def_blocked_shots_last5"] = compute_rolling(df, "blocked_shots", "opp_team_id", "opp_def_blocked_shots_last5")
df["opp_def_plus_minus_last5"] = compute_rolling(df, "plus_minus", "opp_team_id", "opp_def_plus_minus_last5")

# -------------------------------------------------
# 4. Preprocessing
# -------------------------------------------------
df["home_away"] = df["home_away"].map({"home": 1, "away": 0})

numeric_cols = df.select_dtypes(include="number").columns
df[numeric_cols] = df[numeric_cols].fillna(0)

# -------------------------------------------------
# 5. Rolling season backtest
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
# 6. Results
# -------------------------------------------------
if not results:
    raise RuntimeError("No backtest results generated")

all_results = pd.concat(results, ignore_index=True)

print("\nBacktest summary:")
print(all_results.groupby("test_season")["mae"].mean().round(3))
print("\nDone.")
