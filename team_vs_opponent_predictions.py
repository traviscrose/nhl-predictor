import pandas as pd
import numpy as np
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_squared_error, mean_absolute_error
from db import engine

# -----------------------------
# 1. Load features from DB and join games for date
# -----------------------------
query = """
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
    t.opp_goals,
    t.opp_shots,
    t.opp_hits,
    t.opp_points,
    g.date
FROM team_vs_opponent t
JOIN games g ON t.game_id = g.id
ORDER BY t.team_id, t.date ASC;
"""
df = pd.read_sql(query, engine)
df["date"] = pd.to_datetime(df["date"])

# -----------------------------
# 2. Determine season for each game
# -----------------------------
df["season_year"] = df["date"].dt.year
df["season_month"] = df["date"].dt.month
# Season starts in October
df["season_start"] = np.where(df["season_month"] < 7, df["season_year"] - 1, df["season_year"])

# -----------------------------
# 3. Compute rolling last-5 averages for team stats
# -----------------------------
df = df.sort_values(["team_id", "date"])

team_cols = ["goals", "goals_against", "shots", "hits", "points"]
opp_cols  = ["opp_goals", "opp_shots", "opp_hits", "opp_points"]

for col in team_cols:
    df[f"{col}_last5"] = df.groupby("team_id")[col].rolling(5, min_periods=1).mean().reset_index(level=0, drop=True)

for col in opp_cols:
    df[f"{col}_last5"] = df.groupby("team_id")[col].rolling(5, min_periods=1).mean().reset_index(level=0, drop=True)

# -----------------------------
# 4. Compute delta features
# -----------------------------
df["goal_diff"]  = df["goals"] - df["opp_goals"]
df["goals_last5_diff"]  = df["goals_last5"] - df["opp_goals_last5"]
df["shots_last5_diff"]  = df["shots_last5"] - df["opp_shots_last5"]
df["hits_last5_diff"]   = df["hits_last5"] - df["opp_hits_last5"]
df["points_last5_diff"] = df["points_last5"] - df["opp_points_last5"]

features = [
    "home_away",
    "goals_last5_diff",
    "shots_last5_diff",
    "hits_last5_diff",
    "points_last5_diff"
]

# Convert home_away to numeric
df["home_away"] = df["home_away"].map({"home": 1, "away": 0})

# -----------------------------
# 5. Rolling season-by-season backtesting
# -----------------------------
results = []
seasons = sorted(df["season_start"].unique())

for i in range(1, len(seasons)):
    train_seasons = seasons[:i]      # all prior seasons
    test_season   = seasons[i]       # next season
    
    train = df[df["season_start"].isin(train_seasons)]
    test  = df[df["season_start"] == test_season]
    
    X_train = train[features]
    y_train = train["goal_diff"]
    X_test  = test[features]
    y_test  = test["goal_diff"]
    
    model = Ridge(alpha=1.0)
    model.fit(X_train, y_train)
    
    y_pred = model.predict(X_test)
    
    rmse = mean_squared_error(y_test, y_pred, squared=False)
    mae  = mean_absolute_error(y_test, y_pred)
    
    print(f"Season {test_season}-{test_season+1} | RMSE: {rmse:.3f} | MAE: {mae:.3f}")
    
    season_results = test.copy()
    season_results["pred_goal_diff"] = y_pred
    season_results["pred_win_prob"]  = 1 / (1 + np.exp(-y_pred))  # sigmoid
    season_results["season_tested"] = f"{test_season}-{test_season+1}"
    results.append(season_results)

# -----------------------------
# 6. Concatenate all season predictions
# -----------------------------
all_results = pd.concat(results, ignore_index=True)

# -----------------------------
# 7. (Optional) Save predictions back to DB
# -----------------------------
# all_results.to_sql("team_vs_opponent_predictions", engine, if_exists="replace", index=False)

# -----------------------------
# 8. Quick check
# -----------------------------
print(all_results[["game_id", "team_id", "goal_diff", "pred_goal_diff", "pred_win_prob"]].head())
