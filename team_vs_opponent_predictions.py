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
    "opp_shots_last5",
    "opp_hits_last5",
    "opp_points_last5",
    "home_away",
]

TARGET = "goals"

# -------------------------------------------------
# 1. Load data
# -------------------------------------------------

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
    t.shots_last5,
    t.hits_last5,
    t.points_last5,
    t.opp_shots_last5,
    t.opp_hits_last5,
    t.opp_points_last5,
    g.game_date,
    g.season_id
FROM team_vs_opponent t
JOIN games g
  ON t.game_id = g.id
ORDER BY g.date;
"""

df = pd.read_sql(query, engine)
df["date"] = pd.to_datetime(df["date"])

# -------------------------------------------------
# 2. Sanity checks
# -------------------------------------------------

required_cols = FEATURES + [TARGET, "season", "date"]
missing = [c for c in required_cols if c not in df.columns]
if missing:
    raise RuntimeError(f"Missing required columns: {missing}")

if df.empty:
    raise RuntimeError("Loaded dataframe is empty")

if df["season"].isna().any():
    raise RuntimeError("Null season values detected")

print("Loaded rows:", len(df))
print("Seasons:", sorted(df["season"].unique()))
print("Rows per season:")
print(df.groupby("season").size())

# -------------------------------------------------
# 3. Preprocessing
# -------------------------------------------------

df["home_away"] = df["home_away"].map({"home": 1, "away": 0})

numeric_cols = df.select_dtypes(include="number").columns
df[numeric_cols] = df[numeric_cols].fillna(0)

# -------------------------------------------------
# 4. Rolling season backtest
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
# 5. Results
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
