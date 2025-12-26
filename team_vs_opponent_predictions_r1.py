import pandas as pd
from sklearn.linear_model import PoissonRegressor
from sklearn.metrics import mean_absolute_error
from db import engine

# -------------------------------------------------
# Config
# -------------------------------------------------

FEATURES = [
    # per-game rates
    "shots_pg",
    "hits_pg",
    "points_pg",
    "opp_shots_pg",
    "opp_hits_pg",
    "opp_points_pg",

    # interactions
    "shot_pressure",
    "home_offense",

    # environment
    "adj_points_pg",
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
    g.game_date AS date,
    g.season
FROM team_vs_opponent t
JOIN games g
  ON t.game_id = g.id
ORDER BY g.game_date;
"""

df = pd.read_sql(query, engine)
df["date"] = pd.to_datetime(df["date"])

# -------------------------------------------------
# 2. Sanity checks
# -------------------------------------------------

RAW_FEATURES = [
    "shots_last5",
    "hits_last5",
    "points_last5",
    "opp_shots_last5",
    "opp_hits_last5",
    "opp_points_last5",
    "home_away",
]

required_cols = RAW_FEATURES + [TARGET, "season", "date"]

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

# --------------------------
# Rate features (Poisson-friendly)
# --------------------------

df["shots_pg"] = df["shots_last5"] / 5
df["hits_pg"] = df["hits_last5"] / 5
df["points_pg"] = df["points_last5"] / 5

df["opp_shots_pg"] = df["opp_shots_last5"] / 5
df["opp_hits_pg"] = df["opp_hits_last5"] / 5
df["opp_points_pg"] = df["opp_points_last5"] / 5

# --------------------------
# Interaction features
# --------------------------

# Pace / chaos proxy
df["shot_pressure"] = (
    df["shots_pg"] * df["opp_shots_pg"]
)

# Home boost to offense
df["home_offense"] = (
    df["home_away"] * df["points_pg"]
)

# --------------------------
# Season scoring environment normalization
# --------------------------

season_goal_env = df.groupby("season")["goals"].mean()
df["season_goal_env"] = df["season"].map(season_goal_env)

season_goal_env = df.groupby("season")["goals"].mean()
df["season_goal_env"] = df["season"].map(season_goal_env)

df["adj_points_pg"] = (
    df["points_pg"] - df["season_goal_env"]
)


missing_engineered = [c for c in FEATURES if c not in df.columns]
if missing_engineered:
    raise RuntimeError(
        f"Missing engineered features: {missing_engineered}"
    )
    
FEATURE_CLIP = {
    "shots_pg": (10, 45),
    "hits_pg": (5, 40),
    "points_pg": (0.5, 6),
    "opp_shots_pg": (10, 45),
    "opp_hits_pg": (5, 40),
    "opp_points_pg": (0.5, 6),
    "shot_pressure": (100, 2000),
    "home_offense": (0, 6),
    "adj_points_pg": (-3, 3),
}

for col, (lo, hi) in FEATURE_CLIP.items():
    df[col] = df[col].clip(lo, hi)


# -------------------------------------------------
# 4. Rolling season backtest
# -------------------------------------------------

X = df[FEATURES]

if not np.isfinite(X.to_numpy()).all():
    raise RuntimeError("Non-finite values detected in features")

print("Feature std dev:")
print(X.std().round(3))


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
    
    baseline = y_train.mean()
    baseline_mae = mean_absolute_error(
    y_test, [baseline] * len(y_test)
    )

    print(f"Baseline MAE: {baseline_mae:.3f}")


    model = PoissonRegressor(alpha=0.05, max_iter=5000)
    model.fit(X_train, y_train)

    test = test.copy()
    test["pred_goals"] = (
    model.predict(X_test)
    .clip(0, 5.5)
    )


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
