import pandas as pd
from sklearn.linear_model import PoissonRegressor
from sklearn.metrics import mean_absolute_error
from db import engine

# -------------------------------------------------
# Config
# -------------------------------------------------

FEATURES = [
    # team offensive
    "shots_last5",
    "hits_last5",
    "points_last5",
    # opponent offensive
    "opp_shots_last5",
    "opp_hits_last5",
    "opp_points_last5",
    # home/away
    "home_away",
    # team defense
    "def_blocked_shots_last5",
    "def_hits_last5",
    "def_takeaways_last5",
    "def_giveaways_last5",
    "def_plus_minus_last5",
    # opponent defense
    "opp_def_blocked_shots_last5",
    "opp_def_hits_last5",
    "opp_def_takeaways_last5",
    "opp_def_giveaways_last5",
    "opp_def_plus_minus_last5",
]

TARGET = "goals"

# -------------------------------------------------
# 1. Load main data
# -------------------------------------------------

query_main = """
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

df_main = pd.read_sql(query_main, engine)
df_main["date"] = pd.to_datetime(df_main["date"])

# -------------------------------------------------
# 2. Load defense stats
# -------------------------------------------------

query_def = """
SELECT
    game_id,
    team_id,
    SUM(blocked_shots) AS blocked_shots,
    SUM(hits) AS hits,
    SUM(takeaways) AS takeaways,
    SUM(giveaways) AS giveaways,
    SUM(plus_minus) AS plus_minus
FROM team_game_defense
GROUP BY game_id, team_id
ORDER BY game_id;
"""

df_def = pd.read_sql(query_def, engine)

# -------------------------------------------------
# 3. Merge main + team defense stats
# -------------------------------------------------

df = df_main.merge(df_def, on=["game_id", "team_id"], how="left", suffixes=("", "_def"))

# -------------------------------------------------
# 4. Compute rolling last5 team defense features
# -------------------------------------------------

df = df.sort_values(["team_id", "date"])
def_features = ["blocked_shots", "hits", "takeaways", "giveaways", "plus_minus"]
for col in def_features:
    df[f"def_{col}_last5"] = df.groupby("team_id")[col].transform(lambda x: x.shift().rolling(5, min_periods=1).sum())

# -------------------------------------------------
# 5. Merge opponent defense stats
# -------------------------------------------------

df = df.merge(
    df_def.rename(
        columns={
            "team_id": "opp_team_id",
            "blocked_shots": "opp_blocked_shots",
            "hits": "opp_hits",
            "takeaways": "opp_takeaways",
            "giveaways": "opp_giveaways",
            "plus_minus": "opp_plus_minus",
        }
    ),
    on=["game_id", "opp_team_id"],
    how="left",
)

# -------------------------------------------------
# 6. Compute rolling last5 opponent defense features
# -------------------------------------------------

df = df.sort_values(["opp_team_id", "date"])
opp_def_features = ["opp_blocked_shots", "opp_hits", "opp_takeaways", "opp_giveaways", "opp_plus_minus"]
for col in opp_def_features:
    df[f"opp_def_{col.split('_')[1]}_last5"] = df.groupby("opp_team_id")[col].transform(lambda x: x.shift().rolling(5, min_periods=1).sum())

# -------------------------------------------------
# 7. Sanity checks
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
# 8. Preprocessing
# -------------------------------------------------

df["home_away"] = df["home_away"].map({"home": 1, "away": 0})
numeric_cols = df.select_dtypes(include="number").columns
df[numeric_cols] = df[numeric_cols].fillna(0)

# -------------------------------------------------
# 9. Rolling season backtest
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
# 10. Results
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
