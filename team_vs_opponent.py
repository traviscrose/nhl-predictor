import os
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor

# ------------------ DATABASE CONNECTION ------------------
DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable not set")

def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)

# ------------------ LOAD PLAYER STATS + GAME INFO ------------------
with get_conn() as conn:
    query = """
    SELECT ps.*, p.position, g.id AS game_id, g.home_team_id, g.away_team_id
    FROM player_stats ps
    JOIN players p ON ps.player_id = p.id
    JOIN games g ON ps.game_id = g.id
    """
    df = pd.read_sql(query, conn)

# ------------------ ENSURE NUMERIC TYPES ------------------
# Convert IDs to numeric
df['game_id'] = pd.to_numeric(df['game_id'], errors='coerce')
df['team_id'] = pd.to_numeric(df['team_id'], errors='coerce')

# Convert player stats to numeric
numeric_cols = ['goals', 'assists', 'points', 'shots', 'hits']
for col in numeric_cols:
    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

# Convert time_on_ice to minutes
def toi_to_minutes(toi_str):
    if not toi_str or ':' not in toi_str:
        return 0
    m, s = map(int, toi_str.split(":"))
    return m + s / 60

df['toi_minutes'] = df['time_on_ice'].apply(toi_to_minutes)

# ------------------ AGGREGATE SKATERS ------------------
skaters = df[df['position'] != 'G']
team_stats = skaters.groupby(['game_id', 'team_id']).agg({
    'goals': 'sum',
    'assists': 'sum',
    'points': 'sum',
    'shots': 'sum',
    'hits': 'sum',
    'toi_minutes': 'sum'
}).reset_index()

# ------------------ AGGREGATE GOALIES ------------------
goalies = df[df['position'] == 'G']
goalie_stats = goalies.groupby(['game_id', 'team_id']).agg({
    'goals': 'sum',   # goals against
    'shots': 'sum',   # shots against
    'toi_minutes': 'sum'
}).reset_index().rename(columns={
    'goals': 'goals_against',
    'shots': 'shots_against',
    'toi_minutes': 'goalie_toi'
})

# Merge skaters + goalies
team_game_stats = pd.merge(team_stats, goalie_stats, on=['game_id', 'team_id'], how='left')

# Fill missing goalie stats
for col in ['goals_against', 'shots_against', 'goalie_toi']:
    team_game_stats[col] = pd.to_numeric(team_game_stats[col], errors='coerce').fillna(0)

# ------------------ ADD HOME/AWAY INFO ------------------
with get_conn() as conn:
    games_df = pd.read_sql("SELECT id AS game_id, home_team_id, away_team_id FROM games", conn)
games_df['game_id'] = pd.to_numeric(games_df['game_id'], errors='coerce')

team_game_stats = team_game_stats.merge(games_df, on='game_id', how='left')
team_game_stats['home_away'] = team_game_stats.apply(
    lambda row: 'home' if row['team_id'] == row['home_team_id'] else 'away', axis=1
)

# ------------------ MERGE OPPONENT STATS ------------------
opponent_stats = team_game_stats[['game_id', 'team_id', 'goals', 'shots', 'hits', 'points']].copy()
opponent_stats = opponent_stats.rename(columns={
    'team_id': 'opp_team_id',
    'goals': 'opp_goals',
    'shots': 'opp_shots',
    'hits': 'opp_hits',
    'points': 'opp_points'
})

team_vs_opponent = pd.merge(
    team_game_stats, opponent_stats,
    left_on=['game_id', 'team_id'],
    right_on=['game_id', 'opp_team_id'],
    how='left'
)

# Drop rows where team_id == opp_team_id (self-merge)
team_vs_opponent = team_vs_opponent[team_vs_opponent['team_id'] != team_vs_opponent['opp_team_id']]

# ------------------ ENSURE NUMERIC TYPES AGAIN ------------------
all_numeric_cols = ['goals', 'assists', 'points', 'shots', 'hits', 'toi_minutes',
                    'goals_against', 'shots_against', 'goalie_toi',
                    'opp_goals', 'opp_shots', 'opp_hits', 'opp_points']
for col in all_numeric_cols:
    team_vs_opponent[col] = pd.to_numeric(team_vs_opponent[col], errors='coerce').fillna(0)

# ------------------ COMPUTE ROLLING FEATURES ------------------
team_vs_opponent = team_vs_opponent.sort_values(['team_id', 'game_id'])
for col in ['goals', 'goals_against', 'shots', 'hits', 'points']:
    team_vs_opponent[f'{col}_last5'] = team_vs_opponent.groupby('team_id')[col]\
        .rolling(5, min_periods=1).mean().reset_index(0, drop=True)

# ------------------ FINAL DATASET ------------------
print(team_vs_opponent.head())
