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
with get_conn() as conn:
    query = """
    SELECT ps.player_id, ps.game_id, ps.team_id, ps.goals, ps.assists, ps.points
    FROM player_stats ps
    LIMIT 5
    """
    df = pd.read_sql(query, conn)
print(df)
