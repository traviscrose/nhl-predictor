import os
import psycopg2

# Get DATABASE_URL from Render environment
DATABASE_URL = os.environ.get("DATABASE_URL")

if not DATABASE_URL:
    raise Exception("DATABASE_URL environment variable not set")

# Connect to Render Postgres
conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

# Read SQL schema file
with open("schema.sql", "r") as f:
    sql = f.read()

# Execute schema creation
cur.execute(sql)
conn.commit()
cur.close()
conn.close()

print("Database schema created successfully!")
