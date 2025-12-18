import os
import psycopg2
from urllib.parse import urlparse

# Get DATABASE_URL from environment
DATABASE_URL = os.environ.get("DATABASE_URL")

if not DATABASE_URL:
    raise Exception("DATABASE_URL environment variable not set")

# Parse the URL and convert to DSN format
result = urlparse(DATABASE_URL)
username = result.username
password = result.password
database = result.path[1:]  # remove leading '/'
hostname = result.hostname
port = result.port

# Connect with SSL enabled
conn = psycopg2.connect(
    dbname=database,
    user=username,
    password=password,
    host=hostname,
    port=port,
    sslmode="require"
)

cur = conn.cursor()

# Read and execute schema
with open("schema.sql", "r") as f:
    sql = f.read()

cur.execute(sql)
conn.commit()
cur.close()
conn.close()

print("Database schema created successfully!")
