from sqlalchemy import create_engine
import psycopg2
from psycopg2.extras import RealDictCursor

DB_USER = "nhl_user"
DB_PASSWORD = "nhl_pass"
DB_HOST = "NUC-nhl-predictor-db"
DB_PORT = "5432"
DB_NAME = "nhl_predictor"

DATABASE_URL = (
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}"
    f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True
)

def get_conn():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        cursor_factory=RealDictCursor
    )
