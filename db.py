from sqlalchemy import create_engine
import os
import psycopg2
from psycopg2.extras import RealDictCursor

def get_conn():
    return psycopg2.connect(
        os.environ["DATABASE_URL"],
        cursor_factory=RealDictCursor
    )

engine = create_engine(DATABASE_URL, echo=False)
