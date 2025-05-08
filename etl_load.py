import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import logging
import os

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_postgres_conn():
    """Get a connection string to PostgreSQL database."""
    conn = psycopg2.connect(
        dbname=os.getenv('POSTGRES_DB'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD'),
        host=os.getenv('POSTGRES_HOST'),
        port=os.getenv('POSTGRES_PORT')
    )
    return conn

def create_sqlalchemy_engine():
    """Create SQLAlchemy engine for pandas to interact with PostgreSQL."""
    engine = create_engine(
        f'postgresql://{os.getenv("POSTGRES_USER")}:{os.getenv("POSTGRES_PASSWORD")}@{os.getenv("POSTGRES_HOST")}:{os.getenv("POSTGRES_PORT")}/{os.getenv("POSTGRES_DB")}'
    )
    return engine

def load_data_to_postgres(df, table_name):
    """Load DataFrame to PostgreSQL as a new table."""
    engine = create_sqlalchemy_engine()
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    logger.info(f"Data loaded to PostgreSQL table: {table_name}")
    
# Fetch the data
    query = f"SELECT * FROM {table_name};"
    df = pd.read_sql_query(query, engine)
    # Display the result
    print(df)   
