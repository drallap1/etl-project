import psycopg2
import logging
import os
import pandas as pd
from dotenv import load_dotenv

# Load the environment variables from the .env file
load_dotenv(dotenv_path='config.env')
# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_postgres_conn():
    conn = psycopg2.connect(
        dbname=os.getenv('POSTGRES_DB'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD'),
        host=os.getenv('POSTGRES_HOST'),
        port=os.getenv('POSTGRES_PORT')
    )
    return conn

def transform_data():
    conn = get_postgres_conn()
    cursor = conn.cursor()
    transform_query = """
    SELECT p."userId" AS UserID, p.id AS PostID, p.title AS Title, p.body AS PostBody, u.name AS Name, u.username AS UserName, u.email AS EmailId
    FROM staging_posts p
    JOIN staging_users u ON u.id = p."userId" limit 1;
"""

    cursor.execute(transform_query)
    transformed_data = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]  # Get column names from cursor description
    df = pd.DataFrame(transformed_data, columns=column_names)

    cursor.close()
    conn.close()
    return df
