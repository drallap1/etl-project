import os
import pyodbc
import pandas as pd
import logging
from sqlalchemy import create_engine, text

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
driver ='SQL Server Native Client 11.0'
from sqlalchemy import create_engine
import urllib



def load_transformed_dataframe_to_sqlserver(table_name,df: pd.DataFrame):
    """Load DataFrame into FactUserPosts table."""
    
    params = urllib.parse.quote_plus(
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=localhost,1433;"
        "DATABASE=warehouse_db;"
        "UID=sa;"
        "PWD=MssqlP@ssword123;"
        "TrustServerCertificate=yes;"
        "Authentication=SqlPassword;"
        "Connection Timeout=30;"
        "Application Name=python-sqlalchemy"
    )

    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
        #engine = get_sqlalchemy_engine_mssql()

    # Optional: Clear existing rows to avoid duplicates (idempotency)
    with engine.connect() as conn:
        conn.execute(text("DELETE FROM FactUserPosts"))
        logger.info("Cleared existing records from FactUserPosts.")

    # Load new data
    df.to_sql(table_name, engine, if_exists='append', index=False)
    logger.info("Transformed data loaded into FactUserPosts.")

    # Fetch the data
    query = f"SELECT * FROM {table_name};"
    df = pd.read_sql_query(query, engine)
    # Display the result
    print(df)   
