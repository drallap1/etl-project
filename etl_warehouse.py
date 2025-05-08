import urllib
from sqlalchemy import create_engine, text
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Connection parameters
MSSQL_HOST = 'localhost'
MSSQL_PORT = '1433'
MSSQL_USER = 'sa'
MSSQL_PASSWORD = 'MssqlP@ssword123'
DATABASE_NAME = 'warehouse_db'

params_master = urllib.parse.quote_plus(
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER={MSSQL_HOST},{MSSQL_PORT};"
    f"DATABASE=master;"
    f"UID={MSSQL_USER};"
    f"PWD={MSSQL_PASSWORD};"
    f"TrustServerCertificate=yes;"
    f"Connection Timeout=30;"
    f"Application Name=python-sqlalchemy"
)
engine_master = create_engine(f"mssql+pyodbc:///?odbc_connect={params_master}")

# Use AUTOCOMMIT for CREATE DATABASE
with engine_master.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
    conn.execute(
        text(f"IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '{DATABASE_NAME}') CREATE DATABASE {DATABASE_NAME};")
    )
    logger.info(f"Checked and created database '{DATABASE_NAME}' if it did not exist.")

# 2. Connect to the newly created (or existing) database
params_db = urllib.parse.quote_plus(
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER={MSSQL_HOST},{MSSQL_PORT};"
    f"DATABASE={DATABASE_NAME};"
    f"UID={MSSQL_USER};"
    f"PWD={MSSQL_PASSWORD};"
    f"TrustServerCertificate=yes;"
    f"Connection Timeout=30;"
    f"Application Name=python-sqlalchemy"
)
engine_db = create_engine(f"mssql+pyodbc:///?odbc_connect={params_db}")

# 3. Function to load dataframe to SQL Server after ensuring database exists
def load_transformed_dataframe_to_sqlserver(table_name, df: pd.DataFrame):
    #with engine_db.connect() as conn:
        # Optional: Clear existing rows to avoid duplicates (idempotency)
        # conn.execute(text(f"DELETE FROM {table_name}"))
        # logger.info(f"Cleared existing records from {table_name}.")

    # Load new data
    df.to_sql(table_name, engine_db, if_exists='append', index=False)
    logger.info(f"Transformed data loaded into {table_name}.")

    # Fetch and display the data
    query = f"SELECT * FROM {table_name};"
    df_result = pd.read_sql_query(query, engine_db)
    print(df_result)

# Example usage:
# df = pd.DataFrame({...})  # Your DataFrame here
# load_transformed_dataframe_to_sqlserver('FactUserPosts', df)
