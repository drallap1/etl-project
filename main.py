import logging
from etl_extract import fetch_users, fetch_posts
from etl_load import load_data_to_postgres
from etl_transform import transform_data
from etl_warehouse import load_transformed_dataframe_to_sqlserver 
from dotenv import load_dotenv
table = "FactUserPosts"
# Load the environment variables from the .env file
load_dotenv(dotenv_path='config.env')
# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_etl():
    # Extract
    logger.info("Fetching data from APIs...")
    users_df = fetch_users()
    posts_df = fetch_posts()
    
    if users_df is None or posts_df is None:
        logger.error("Failed to fetch data from APIs. Exiting...")
        return

    # Load into PostgreSQL tables (create table based on DataFrame structure)
    logger.info("Loading users data into PostgreSQL...")
    load_data_to_postgres(users_df, "staging_users")
    
    logger.info("Loading posts data into PostgreSQL...")
    load_data_to_postgres(posts_df, "staging_posts")

    logger.info("ETL process completed successfully.")
    #Transform data
    logger.info("Transforming data...")
    transformed_data = transform_data()

    # Load into SQL Server warehouse


    logger.info("Loading transformed data into SQL Server...")
    load_transformed_dataframe_to_sqlserver(table,transformed_data)

if __name__ == "__main__":
    run_etl()
