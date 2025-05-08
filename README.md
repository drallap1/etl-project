# ETL Pipeline: JSONPlaceholder to PostgreSQL and SQL Server

## Overview

This project demonstrates an ETL (Extract, Transform, Load) pipeline that:

1. Fetches data from the JSONPlaceholder API.
2. Loads raw data into PostgreSQL staging tables.
3. Transforms and joins the data.
4. Loads the transformed data into a SQL Server data warehouse.

## Project Structure

1. Data extracted from the json is converted to a dataframe.
2. Staging table for both users and posts are built on top of the dataframes.
3. Both the tables are Joined on usedId in the transformation and are loaded into the data warehouse

## Incremental Loads

1. During ingestion job id can be tracked which will be the current ingestion timestamp.
2. Based on the timestap of previous job id and the current job id data can be ingested at the extraction level.
3. System can have metadata which can track the previous job id and using that if an API contains a timestamp, using only new data can be extracted from the source.

## Additional tasks that can be Performed

1. A metadata table can be created to track all the ingestion details.
2. Based on the merge strategy during staging rows can be merged based on the primary keys.

## Other details

1. Docker engine must be running
2. Run the docker-compose file using docker-compose up -d
