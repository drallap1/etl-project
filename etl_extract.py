import requests
import pandas as pd
import logging
import os

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fetch_data(api_url: str):
    """Fetch data from API and return as DataFrame."""
    try:
        response = requests.get(api_url)
        response.raise_for_status()  # Raise an exception for bad responses (4xx, 5xx)
        json_data = response.json()
        flattened_data = [flatten_json(record) for record in json_data]
        return pd.DataFrame(flattened_data)
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data from {api_url}: {e}")
        return e
def flatten_json(nested_json, prefix=''):
    """Recursively flattens a nested JSON object."""
    flat_dict = {}

    for key, value in nested_json.items():
        new_key = f"{prefix}{key}" if prefix == '' else f"{prefix}_{key}"
        
        if isinstance(value, dict):
            # Recursively flatten if the value is a dictionary
            flat_dict.update(flatten_json(value, new_key))
        elif isinstance(value, list):
            # If the value is a list, iterate through it
            for i, item in enumerate(value):
                flat_dict.update(flatten_json(item, f"{new_key}_{i}"))
        else:
            # If the value is a basic data type, just add it to the dictionary
            flat_dict[new_key] = value

    return flat_dict

def fetch_users():
    """Fetch users data and return as DataFrame."""
    api_url = os.getenv("USERS_URL")
    return fetch_data(api_url)

def fetch_posts():
    """Fetch posts data and return as DataFrame."""
    api_url = os.getenv("POSTS_URL")
    return fetch_data(api_url)
