import requests
import json
import pandas as pd
import os
import logging
from datetime import datetime

# Import configuration
from config import (
    OPENAQ_API_KEY, AIRNOW_API_KEY, 
    OPENAQ_V3_ENDPOINT, AIRNOW_ENDPOINT,
    CITIES, PARAMETERS, 
    LA_ZIP, RAW_DATA_DIR,
    LOG_FILE, LOG_LEVEL
)

# Set up logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Create data directory if it doesn't exist
os.makedirs(RAW_DATA_DIR, exist_ok=True)

def fetch_air_quality_data(city, parameter, limit=100):
    """
    Fetch air quality data from OpenAQ API v3 for a specific city and parameter.
    
    Args:
        city (str): City name
        parameter (str): Pollutant parameter (pm25, o3, etc.)
        limit (int): Number of results to return
        
    Returns:
        dict: JSON response from API
    """
    params = {
        "city": city,
        "parameter": parameter,
        "limit": limit,
        "sort": "desc",  # v3 uses 'sort' instead of 'order_by'
    }
    
    headers = {
        "X-API-Key": OPENAQ_API_KEY,
        "Accept": "application/json"
    }
    
    try:
        logger.info(f"Fetching {parameter} data for {city} using OpenAQ v3 API")
        response = requests.get(OPENAQ_V3_ENDPOINT, params=params, headers=headers)
        
        # Log the response status for debugging
        logger.info(f"Response status code: {response.status_code}")
        
        response.raise_for_status()  # Raise an exception for bad responses
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data from OpenAQ v3: {e}")
        # Log more details about the error for debugging
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"Response content: {e.response.text}")
        return None

def save_data(data, city, parameter):
    """
    Save raw data to CSV and JSON files.
    
    Args:
        data (dict): API response data
        city (str): City name
        parameter (str): Pollutant parameter
    """
    # Handle different response structure in v3
    if not data:
        logger.warning(f"No data to save for {city} - {parameter}")
        return
    
    # Check for v3 API structure (results are directly in the response)
    if 'results' in data:
        results = data['results']
    # Check for v3 API structure (measurements may be in 'data')
    elif 'data' in data:
        results = data['data']
    else:
        logger.warning(f"Unexpected data structure for {city} - {parameter}")
        return
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_filename = f"{RAW_DATA_DIR}/{city}_{parameter}_{timestamp}.json"
    csv_filename = f"{RAW_DATA_DIR}/{city}_{parameter}_{timestamp}.csv"
    
    # Save raw JSON
    with open(json_filename, 'w') as f:
        json.dump(data, f)
    
    # Convert to DataFrame and save as CSV
    if results:
        df = pd.json_normalize(results)
        df.to_csv(csv_filename, index=False)
        logger.info(f"Saved {len(results)} records to {csv_filename}")
    else:
        logger.warning(f"No results found for {city} - {parameter}")

def try_alternative_api():
    """
    Attempts to use AirNow API as an alternative if OpenAQ doesn't work.
    """
    logger.info("Attempting to use AirNow API as alternative")
    
    # AirNow API parameters
    params = {
        "format": "application/json",
        "zipCode": LA_ZIP,  # Los Angeles zip code from config
        "distance": 25,
        "API_KEY": AIRNOW_API_KEY
    }
    
    try:
        logger.info("Trying AirNow API")
        response = requests.get(AIRNOW_ENDPOINT, params=params)
        logger.info(f"AirNow response status: {response.status_code}")
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error with AirNow API: {e}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"AirNow response content: {e.response.text}")
        return None

def fetch_dummy_data():
    """
    Create dummy data if all API methods fail.
    This ensures the pipeline can continue to subsequent phases for testing.
    """
    logger.info("Creating dummy air quality data for testing")
    
    for city in CITIES:
        for parameter in PARAMETERS:
            # Create a simple dummy data structure
            dummy_data = {
                "data": [
                    {
                        "location": f"{city} Downtown",
                        "parameter": parameter,
                        "value": 35.0 if parameter == "pm25" else (45.0 if parameter == "o3" else 25.0),
                        "unit": "µg/m³" if parameter == "pm25" else "ppb",
                        "coordinates": {
                            "latitude": 34.05 if city == "Los Angeles" else (40.71 if city == "New York" else 51.51),
                            "longitude": -118.24 if city == "Los Angeles" else (-74.01 if city == "New York" else -0.13)
                        },
                        "date": {
                            "utc": datetime.utcnow().isoformat(),
                            "local": datetime.now().isoformat()
                        },
                        "sourceName": "Dummy Data Generator"
                    }
                    for _ in range(10)  # Generate 10 records
                ]
            }
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            json_filename = f"{RAW_DATA_DIR}/{city}_{parameter}_{timestamp}_dummy.json"
            csv_filename = f"{RAW_DATA_DIR}/{city}_{parameter}_{timestamp}_dummy.csv"
            
            # Save dummy JSON
            with open(json_filename, 'w') as f:
                json.dump(dummy_data, f)
            
            # Convert to DataFrame and save as CSV
            df = pd.json_normalize(dummy_data['data'])
            df.to_csv(csv_filename, index=False)
            logger.info(f"Saved 10 dummy records to {csv_filename}")

def main():
    """Main function to extract air quality data."""
    success = False
    
    # First try OpenAQ v3
    for city in CITIES:
        for parameter in PARAMETERS:
            data = fetch_air_quality_data(city, parameter)
            if data:
                save_data(data, city, parameter)
                success = True
    
    # If OpenAQ v3 didn't work, try AirNow API
    if not success:
        logger.warning("OpenAQ API v3 failed. Trying AirNow API...")
        alternative_data = try_alternative_api()
        if alternative_data:
            # Process the alternative API data
            logger.info("Successfully retrieved data from AirNow API")
            
            # Save AirNow data
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            json_filename = f"{RAW_DATA_DIR}/airnow_{timestamp}.json"
            with open(json_filename, 'w') as f:
                json.dump(alternative_data, f)
            
            logger.info(f"Saved AirNow API data to {json_filename}")
            success = True
    
    # If both APIs failed, generate dummy data for testing
    if not success:
        logger.warning("All APIs failed. Generating dummy data for testing...")
        fetch_dummy_data()
        logger.info("Dummy data generation completed")

if __name__ == "__main__":
    logger.info("Starting data extraction process")
    main()
    logger.info("Data extraction completed")