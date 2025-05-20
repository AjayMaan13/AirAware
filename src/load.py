# src/load.py

import os
import logging
import json
import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
import glob
from datetime import datetime

# Import configuration
from config import (
    PROCESSED_DATA_DIR,
    LOG_FILE,
    LOG_LEVEL,
    DB_NAME,
    DB_USER,
    DB_PASSWORD,
    DB_HOST,
    DB_PORT
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

def connect_to_db():
    """
    Connect to the PostgreSQL database.
    
    Returns:
        connection: PostgreSQL database connection
    """
    try:
        logger.info(f"Connecting to database {DB_NAME} on {DB_HOST}")
        connection = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        logger.info("Database connection established")
        return connection
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        return None

def create_tables(connection):
    """
    Create database tables if they don't exist.
    
    Args:
        connection: PostgreSQL database connection
    """
    try:
        cursor = connection.cursor()
        
        # Read the schema file
        schema_path = os.path.join("../database", "schema.sql")
        with open(schema_path, "r") as schema_file:
            schema_sql = schema_file.read()
        
        # Execute the schema SQL
        cursor.execute(schema_sql)
        connection.commit()
        logger.info("Database tables created successfully")
    except Exception as e:
        logger.error(f"Error creating tables: {e}")
        connection.rollback()

def load_latest_processed_data():
    """
    Load the most recent processed data file.
    
    Returns:
        DataFrame: Combined processed data
    """
    try:
        # Look for combined data files
        combined_files = glob.glob(f"{PROCESSED_DATA_DIR}/combined_*.csv")
        
        if not combined_files:
            logger.warning("No combined processed data files found")
            return None
        
        # Sort files by timestamp (newest first)
        combined_files.sort(reverse=True)
        latest_file = combined_files[0]
        
        # Load the data
        logger.info(f"Loading processed data from {latest_file}")
        df = pd.read_csv(latest_file)
        logger.info(f"Loaded {len(df)} records from {latest_file}")
        
        return df
    except Exception as e:
        logger.error(f"Error loading processed data: {e}")
        return None

def load_locations(connection, df):
    """
    Load unique locations into the locations table.
    
    Args:
        connection: PostgreSQL database connection
        df: DataFrame with processed data
        
    Returns:
        dict: Mapping of (city, district) tuples to location_id values
    """
    try:
        cursor = connection.cursor()
        
        # Get unique location data
        if 'district' not in df.columns:
            df['district'] = 'Downtown'  # Default district
        
        location_columns = ['city', 'district', 'latitude', 'longitude']
        missing_columns = [col for col in location_columns if col not in df.columns]
        
        if missing_columns:
            logger.warning(f"Missing location columns: {missing_columns}")
            # Add missing columns with default values
            for col in missing_columns:
                if col == 'latitude' or col == 'longitude':
                    df[col] = 0.0
                else:
                    df[col] = 'Unknown'
        
        # Get unique locations
        locations = df[location_columns].drop_duplicates()
        logger.info(f"Found {len(locations)} unique locations")
        
        # Dictionary to store location_id mapping
        location_map = {}
        
        # For each location, insert if not exists and get the ID
        for idx, location in locations.iterrows():
            city = location['city']
            district = location['district']
            latitude = location['latitude']
            longitude = location['longitude']
            
            # Check if location already exists
            cursor.execute(
                """
                SELECT location_id FROM locations
                WHERE city = %s AND district = %s
                """,
                (city, district)
            )
            result = cursor.fetchone()
            
            if result:
                # Location exists, get the ID
                location_id = result[0]
                logger.info(f"Location '{city} - {district}' already exists with ID {location_id}")
            else:
                # Insert new location
                cursor.execute(
                    """
                    INSERT INTO locations (city, district, latitude, longitude)
                    VALUES (%s, %s, %s, %s)
                    RETURNING location_id
                    """,
                    (city, district, latitude, longitude)
                )
                location_id = cursor.fetchone()[0]
                logger.info(f"Inserted new location '{city} - {district}' with ID {location_id}")
            
            # Store mapping of location to ID
            location_map[(city, district)] = location_id
        
        connection.commit()
        logger.info(f"Loaded {len(location_map)} locations into database")
        
        return location_map
    except Exception as e:
        logger.error(f"Error loading locations: {e}")
        connection.rollback()
        return {}

def load_readings(connection, df, location_map):
    """
    Load air quality readings into the readings table.
    
    Args:
        connection: PostgreSQL database connection
        df: DataFrame with processed data
        location_map: Mapping of (city, district) tuples to location_id values
    """
    try:
        cursor = connection.cursor()
        
        # Prepare data for insertion
        readings_data = []
        
        # Check for required columns
        required_columns = ['city', 'parameter', 'value', 'aqi', 'aqi_category', 'health_recommendation']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            logger.warning(f"Missing required columns for readings: {missing_columns}")
            # Add missing columns with default values
            for col in missing_columns:
                if col in ['value', 'aqi']:
                    df[col] = 0
                elif col == 'aqi_category':
                    df[col] = 'Unknown'
                elif col == 'health_recommendation':
                    df[col] = 'No recommendation available'
                else:
                    df[col] = 'Unknown'
        
        # If district is not in df, add it with default value
        if 'district' not in df.columns:
            df['district'] = 'Downtown'
        
        # If date columns are not consistent, use current time
        date_column = None
        for col in df.columns:
            if 'date' in col.lower() or 'time' in col.lower():
                date_column = col
                break
        
        if date_column is None:
            logger.warning("No date/time column found, using current timestamp for all readings")
            now = datetime.now()
            df['timestamp'] = now
            date_column = 'timestamp'
        
        # Add a unit column if it doesn't exist
        if 'unit' not in df.columns:
            # Assign default units based on parameter
            def get_unit(parameter):
                if parameter.lower() == 'pm25':
                    return 'µg/m³'
                elif parameter.lower() in ['o3', 'no2', 'so2', 'co']:
                    return 'ppb'
                else:
                    return 'unknown'
            
            df['unit'] = df['parameter'].apply(get_unit)
        
        # Add source name if it doesn't exist
        if 'sourceName' not in df.columns and 'source_name' not in df.columns:
            df['source_name'] = 'Air Quality Monitoring System'
        elif 'sourceName' in df.columns:
            df.rename(columns={'sourceName': 'source_name'}, inplace=True)
        
        # Prepare the data for bulk insertion
        for idx, row in df.iterrows():
            try:
                # Get location_id from the mapping
                location_key = (row['city'], row['district'])
                location_id = location_map.get(location_key)
                
                if location_id is None:
                    logger.warning(f"Location not found for {location_key}, skipping reading")
                    continue
                
                # Parse timestamp
                try:
                    if pd.isna(row[date_column]):
                        timestamp = datetime.now()
                    elif isinstance(row[date_column], str):
                        timestamp = pd.to_datetime(row[date_column])
                    else:
                        timestamp = row[date_column]
                except:
                    timestamp = datetime.now()
                
                # Prepare reading data
                reading = (
                    location_id,
                    timestamp,
                    row['parameter'],
                    float(row['value']),
                    row['unit'],
                    int(row['aqi']) if not pd.isna(row['aqi']) else 0,
                    row['aqi_category'],
                    row['health_recommendation'],
                    row['source_name']
                )
                
                readings_data.append(reading)
            except Exception as e:
                logger.error(f"Error processing row {idx}: {e}")
        
        # Bulk insert readings
        if readings_data:
            execute_values(
                cursor,
                """
                INSERT INTO readings 
                (location_id, timestamp, parameter, value, unit, aqi, aqi_category, health_recommendation, source_name)
                VALUES %s
                """,
                readings_data
            )
            
            connection.commit()
            logger.info(f"Loaded {len(readings_data)} readings into database")
        else:
            logger.warning("No readings to load")
    except Exception as e:
        logger.error(f"Error loading readings: {e}")
        connection.rollback()

def main():
    """Main function to orchestrate data loading process."""
    logger.info("Starting data loading process")
    
    # Load processed data
    df = load_latest_processed_data()
    if df is None:
        logger.error("Failed to load processed data. Exiting.")
        return
    
    # Connect to database
    connection = connect_to_db()
    if connection is None:
        logger.error("Failed to connect to database. Exiting.")
        return
    
    try:
        # Create tables if they don't exist
        create_tables(connection)
        
        # Load locations and get mapping
        location_map = load_locations(connection, df)
        
        # Load readings
        load_readings(connection, df, location_map)
        
        logger.info("Data loading process completed successfully")
    except Exception as e:
        logger.error(f"Error in data loading process: {e}")
    finally:
        connection.close()
        logger.info("Database connection closed")

if __name__ == "__main__":
    main()