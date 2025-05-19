import pandas as pd
import numpy as np
import os
import glob
import logging
import json
from datetime import datetime

# Import configuration
from config import (
    RAW_DATA_DIR, 
    PROCESSED_DATA_DIR,
    LOG_FILE, 
    LOG_LEVEL,
    PARAMETERS
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

# Create processed data directory if it doesn't exist
os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)

def load_latest_raw_data():
    """
    Load the most recent raw data files for each city and parameter.
    
    Returns:
        dict: Dictionary with city_parameter as keys and DataFrames as values
    """
    logger.info("Loading latest raw data files")
    
    # Get all CSV files in the raw data directory
    csv_files = glob.glob(f"{RAW_DATA_DIR}/*.csv")
    
    if not csv_files:
        logger.error("No raw data files found. Run extract.py first.")
        return None
    
    # Sort by filename which contains timestamp
    csv_files.sort(reverse=True)
    
    # Dictionary to store the latest data for each city-parameter combination
    latest_data = {}
    
    # Track which city-parameter combinations we've already found
    found_combinations = set()
    
    for file_path in csv_files:
        # Extract city and parameter from filename
        filename = os.path.basename(file_path)
        parts = filename.split('_')
        
        # Handle filenames with spaces in city names
        if len(parts) >= 3:
            if parts[0] == 'Los' and parts[1] == 'Angeles':
                city = 'Los Angeles'
                parameter = parts[2]
            elif parts[0] == 'New' and parts[1] == 'York':
                city = 'New York'
                parameter = parts[2]
            else:
                city = parts[0]
                parameter = parts[1]
            
            # Check if we already found this city-parameter combination
            combination = f"{city}_{parameter}"
            if combination not in found_combinations:
                # Load the CSV file as a DataFrame
                try:
                    df = pd.read_csv(file_path)
                    logger.info(f"Loaded data from {file_path}")
                    latest_data[combination] = df
                    found_combinations.add(combination)
                except Exception as e:
                    logger.error(f"Error loading {file_path}: {e}")
    
    logger.info(f"Loaded {len(latest_data)} city-parameter datasets")
    return latest_data

def clean_data(data_dict):
    """
    Clean the raw data by handling missing values and standardizing column names.
    
    Args:
        data_dict (dict): Dictionary with city_parameter as keys and DataFrames as values
        
    Returns:
        dict: Dictionary with cleaned DataFrames
    """
    logger.info("Starting data cleaning process")
    
    if not data_dict:
        logger.error("No data to clean")
        return None
    
    cleaned_data = {}
    
    for key, df in data_dict.items():
        logger.info(f"Cleaning dataset: {key}")
        
        # Make a copy to avoid modifying the original
        df_clean = df.copy()
        
        # Standardize column names
        df_clean.columns = [col.lower().replace('.', '_') for col in df_clean.columns]
        
        # Handle missing values
        for col in df_clean.columns:
            missing = df_clean[col].isna().sum()
            if missing > 0:
                logger.info(f"Found {missing} missing values in column {col}")
                
                # For numeric columns, fill with mean or median
                if pd.api.types.is_numeric_dtype(df_clean[col]):
                    median_value = df_clean[col].median()
                    df_clean[col].fillna(median_value, inplace=True)
                    logger.info(f"Filled missing values with median: {median_value}")
                else:
                    # For non-numeric columns, fill with most common value or "Unknown"
                    if len(df_clean[col].dropna()) > 0:
                        most_common = df_clean[col].value_counts().index[0]
                        df_clean[col].fillna(most_common, inplace=True)
                        logger.info(f"Filled missing values with most common value: {most_common}")
                    else:
                        df_clean[col].fillna("Unknown", inplace=True)
                        logger.info("Filled missing values with 'Unknown'")
        
        # Ensure we have the necessary columns for later processing
        required_columns = ['value', 'parameter']
        missing_columns = [col for col in required_columns if col not in df_clean.columns]
        
        if missing_columns:
            logger.warning(f"Dataset {key} is missing required columns: {missing_columns}")
            
            # Try to find alternative column names
            for missing_col in missing_columns:
                if missing_col == 'value':
                    alternatives = ['average', 'mean', 'concentration', 'result']
                    for alt in alternatives:
                        if alt in df_clean.columns:
                            df_clean['value'] = df_clean[alt]
                            logger.info(f"Used '{alt}' column for 'value'")
                            break
                elif missing_col == 'parameter':
                    # Extract parameter from the key (city_parameter)
                    parameter = key.split('_')[-1]
                    df_clean['parameter'] = parameter
                    logger.info(f"Added 'parameter' column with value '{parameter}'")
        
        # Add or normalize city column
        if 'city' not in df_clean.columns:
            city = key.split('_')[0]
            if city == 'Los' and 'Angeles' in key:
                city = 'Los Angeles'
            elif city == 'New' and 'York' in key:
                city = 'New York'
            df_clean['city'] = city
            logger.info(f"Added 'city' column with value '{city}'")
        
        # Check for outliers in the 'value' column if it exists and is numeric
        if 'value' in df_clean.columns and pd.api.types.is_numeric_dtype(df_clean['value']):
            # Define outliers as values more than 3 standard deviations from the mean
            mean = df_clean['value'].mean()
            std = df_clean['value'].std()
            
            lower_bound = mean - 3 * std
            upper_bound = mean + 3 * std
            
            # Count outliers
            outliers = df_clean[(df_clean['value'] < lower_bound) | (df_clean['value'] > upper_bound)]
            num_outliers = len(outliers)
            
            if num_outliers > 0:
                logger.info(f"Found {num_outliers} outliers in 'value' column")
                
                # Replace outliers with upper/lower bounds
                df_clean.loc[df_clean['value'] < lower_bound, 'value'] = lower_bound
                df_clean.loc[df_clean['value'] > upper_bound, 'value'] = upper_bound
                
                logger.info(f"Capped outliers to range [{lower_bound:.2f}, {upper_bound:.2f}]")
        
        cleaned_data[key] = df_clean
        logger.info(f"Cleaned dataset: {key}, rows: {len(df_clean)}, columns: {len(df_clean.columns)}")
    
    return cleaned_data

def calculate_aqi(concentration, parameter):
    """
    Calculate Air Quality Index (AQI) from pollutant concentration.
    
    Args:
        concentration (float): Pollutant concentration
        parameter (str): Pollutant parameter (pm25, o3, no2)
        
    Returns:
        tuple: (AQI value, AQI category)
    """
    # AQI breakpoints for different pollutants
    # Format: (Clow, Chigh, Ilow, Ihigh)
    # Units: PM2.5 in µg/m³, O3 and NO2 in ppb
    
    # PM2.5 breakpoints (24-hour average)
    pm25_breakpoints = [
        (0.0, 12.0, 0, 50),      # Good
        (12.1, 35.4, 51, 100),   # Moderate
        (35.5, 55.4, 101, 150),  # Unhealthy for Sensitive Groups
        (55.5, 150.4, 151, 200), # Unhealthy
        (150.5, 250.4, 201, 300), # Very Unhealthy
        (250.5, 500.4, 301, 500)  # Hazardous
    ]
    
    # Ozone breakpoints (8-hour average)
    o3_breakpoints = [
        (0, 54, 0, 50),         # Good
        (55, 70, 51, 100),      # Moderate
        (71, 85, 101, 150),     # Unhealthy for Sensitive Groups
        (86, 105, 151, 200),    # Unhealthy
        (106, 200, 201, 300),   # Very Unhealthy
        (201, 604, 301, 500)    # Hazardous
    ]
    
    # NO2 breakpoints (1-hour average)
    no2_breakpoints = [
        (0, 53, 0, 50),        # Good
        (54, 100, 51, 100),    # Moderate
        (101, 360, 101, 150),  # Unhealthy for Sensitive Groups
        (361, 649, 151, 200),  # Unhealthy
        (650, 1249, 201, 300), # Very Unhealthy
        (1250, 2049, 301, 500) # Hazardous
    ]
    
    # Select the appropriate breakpoints
    if parameter.lower() == 'pm25':
        breakpoints = pm25_breakpoints
    elif parameter.lower() == 'o3':
        breakpoints = o3_breakpoints
    elif parameter.lower() == 'no2':
        breakpoints = no2_breakpoints
    else:
        logger.warning(f"Unknown parameter: {parameter}, defaulting to PM2.5")
        breakpoints = pm25_breakpoints
    
    # AQI categories
    categories = [
        "Good",
        "Moderate",
        "Unhealthy for Sensitive Groups",
        "Unhealthy",
        "Very Unhealthy",
        "Hazardous"
    ]
    
    # Calculate AQI
    for i, (c_low, c_high, i_low, i_high) in enumerate(breakpoints):
        if c_low <= concentration <= c_high:
            aqi = ((i_high - i_low) / (c_high - c_low)) * (concentration - c_low) + i_low
            category = categories[i]
            return round(aqi), category
    
    # If concentration is higher than the highest breakpoint
    if concentration > breakpoints[-1][1]:
        return 500, "Hazardous (Beyond Index)"
    
    # If concentration is lower than the lowest breakpoint
    return 0, "Good (Below Index)"

def transform_to_aqi(cleaned_data):
    """
    Transform cleaned pollutant data to AQI values and categories.
    
    Args:
        cleaned_data (dict): Dictionary with cleaned DataFrames
        
    Returns:
        dict: Dictionary with transformed DataFrames including AQI values
    """
    logger.info("Transforming pollutant concentrations to AQI values")
    
    if not cleaned_data:
        logger.error("No cleaned data to transform")
        return None
    
    transformed_data = {}
    
    for key, df in cleaned_data.items():
        logger.info(f"Transforming dataset: {key}")
        
        # Make a copy to avoid modifying the original
        df_transformed = df.copy()
        
        # Check if we have the required columns
        if 'value' not in df_transformed.columns or 'parameter' not in df_transformed.columns:
            logger.warning(f"Dataset {key} missing required columns for AQI calculation")
            transformed_data[key] = df_transformed
            continue
        
        # Apply AQI calculation
        aqi_values = []
        aqi_categories = []
        
        for idx, row in df_transformed.iterrows():
            try:
                parameter = row['parameter'].lower() if isinstance(row['parameter'], str) else 'pm25'
                concentration = float(row['value'])
                aqi, category = calculate_aqi(concentration, parameter)
                aqi_values.append(aqi)
                aqi_categories.append(category)
            except Exception as e:
                logger.error(f"Error calculating AQI for row {idx}: {e}")
                aqi_values.append(None)
                aqi_categories.append("Unknown")
        
        # Add AQI values and categories to the DataFrame
        df_transformed['aqi'] = aqi_values
        df_transformed['aqi_category'] = aqi_categories
        
        # Add health recommendations based on AQI category
        health_recommendations = {
            "Good": "Air quality is satisfactory, and air pollution poses little or no risk.",
            "Moderate": "Air quality is acceptable. However, there may be a risk for some people, particularly those who are unusually sensitive to air pollution.",
            "Unhealthy for Sensitive Groups": "Members of sensitive groups may experience health effects. The general public is less likely to be affected.",
            "Unhealthy": "Some members of the general public may experience health effects; members of sensitive groups may experience more serious health effects.",
            "Very Unhealthy": "Health alert: The risk of health effects is increased for everyone.",
            "Hazardous": "Health warning of emergency conditions: everyone is more likely to be affected.",
            "Hazardous (Beyond Index)": "Health warning of emergency conditions: everyone is at risk of serious health effects.",
            "Unknown": "Unable to determine health risk due to missing or invalid data."
        }
        
        df_transformed['health_recommendation'] = df_transformed['aqi_category'].map(health_recommendations)
        
        transformed_data[key] = df_transformed
        logger.info(f"Transformed dataset: {key}, added AQI values and categories")
    
    return transformed_data

def add_geo_data(transformed_data):
    """
    Add or standardize geographical data for mapping purposes.
    
    Args:
        transformed_data (dict): Dictionary with transformed DataFrames
        
    Returns:
        dict: Dictionary with DataFrames including geographical data
    """
    logger.info("Adding geographical coordinates for mapping")
    
    if not transformed_data:
        logger.error("No transformed data to enhance")
        return None
    
    geo_data = {}
    
    # City coordinates
    city_coords = {
        "Los Angeles": {"latitude": 34.0522, "longitude": -118.2437},
        "New York": {"latitude": 40.7128, "longitude": -74.0060},
        "London": {"latitude": 51.5074, "longitude": -0.1278}
    }
    
    for key, df in transformed_data.items():
        logger.info(f"Adding geographical data to dataset: {key}")
        
        # Make a copy to avoid modifying the original
        df_geo = df.copy()
        
        # Check if we already have coordinates
        has_lat = any(col for col in df_geo.columns if 'lat' in col.lower())
        has_lon = any(col for col in df_geo.columns if 'lon' in col.lower())
        
        # If we don't have coordinates, add them based on city
        if not (has_lat and has_lon) and 'city' in df_geo.columns:
            for city, coords in city_coords.items():
                mask = df_geo['city'] == city
                if mask.any():
                    df_geo.loc[mask, 'latitude'] = coords['latitude']
                    df_geo.loc[mask, 'longitude'] = coords['longitude']
            
            logger.info(f"Added coordinates based on city names")
        
        # Make sure we have standardized column names for coordinates
        for col in df_geo.columns:
            if 'lat' in col.lower() and col != 'latitude':
                df_geo['latitude'] = df_geo[col]
                logger.info(f"Standardized '{col}' to 'latitude'")
            elif 'lon' in col.lower() and col != 'longitude':
                df_geo['longitude'] = df_geo[col]
                logger.info(f"Standardized '{col}' to 'longitude'")
        
        # Add district/neighborhood information (dummy for now)
        if 'district' not in df_geo.columns:
            df_geo['district'] = "Downtown"  # Simplified for the example
            logger.info("Added placeholder district information")
        
        geo_data[key] = df_geo
        logger.info(f"Added geographical data to dataset: {key}")
    
    return geo_data

def save_processed_data(processed_data):
    """
    Save processed data to CSV and JSON files.
    
    Args:
        processed_data (dict): Dictionary with fully processed DataFrames
    """
    logger.info("Saving processed data")
    
    if not processed_data:
        logger.error("No processed data to save")
        return
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Save individual city-parameter files
    for key, df in processed_data.items():
        csv_filename = f"{PROCESSED_DATA_DIR}/{key}_{timestamp}.csv"
        json_filename = f"{PROCESSED_DATA_DIR}/{key}_{timestamp}.json"
        
        # Save as CSV
        df.to_csv(csv_filename, index=False)
        logger.info(f"Saved processed data to {csv_filename}")
        
        # Save as JSON
        df_json = df.to_dict(orient='records')
        with open(json_filename, 'w') as f:
            json.dump(df_json, f, indent=2)
        logger.info(f"Saved processed data to {json_filename}")
    
    # Combine all data into a single dataset
    combined_df = pd.concat(processed_data.values(), ignore_index=True)
    
    # Save combined dataset
    combined_csv = f"{PROCESSED_DATA_DIR}/combined_{timestamp}.csv"
    combined_json = f"{PROCESSED_DATA_DIR}/combined_{timestamp}.json"
    
    combined_df.to_csv(combined_csv, index=False)
    logger.info(f"Saved combined processed data to {combined_csv}")
    
    combined_json_data = combined_df.to_dict(orient='records')
    with open(combined_json, 'w') as f:
        json.dump(combined_json_data, f, indent=2)
    logger.info(f"Saved combined processed data to {combined_json}")

def main():
    """Main function to orchestrate the data transformation process."""
    logger.info("Starting data transformation process")
    
    # Step 1: Load the latest raw data
    raw_data = load_latest_raw_data()
    if not raw_data:
        logger.error("Failed to load raw data. Exiting.")
        return
    
    # Step 2: Clean the data
    cleaned_data = clean_data(raw_data)
    if not cleaned_data:
        logger.error("Failed to clean data. Exiting.")
        return
    
    # Step 3: Transform data to AQI values
    transformed_data = transform_to_aqi(cleaned_data)
    if not transformed_data:
        logger.error("Failed to transform data. Exiting.")
        return
    
    # Step 4: Add geographical data
    processed_data = add_geo_data(transformed_data)
    if not processed_data:
        logger.error("Failed to add geographical data. Exiting.")
        return
    
    # Step 5: Save processed data
    save_processed_data(processed_data)
    
    logger.info("Data transformation process completed successfully")

if __name__ == "__main__":
    main()