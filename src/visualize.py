# src/visualize.py

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import os
import logging
import psycopg2
from psycopg2 import sql
from datetime import datetime, timedelta
import matplotlib.dates as mdates

# Import configuration
from config import (
    DB_NAME,
    DB_USER,
    DB_PASSWORD,
    DB_HOST,
    DB_PORT,
    LOG_FILE,
    LOG_LEVEL,
    VISUALIZATION_OUTPUT_DIR
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

# Set plot style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 8)

# Create visualization output directory if it doesn't exist
os.makedirs(VISUALIZATION_OUTPUT_DIR, exist_ok=True)

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

def get_latest_readings(connection, days=7):
    """
    Get the latest air quality readings from the database.
    
    Args:
        connection: PostgreSQL database connection
        days: Number of days of data to retrieve
        
    Returns:
        DataFrame: Latest air quality readings
    """
    try:
        # Calculate start date for filtering
        start_date = datetime.now() - timedelta(days=days)
        
        # SQL query to join readings with locations
        query = """
        SELECT 
            r.reading_id,
            r.timestamp,
            r.parameter,
            r.value,
            r.unit,
            r.aqi,
            r.aqi_category,
            r.health_recommendation,
            l.city,
            l.district,
            l.latitude,
            l.longitude
        FROM readings r
        JOIN locations l ON r.location_id = l.location_id
        WHERE r.timestamp >= %s
        ORDER BY r.timestamp DESC
        """
        
        # Execute query
        df = pd.read_sql_query(query, connection, params=(start_date,))
        
        logger.info(f"Retrieved {len(df)} readings from the past {days} days")
        return df
    except Exception as e:
        logger.error(f"Error retrieving data from database: {e}")
        return pd.DataFrame()

def create_time_series_chart(df, parameter, filename):
    """
    Create a time series chart of AQI values by city.
    
    Args:
        df: DataFrame with air quality readings
        parameter: Air quality parameter to visualize (pm25, o3, no2)
        filename: Output filename
    """
    try:
        # Filter data for the specified parameter
        param_df = df[df['parameter'] == parameter].copy()
        
        if param_df.empty:
            logger.warning(f"No data available for parameter: {parameter}")
            return
        
        # Ensure timestamp is datetime format
        param_df['timestamp'] = pd.to_datetime(param_df['timestamp'])
        
        # Create figure
        plt.figure(figsize=(14, 8))
        
        # Plot each city
        for city in param_df['city'].unique():
            city_data = param_df[param_df['city'] == city]
            plt.plot(city_data['timestamp'], city_data['aqi'], marker='o', linestyle='-', label=city)
        
        # Add AQI category background colors
        plt.axhspan(0, 50, alpha=0.2, color='green', label='Good')
        plt.axhspan(51, 100, alpha=0.2, color='yellow', label='Moderate')
        plt.axhspan(101, 150, alpha=0.2, color='orange', label='Unhealthy for Sensitive Groups')
        plt.axhspan(151, 200, alpha=0.2, color='red', label='Unhealthy')
        plt.axhspan(201, 300, alpha=0.2, color='purple', label='Very Unhealthy')
        plt.axhspan(301, 500, alpha=0.2, color='maroon', label='Hazardous')
        
        # Set labels and title
        plt.xlabel('Date', fontsize=12)
        plt.ylabel('Air Quality Index (AQI)', fontsize=12)
        plt.title(f'AQI Trend for {parameter.upper()} by City', fontsize=14)
        
        # Format x-axis dates
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        plt.gca().xaxis.set_major_locator(mdates.DayLocator())
        plt.gcf().autofmt_xdate()
        
        # Add grid and legend
        plt.grid(True, alpha=0.3)
        plt.legend(loc='best')
        
        # Add explanatory text
        plt.figtext(0.02, 0.02, f'Data as of {datetime.now().strftime("%Y-%m-%d %H:%M")}', fontsize=8)
        
        # Save figure
        output_path = os.path.join(VISUALIZATION_OUTPUT_DIR, f'{filename}.png')
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Created time series chart for {parameter} at {output_path}")
    except Exception as e:
        logger.error(f"Error creating time series chart: {e}")

def create_aqi_comparison_chart(df, filename):
    """
    Create a bar chart comparing the average AQI by city and parameter.
    
    Args:
        df: DataFrame with air quality readings
        filename: Output filename
    """
    try:
        # Group data by city and parameter to calculate average AQI
        grouped_df = df.groupby(['city', 'parameter'])['aqi'].mean().reset_index()
        
        if grouped_df.empty:
            logger.warning("No data available for AQI comparison chart")
            return
        
        # Pivot data for plotting
        pivot_df = pd.pivot_table(
            grouped_df,
            values='aqi',
            index='city',
            columns='parameter',
            aggfunc='mean'
        ).reset_index()
        
        # Create figure
        plt.figure(figsize=(12, 8))
        
        # Set width of bars
        bar_width = 0.25
        index = np.arange(len(pivot_df['city']))
        
        # Plot bars for each parameter
        param_columns = [col for col in pivot_df.columns if col != 'city']
        for i, param in enumerate(param_columns):
            plt.bar(
                index + i * bar_width,
                pivot_df[param],
                bar_width,
                label=param.upper()
            )
        
        # Set labels and title
        plt.xlabel('City', fontsize=12)
        plt.ylabel('Average Air Quality Index (AQI)', fontsize=12)
        plt.title('Average AQI by City and Pollutant', fontsize=14)
        
        # Set x-axis ticks
        plt.xticks(index + bar_width, pivot_df['city'])
        
        # Add grid and legend
        plt.grid(True, alpha=0.3, axis='y')
        plt.legend(loc='best')
        
        # Add AQI level reference lines
        plt.axhline(y=50, color='green', linestyle='--', alpha=0.5, label='Good threshold')
        plt.axhline(y=100, color='yellow', linestyle='--', alpha=0.5, label='Moderate threshold')
        plt.axhline(y=150, color='orange', linestyle='--', alpha=0.5, label='Unhealthy for Sensitive Groups threshold')
        
        # Add explanatory text
        plt.figtext(0.02, 0.02, f'Data as of {datetime.now().strftime("%Y-%m-%d %H:%M")}', fontsize=8)
        
        # Save figure
        output_path = os.path.join(VISUALIZATION_OUTPUT_DIR, f'{filename}.png')
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Created AQI comparison chart at {output_path}")
    except Exception as e:
        logger.error(f"Error creating AQI comparison chart: {e}")

def create_aqi_distribution_chart(df, filename):
    """
    Create a histogram showing the distribution of AQI values by category.
    
    Args:
        df: DataFrame with air quality readings
        filename: Output filename
    """
    try:
        # Ensure we have AQI category data
        if 'aqi_category' not in df.columns or df.empty:
            logger.warning("No AQI category data available for distribution chart")
            return
        
        # Create figure
        plt.figure(figsize=(14, 8))
        
        # Define category order and colors
        categories = [
            'Good',
            'Moderate',
            'Unhealthy for Sensitive Groups',
            'Unhealthy',
            'Very Unhealthy',
            'Hazardous'
        ]
        colors = ['green', 'yellow', 'orange', 'red', 'purple', 'maroon']
        
        # Count occurrences of each category
        category_counts = df['aqi_category'].value_counts().reindex(categories, fill_value=0)
        
        # Create bar chart
        bars = plt.bar(
            category_counts.index,
            category_counts.values,
            color=colors[:len(category_counts)]
        )
        
        # Add count labels on top of bars
        for bar in bars:
            height = bar.get_height()
            plt.text(
                bar.get_x() + bar.get_width()/2.,
                height + 0.5,
                f'{int(height)}',
                ha='center',
                va='bottom'
            )
        
        # Set labels and title
        plt.xlabel('AQI Category', fontsize=12)
        plt.ylabel('Number of Readings', fontsize=12)
        plt.title('Distribution of Air Quality Readings by AQI Category', fontsize=14)
        
        # Rotate x-axis labels for better readability
        plt.xticks(rotation=45, ha='right')
        
        # Add grid
        plt.grid(True, alpha=0.3, axis='y')
        
        # Add explanatory text
        plt.figtext(0.02, 0.02, f'Data as of {datetime.now().strftime("%Y-%m-%d %H:%M")}', fontsize=8)
        
        # Adjust layout for rotated labels
        plt.tight_layout()
        
        # Save figure
        output_path = os.path.join(VISUALIZATION_OUTPUT_DIR, f'{filename}.png')
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Created AQI distribution chart at {output_path}")
    except Exception as e:
        logger.error(f"Error creating AQI distribution chart: {e}")

def create_aqi_heatmap(df, filename):
    """
    Create a heatmap showing AQI by city and parameter.
    
    Args:
        df: DataFrame with air quality readings
        filename: Output filename
    """
    try:
        # Group data by city and parameter to calculate average AQI
        pivot_df = df.pivot_table(
            values='aqi',
            index='city',
            columns='parameter',
            aggfunc='mean'
        )
        
        if pivot_df.empty:
            logger.warning("No data available for AQI heatmap")
            return
        
        # Create figure
        plt.figure(figsize=(10, 8))
        
        # Create heatmap
        sns.heatmap(
            pivot_df,
            annot=True,
            cmap='RdYlGn_r',  # Red-Yellow-Green color map (reversed)
            fmt='.1f',
            linewidths=.5,
            cbar_kws={'label': 'Average AQI'}
        )
        
        # Set title
        plt.title('Average Air Quality Index (AQI) by City and Pollutant', fontsize=14)
        
        # Save figure
        output_path = os.path.join(VISUALIZATION_OUTPUT_DIR, f'{filename}.png')
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Created AQI heatmap at {output_path}")
    except Exception as e:
        logger.error(f"Error creating AQI heatmap: {e}")

def main():
    """Main function to create visualizations."""
    logger.info("Starting visualization generation")
    
    # Connect to database
    connection = connect_to_db()
    if connection is None:
        logger.error("Failed to connect to database. Exiting.")
        return
    
    try:
        # Get the latest data
        df = get_latest_readings(connection)
        
        if df.empty:
            logger.warning("No data available for visualization")
            return
        
        # Create visualizations
        create_time_series_chart(df, 'pm25', 'pm25_time_series')
        create_time_series_chart(df, 'o3', 'o3_time_series')
        create_time_series_chart(df, 'no2', 'no2_time_series')
        create_aqi_comparison_chart(df, 'aqi_comparison')
        create_aqi_distribution_chart(df, 'aqi_distribution')
        create_aqi_heatmap(df, 'aqi_heatmap')
        
        logger.info("Visualization generation completed successfully")
    except Exception as e:
        logger.error(f"Error in visualization process: {e}")
    finally:
        connection.close()
        logger.info("Database connection closed")

if __name__ == "__main__":
    main()