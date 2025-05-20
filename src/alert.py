# src/alert.py

import pandas as pd
import logging
import psycopg2
from datetime import datetime
import os
import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Import configuration
from config import (
    DB_NAME,
    DB_USER,
    DB_PASSWORD,
    DB_HOST,
    DB_PORT,
    LOG_FILE,
    LOG_LEVEL,
    ALERT_LOG_DIR,
    EMAIL_ENABLED,
    EMAIL_SENDER,
    EMAIL_PASSWORD,
    EMAIL_RECIPIENTS,
    EMAIL_SMTP_SERVER,
    EMAIL_SMTP_PORT
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

# Create alert log directory if it doesn't exist
os.makedirs(ALERT_LOG_DIR, exist_ok=True)

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

def get_latest_readings(connection):
    """
    Get the latest air quality readings from the database.
    
    Args:
        connection: PostgreSQL database connection
        
    Returns:
        DataFrame: Latest air quality readings
    """
    try:
        # SQL query to get the latest reading for each location-parameter combination
        query = """
        WITH latest_readings AS (
            SELECT 
                location_id,
                parameter,
                MAX(timestamp) as latest_timestamp
            FROM readings
            GROUP BY location_id, parameter
        )
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
        JOIN latest_readings lr ON 
            r.location_id = lr.location_id AND 
            r.parameter = lr.parameter AND 
            r.timestamp = lr.latest_timestamp
        ORDER BY l.city, r.parameter
        """
        
        # Execute query
        df = pd.read_sql_query(query, connection)
        
        logger.info(f"Retrieved {len(df)} latest readings from database")
        return df
    except Exception as e:
        logger.error(f"Error retrieving latest readings from database: {e}")
        return pd.DataFrame()

def check_for_alerts(df):
    """
    Check for dangerous air quality levels.
    
    Args:
        df: DataFrame with latest air quality readings
        
    Returns:
        list: List of alerts
    """
    alerts = []
    
    # Define alert thresholds
    thresholds = {
        'pm25': {
            'High': 35.5,  # Unhealthy for Sensitive Groups
            'Severe': 55.5  # Unhealthy
        },
        'o3': {
            'High': 71,  # Unhealthy for Sensitive Groups
            'Severe': 86  # Unhealthy
        },
        'no2': {
            'High': 101,  # Unhealthy for Sensitive Groups
            'Severe': 361  # Unhealthy
        }
    }
    
    # Check each reading against thresholds
    for idx, row in df.iterrows():
        parameter = row['parameter']
        value = row['value']
        city = row['city']
        district = row['district']
        aqi = row['aqi']
        aqi_category = row['aqi_category']
        
        if parameter in thresholds:
            if value >= thresholds[parameter]['Severe']:
                severity = 'Severe'
                logger.warning(f"SEVERE ALERT: {parameter} in {city} is {value} (AQI: {aqi}, {aqi_category})")
            elif value >= thresholds[parameter]['High']:
                severity = 'High'
                logger.warning(f"HIGH ALERT: {parameter} in {city} is {value} (AQI: {aqi}, {aqi_category})")
            else:
                continue  # No alert needed
            
            # Create alert
            alert = {
                'timestamp': datetime.now().isoformat(),
                'city': city,
                'district': district,
                'parameter': parameter,
                'value': float(value),
                'unit': row['unit'],
                'aqi': int(aqi),
                'aqi_category': aqi_category,
                'severity': severity,
                'message': f"{severity} {parameter} levels in {city} {district}. Value: {value}, AQI: {aqi}, Category: {aqi_category}",
                'health_recommendation': row['health_recommendation']
            }
            
            alerts.append(alert)
    
    logger.info(f"Found {len(alerts)} alerts in the latest readings")
    return alerts

def log_alerts(alerts):
    """
    Log alerts to file.
    
    Args:
        alerts: List of alert dictionaries
    """
    if not alerts:
        logger.info("No alerts to log")
        return
    
    # Create log filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = os.path.join(ALERT_LOG_DIR, f"alerts_{timestamp}.json")
    
    # Write alerts to log file
    try:
        with open(log_filename, 'w') as f:
            json.dump(alerts, f, indent=2)
        
        logger.info(f"Logged {len(alerts)} alerts to {log_filename}")
    except Exception as e:
        logger.error(f"Error logging alerts to file: {e}")

def send_alert_email(alerts):
    """
    Send email notifications for alerts.
    
    Args:
        alerts: List of alert dictionaries
    """
    if not EMAIL_ENABLED or not alerts:
        return
    
    try:
        # Create message
        msg = MIMEMultipart()
        msg['From'] = EMAIL_SENDER
        msg['To'] = ', '.join(EMAIL_RECIPIENTS)
        msg['Subject'] = f"Air Quality Alert: {len(alerts)} alerts detected"
        
        # Email body
        body = "The following air quality alerts have been detected:\n\n"
        
        for alert in alerts:
            body += f"ALERT: {alert['message']}\n"
            body += f"Time: {alert['timestamp']}\n"
            body += f"Health Recommendation: {alert['health_recommendation']}\n\n"
        
        body += "\nThis is an automated message from the Air Quality Monitoring System."
        
        msg.attach(MIMEText(body, 'plain'))
        
        # Connect to server and send email
        server = smtplib.SMTP(EMAIL_SMTP_SERVER, EMAIL_SMTP_PORT)
        server.starttls()
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)
        text = msg.as_string()
        server.sendmail(EMAIL_SENDER, EMAIL_RECIPIENTS, text)
        server.quit()
        
        logger.info(f"Sent alert email to {', '.join(EMAIL_RECIPIENTS)}")
    except Exception as e:
        logger.error(f"Error sending alert email: {e}")

def main():
    """Main function to check for air quality alerts."""
    logger.info("Starting air quality alert check")
    
    # Connect to database
    connection = connect_to_db()
    if connection is None:
        logger.error("Failed to connect to database. Exiting.")
        return
    
    try:
        # Get the latest readings
        df = get_latest_readings(connection)
        
        if df.empty:
            logger.warning("No data available for alert checking")
            return
        
        # Check for alerts
        alerts = check_for_alerts(df)
        
        # Log alerts
        log_alerts(alerts)
        
        # Send email notifications
        if alerts:
            send_alert_email(alerts)
        
        logger.info("Alert check completed successfully")
    except Exception as e:
        logger.error(f"Error in alert check process: {e}")
    finally:
        connection.close()
        logger.info("Database connection closed")

if __name__ == "__main__":
    main()