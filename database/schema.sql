-- database/schema.sql

-- Drop tables if they exist (for clean setup)
DROP TABLE IF EXISTS readings;
DROP TABLE IF EXISTS locations;
DROP TABLE IF EXISTS weather_conditions;

-- Create locations table
CREATE TABLE locations (
    location_id SERIAL PRIMARY KEY,
    city VARCHAR(100) NOT NULL,
    district VARCHAR(100),
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),
    country VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create table for air quality readings
CREATE TABLE readings (
    reading_id SERIAL PRIMARY KEY,
    location_id INTEGER REFERENCES locations(location_id),
    timestamp TIMESTAMP,
    parameter VARCHAR(20) NOT NULL,
    value DECIMAL(10,2),
    unit VARCHAR(20),
    aqi INTEGER,
    aqi_category VARCHAR(50),
    health_recommendation TEXT,
    source_name VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index on timestamp for efficient queries
CREATE INDEX idx_readings_timestamp ON readings(timestamp);
CREATE INDEX idx_readings_parameter ON readings(parameter);
CREATE INDEX idx_readings_location_parameter ON readings(location_id, parameter);

-- Create table for weather conditions (optional)
CREATE TABLE weather_conditions (
    weather_id SERIAL PRIMARY KEY,
    location_id INTEGER REFERENCES locations(location_id),
    timestamp TIMESTAMP,
    temperature DECIMAL(5,2),
    humidity DECIMAL(5,2),
    wind_speed DECIMAL(5,2),
    wind_direction VARCHAR(20),
    precipitation DECIMAL(5,2),
    pressure DECIMAL(7,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index on weather timestamp
CREATE INDEX idx_weather_timestamp ON weather_conditions(timestamp);