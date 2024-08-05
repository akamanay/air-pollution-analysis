import pandas as pd
import requests
import os
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

# Base URL for the OpenWeatherMap API
BASE_URL = 'http://api.openweathermap.org/data/2.5/air_pollution'
API_KEY = os.getenv('API_KEY')  # Ensure you have set this in your environment variables

def get_api_data(lon, lat):
    """Fetch data from OpenWeatherMap API for a given longitude and latitude."""
    try:
        response = requests.get(f'{BASE_URL}?lat={lat}&lon={lon}&appid={API_KEY}')
        response.raise_for_status()  # Raise an error for bad responses
        data = response.json()
        
        if 'list' in data and data['list']:
            return data['list'][0]  # Return the first item in the 'list'
        else:
            return None  # Return None if 'list' is empty or missing
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data for coordinates ({lon}, {lat}): {e}")
        return None

def get_aqi_data(row):
    """Extract AQI data for a given row."""
    api_data = get_api_data(row['lon'], row['lat'])
    if api_data:
        return api_data['main']['aqi']
    else:
        return None

def get_components_data(row):
    """Extract pollutant components data for a given row."""
    api_data = get_api_data(row['lon'], row['lat'])
    if api_data:
        return api_data['components']
    else:
        return None

def transform(demographic_data, geographic_data):
    """Transform the extracted data by merging and enriching with API data."""
    
    # Merge demographic and geographic data on 'Location'
    merged_data = geographic_data.merge(demographic_data, how='inner', on='Location')
    
    # Sample list of locations with lon and lat data
    locations = [
        {'Location': 'Antananarivo', 'lon': 47.5256, 'lat': -18.91},
        {'Location': 'Los Angeles', 'lon': -118.242766, 'lat': 34.0536909},
        {'Location': 'Paris', 'lon': 2.320041, 'lat': 48.8588897},
        {'Location': 'Nairobi', 'lon': 36.8172, 'lat': -1.2833},
        {'Location': 'Lima', 'lon': -77.0365256, 'lat': -12.0621065},
        {'Location': 'Tokyo', 'lon': 139.762221, 'lat': 35.6821936}
    ]

    # Create DataFrame from list of locations
    lon_and_lat_df = pd.DataFrame(locations)

    # Merge the newly created DataFrame with the existing merged data
    enriched_data = merged_data.merge(lon_and_lat_df, how='inner', on='Location')
    
    # Add a new column 'AQI' to the DataFrame by applying the get_aqi_data function
    enriched_data['AQI'] = enriched_data.apply(get_aqi_data, axis=1)
    
    # Add new columns for pollutants by applying the get_components_data function
    components_data = enriched_data.apply(get_components_data, axis=1)
    
    # Convert the list of dictionaries to a DataFrame
    components_df = pd.DataFrame(components_data.tolist())
    
    # Concatenate the components DataFrame with the existing enriched data
    final_data = pd.concat([enriched_data, components_df], axis=1)
    
    print("---> Data transformation complete.")
    
    return final_data
