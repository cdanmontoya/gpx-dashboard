"""
This is a boilerplate pipeline 'data_enrichment'
generated using Kedro 0.18.6
"""
import pandas as pd
from geopy import Nominatim
from geopy.extra.rate_limiter import RateLimiter

def add_geographic_attributes(df: pd.DataFrame) -> pd.DataFrame:
    geolocator = Nominatim(user_agent="geoapiExercises")
    geocode = RateLimiter(geolocator.reverse, min_delay_seconds=1)

    df['lat_lon'] = df['latitude'].astype('string') + ", " + df['longitude'].astype('string')
    df['location'] =df['lat_lon'].apply(geocode)
    df['neighbourhood'] =df['location'].transform(lambda location: location.raw['address']['neighbourhood'])
    df['suburb'] =df['location'].transform(lambda location: location.raw['address']['suburb'])
    df['city'] =df['location'].transform(lambda location: location.raw['address']['city'])
    df['postcode'] =df['location'].transform(lambda location: location.raw['address']['postcode'])
    df.drop('lat_lon', axis='columns')

    return df



