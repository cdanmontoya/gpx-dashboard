import os
from typing import List
from geopy.geocoders import Nominatim

import gpxpy
import pandas as pd

#TODO: Borrar esta funcion cuando se integre al codigo de ingesta
"""
geolocator = Nominatim(user_agent="geoapiExercises")
def city_state_country(coord):
    location = geolocator.reverse(coord, exactly_one=True)
    #print(geopy.location.Location.__dict__.keys())
    address = location.raw['address']
    city = address.get('city', '')
    state = address.get('state', '')
    country = address.get('country', '')
    print(f"Toda la location {address}")
    return city, state, country
print(city_state_country("6.208512, -75.571616"))
"""

def get_files_path(prefix='data/01_raw/', extension='gpx') -> List[str]:
    return list(map(lambda x: prefix + x, filter(lambda x: x.endswith(extension), os.listdir(prefix))))


def read_files(files_path: List[str]) -> pd.DataFrame:
    print(pd.concat([read_gpx(file) for file in files_path], ignore_index=True))
    return pd.concat([read_gpx(file) for file in files_path], ignore_index=True)


def read_gpx(file: str) -> pd.DataFrame:
    points = []

    with open(file) as f:
        gpx = gpxpy.parse(f)

    for segment in gpx.tracks[0].segments:
        for p in segment.points:
            points.append({
                'file': file,
                'time': p.time,
                'latitude': p.latitude,
                'longitude': p.longitude,
                'elevation': p.elevation,
            })

    return pd.DataFrame.from_records(points)
