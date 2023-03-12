import os
from typing import List

import gpxpy
import pandas as pd


def get_files_path(prefix='data/01_raw/', extension='gpx') -> List[str]:
    return list(map(lambda x: prefix + x, filter(lambda x: x.endswith(extension), os.listdir(prefix))))


def read_files(files_path: List[str]) -> pd.DataFrame:
    return pd.concat([read_gpx(file) for file in files_path])


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
