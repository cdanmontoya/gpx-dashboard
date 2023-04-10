import os
from typing import List

import gpxpy
import pandas as pd
import logging

log = logging.getLogger(__name__)


def get_files_path(prefix='data/01_raw/', extension='gpx') -> List[str]:
    return list(map(lambda x: prefix + x, filter(lambda x: x.endswith(extension), os.listdir(prefix))))


def read_files(files_path: List[str]) -> pd.DataFrame:
    df = pd.concat([read_gpx(file) for file in files_path], ignore_index=True)
    df["time"] = pd.to_datetime(df.time).dt.tz_localize(None)
    return df


def read_gpx(file: str) -> pd.DataFrame:
    log.info(f'Opening file {file}')
    points = []

    with open(file) as f:
        gpx = gpxpy.parse(f)

    for segment in gpx.tracks[0].segments:
        for i, p in enumerate(segment.points):
            points.append({
                'trip': file,
                'step': i,
                'time': p.time,
                'latitude': p.latitude,
                'longitude': p.longitude,
                'elevation': p.elevation,
            })

    return pd.DataFrame.from_records(points)
