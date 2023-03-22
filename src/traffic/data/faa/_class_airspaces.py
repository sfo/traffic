from __future__ import annotations

import logging
from typing import Dict

import geopandas as gpd

import pandas as pd

import shapefile

from shapely.geometry import shape
from shapely.ops import orient

from tqdm.rich import tqdm

from ...core.airspace import Airspace, Airspaces, ExtrudedPolygon

_log = logging.getLogger(__name__)


class Class_Airspaces(Airspaces):
    id_ = "67885972e4e940b2aa6d74024901c561_0"
    filename = "faa_airspace_boundary.json"

    def __init__(self, data: gpd.GeoDataFrame | None = None) -> None:
        self.data = data
        if data is None:
            sf = shapefile.Reader(
                "../../../data/infrastructure/class_airspace_shape_files.zip"
            )  # TODO make configurable like for AIRAC cycle data
            record_fields = [
                field for field, _, _, _ in sf.fields if field != "DeletionFlag"
            ]
            self.data = (
                gpd.GeoDataFrame(
                    pd.concat(
                        [
                            gpd.GeoDataFrame(
                                {
                                    field: shapeRecord.record[field]
                                    for field in record_fields
                                }
                                | {"geometry": shapeRecord.shape},
                                index=[i],
                            )
                            for i, shapeRecord in enumerate(
                                tqdm(
                                    sf.iterShapeRecords(record_fields),
                                    total=sf.numRecords,
                                )
                            )
                        ]
                    )
                )
                .rename(
                    columns=dict(
                        NAME="name",
                        LOWER_VAL="lower",
                        UPPER_VAL="upper",
                        TYPE_CODE="type",
                        IDENT="designator",
                    )
                )
                .assign(
                    latitude=lambda df: df.geometry.centroid.y,
                    longitude=lambda df: df.geometry.centroid.x,
                    name=lambda df: df.name.str.strip(),
                    lower=lambda df: df.lower.astype(float).replace(-9998, 0),
                    upper=lambda df: df.upper.astype(float).replace(
                        -9998, float("inf")
                    ),
                )
            )

    def download_data(self) -> None:
        from .. import session

        _log.warning(
            f"Downloading data from {self.website}. Please check terms of use."
        )
        c = session.get(self.json_url)
        c.raise_for_status()
        json_contents = c.json()
        with self.cache_file.open("w") as fh:
            json.dump(json_contents, fh)
