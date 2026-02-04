from pathlib import Path

from typing import List, Optional
from yeoda.datacube import DataCubeReader
from geopathfinder.file_naming import SmartFilename
from shapely.geometry import Polygon
from .process_geojson import load_event_geojson, filterby_dc_poly
from geospade.crs import SpatialRef
import logging

# disable future warnings
import warnings

warnings.simplefilter(action="ignore", category=FutureWarning)

logger = logging.getLogger("gfm_logger")


def build_datacube(
    images_paths: List[Path],
    dimensions: tuple,
    fields_def: dict,
    stack_dimension: str = "time",
    tile_dimension: str = "tile_name",
):
    return DataCubeReader.from_filepaths(
        filepaths=images_paths,
        dimensions=dimensions,
        fields_def=fields_def,
        stack_dimension=stack_dimension,
        tile_dimension=tile_dimension,
        fn_class=SmartFilename,
    )


def filter_datacube_by_event(
    dc,
    event_id: str,
    polygons: List[Polygon],
    sref: SpatialRef,
    LOGGER=None,
):

    logger.info(f"Event ({event_id}): Loaded {len(polygons)} polygons from GeoJSON")

    filtered_dcs = []
    for poly in polygons:
        dc_sel = filterby_dc_poly(dc, poly, sref, event_id, LOGGER)
        if dc_sel is not None:
            filtered_dcs.append(dc_sel)

    if not filtered_dcs:
        if LOGGER:
            LOGGER.warning(f"Event ({event_id}): No data after AOI filtering")
        return None

    return filtered_dcs
