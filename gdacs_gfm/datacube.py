from pathlib import Path

from typing import List, Optional
from yeoda.datacube import DataCubeReader
from geopathfinder.file_naming import SmartFilename
from shapely.geometry import Polygon
from .process_geojson import load_event_geojson, filterby_dc_poly

# disable future warnings 
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
GEOJSON_DIR = Path(__file__).parent.parent.parent / "data" / "aois"


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
    LOGGER=None,
):
    polygons, sref = load_event_geojson(event_id, GEOJSON_DIR)
    print(f"Event ({event_id}): Loaded {len(polygons)} polygons from GeoJSON")

    filtered_dcs = []
    for poly in polygons:
        dc_sel = filterby_dc_poly(dc, poly, sref, event_id, LOGGER)
        print(len(dc_sel))
        print(dc_sel.file_register)
        if dc_sel is not None:
            filtered_dcs.append(dc_sel)

    if not filtered_dcs:
        if LOGGER:
            LOGGER.warning(f"Event ({event_id}): No data after AOI filtering")
        return None

    return filtered_dcs




if __name__ == "__main__":
    import pandas as pd
    db_path = Path("/eodc/private/tuwgeo/users/mabdelaa/repos/GDACS_Flood_DB/data/gdacs_flood_db_corrected.csv")
    df = pd.read_csv(db_path)
    sample_event = df.iloc[3000]
    print(sample_event)
    event_id = sample_event["GDACS_ID"]
    fromdate = sample_event["fromdate"]
    todate = sample_event["todate"]
    equi7grid = sample_event["equi7_code"]

    from .gfm_index import find_gfm_images
    from .algorithms import GFMAlgorithm

    #convert data to datetime
    from datetime import datetime
    event_start = datetime.strptime(fromdate, "%Y-%m-%dT%H:%M:%S")
    event_end = datetime.strptime(todate, "%Y-%m-%dT%H:%M:%S")
    images = find_gfm_images(
        event_start=event_start,
        event_end=event_end,
        equi7_code=equi7grid,
        algorithm=GFMAlgorithm.ENSEMBLE,
        buffer_days=1,
    )
    print(f"Found {len(images)} images for event {event_id}")

    images = [str(img) for img in images]
    from .config import *
    dc = build_datacube(
        images_paths=images,
        dimensions=DIMENSIONS,
        fields_def=FIELDS_DEF,
    )
    print(dc.file_register)
    import logging
    logger = logging.getLogger("gfm_logger")
    dc_sel = filter_datacube_by_event(dc, event_id , logger)
