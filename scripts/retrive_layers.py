import logging
from pathlib import Path
from datetime import datetime
from tqdm import tqdm
import pandas as pd

from gdacs_gfm.algorithms import GFMAlgorithm
from gdacs_gfm.config import DIMENSIONS
from gdacs_gfm.datacube import build_datacube, filter_datacube_by_event
from gdacs_gfm.logger import setup_logging
from gdacs_gfm.process_geojson import load_event_geojson
from gdacs_gfm.retrieve_gfm_product import (
    find_gfm_layers_images,
    select_field_defs,
    copy_files,
)

# -----------------------
# Setup
# -----------------------
setup_logging()
logger = logging.getLogger("gfm_logger")

DB_PATH = Path(
    "/eodc/private/tuwgeo/users/mabdelaa/repos/GDACS_Flood_DB/data/latest_gdacs_flood_db.csv"
)
GEOJSON_DIR = Path("/eodc/private/tuwgeo/users/mabdelaa/repos/GDACS_Flood_DB/data/aois")
GFM_LAYERS = Path("/eodc/private/tuwgeo/users/mabdelaa/repos/gdacs_gfm/gfm_layers_data")



def save_indicator_file(event_id: str, folder: Path, message: str) -> None:
    file_path = folder / f"{event_id}.txt"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(message)


def parse_dates(row: pd.Series):
    return (
        datetime.strptime(row["fromdate"], "%Y-%m-%dT%H:%M:%S"),
        datetime.strptime(row["todate"], "%Y-%m-%dT%H:%M:%S"),
    )


def copy_dc_images(dcs, destination_dir):
    if dcs is None:
        return

    destination_dir.mkdir(parents=True, exist_ok=True)

    if not isinstance(dcs, list):
        dcs = [dcs]

    for i, dc in enumerate(dcs):
        copy_files(dc.filepaths, destination_dir / f"AOI_{i}" , logger=logger)


def build_filter_copy(files, fields_def, event_id,event_dir, polygons, sref, subfolder):
        if not files:
            return None

        files = [str(p) for p in files]
        dc = build_datacube(files, DIMENSIONS, fields_def)
        dc_sel = filter_datacube_by_event(dc, event_id, polygons, sref, None)

        if dc_sel is None:
            save_indicator_file(
                event_id, event_dir / "no_data_dc_filter" , 
                f"Event {event_id} has no data after DC filter .\n",
            )

            return

        copy_dc_images(dc_sel, event_dir / subfolder)

def process_single_event(row_dict = None ):
    """
    Worker-safe function.
    Receives dict instead of pandas row.
    """
    event_id = row_dict["GDACS_ID"]
    equi7grid = row_dict["equi7_grid_code"]

    event_base_dir = GFM_LAYERS / f"{event_id}"
    event_base_dir.mkdir(parents=True, exist_ok=True)

    event_start = datetime.strptime(row_dict["fromdate"], "%Y-%m-%dT%H:%M:%S")
    event_end = datetime.strptime(row_dict["todate"], "%Y-%m-%dT%H:%M:%S")

    polygons, sref = load_event_geojson(event_id, GEOJSON_DIR)
    if polygons is None:
        save_indicator_file(
            event_id,
            event_base_dir / "no_aoi",
            f"Event {event_id} has no valid AOI polygon.\n",
        )
        return
    for ALGO in [GFMAlgorithm.ENSEMBLE , GFMAlgorithm.LIST , GFMAlgorithm.DLR, GFMAlgorithm.TUW]:

        images = find_gfm_layers_images(
            event_start=event_start,
            event_end=event_end,
            equi7_code=equi7grid,
            algorithm=ALGO,
            buffer_days=5,
        )

        fl, uncer, exc, obsw, adv = images

       

        uncert_root = uncer[0].parents[6].name
        

        field_defs = select_field_defs(ALGO, uncert_root)
        FL_FIELDS_DEF, UN_FIELDS_DEF, EX_FIELDS_DEF, OBS_FIELDS_DEF, ADV_FIELDS_DEF = field_defs

        event_algo_dir = event_base_dir / f"{ALGO.value}_{uncert_root}"
        if not fl:
            save_indicator_file(
                event_id,
                event_algo_dir / "no_data_at_all",
                f"Event {event_id} has no data.\n",
            )
            continue

        build_filter_copy(fl, FL_FIELDS_DEF, event_id, event_algo_dir, polygons, sref, "flood_extent")
        build_filter_copy(uncer, UN_FIELDS_DEF, event_id,event_algo_dir, polygons, sref, "uncertainty")

        if ALGO == GFMAlgorithm.ENSEMBLE:
            build_filter_copy(exc, EX_FIELDS_DEF, event_id, event_base_dir, polygons, sref, "exclusion")
            build_filter_copy(obsw, OBS_FIELDS_DEF, event_id,event_base_dir, polygons, sref, "observed_water")
            build_filter_copy(adv, ADV_FIELDS_DEF, event_id,event_base_dir, polygons, sref, "adv_flags")
    




if __name__ == "__main__":
    df = pd.read_csv(DB_PATH)
    df = df[df['GDACS_ID']=="FL-1000066"]
    rows = df.to_dict(orient="records")
    # rows = rows[3500:]
    for row_dict in tqdm(rows, desc="Processing events"):
        try:
            process_single_event(row_dict)
        except Exception as e:
            event_id = row_dict.get("GDACS_ID", "UNKNOWN")
            logger.error(f"Error processing event {event_id}: {e}")

