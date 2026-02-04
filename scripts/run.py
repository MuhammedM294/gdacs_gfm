import logging
from pathlib import Path
from datetime import datetime

import pandas as pd
from gdacs_gfm.gfm_index import find_gfm_images
from gdacs_gfm.algorithms import GFMAlgorithm
from gdacs_gfm.config import DIMENSIONS, FIELDS_DEF
from gdacs_gfm.datacube import build_datacube, filter_datacube_by_event
from gdacs_gfm.pipeline import process_event
from gdacs_gfm.logger import setup_logging
from gdacs_gfm.process_geojson import load_event_geojson

# -----------------------
# Setup
# -----------------------
setup_logging()
logger = logging.getLogger("gfm_logger")

DB_PATH = Path(
    "/eodc/private/tuwgeo/users/mabdelaa/repos/GDACS_Flood_DB/data/latest_gdacs_flood_db.csv"
)
GEOJSON_DIR = Path("/eodc/private/tuwgeo/users/mabdelaa/repos/GDACS_Flood_DB/data/aois")
RESULTS_DIR = Path("/eodc/private/tuwgeo/users/mabdelaa/repos/gdacs_gfm/results")

df = pd.read_csv(DB_PATH)
logger.info(f"Total number of flood events in DB: {len(df)}")


# -----------------------
# Utility functions
# -----------------------
def save_indicator_file(event_id: str, folder: Path, message: str) -> None:
    """Create a text file indicating an event issue (no AOI, no data, etc.)."""
    file_path = folder / f"{event_id}.txt"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(message)


def parse_dates(row: pd.Series) -> tuple[datetime, datetime]:
    """Parse fromdate and todate from a row."""
    return (
        datetime.strptime(row["fromdate"], "%Y-%m-%dT%H:%M:%S"),
        datetime.strptime(row["todate"], "%Y-%m-%dT%H:%M:%S"),
    )


#------------------------
# Create result df with IDs
#------------------------

df_results = df[["GDACS_ID"]].copy()
df_results["processed"] = False
df_results["status"] = ""

results_file = RESULTS_DIR / "processing_results.csv"
results_file.parent.mkdir(parents=True, exist_ok=True)
df_results.to_csv(results_file, index=False)
# -----------------------
# Main processing loop
# -----------------------
for idx, row in df.iterrows():
    event_id = row["GDACS_ID"]
    country = row["country"]
    equi7grid = row["equi7_grid_code"]

    logger.info(f"Processing event {event_id} in {country}")
    logger.debug(
        f"Event dates: from {row['fromdate']} to {row['todate']}, equi7 grid {equi7grid}"
    )

    event_start, event_end = parse_dates(row)

    # Load AOI polygons
    polygons, sref = load_event_geojson(event_id, GEOJSON_DIR)
    if polygons is None:
        logger.warning(f"{country} ({event_id}): No valid AOI polygon found.")
        logger.info("==============================================")
        logger.info("==============================================")
        save_indicator_file(
            event_id,
            RESULTS_DIR / "no_aoi",
            f"Event {event_id} has no valid AOI polygon in GeoJSON.\n",
        )
        df_results = pd.read_csv(results_file)
        df_results.loc[df_results["GDACS_ID"] == event_id, "processed"] = True
        df_results.loc[df_results["GDACS_ID"] == event_id, "status"] = "no_aoi"
        df_results.to_csv(results_file, index=False)
        continue

    # Find GFM images
    images = find_gfm_images(
        event_start=event_start,
        event_end=event_end,
        equi7_code=equi7grid,
        algorithm=GFMAlgorithm.ENSEMBLE,
        buffer_days=1,
    )
    logger.info(f"Found {len(images)} images for event {event_id}")
    images = [str(img) for img in images]

    # Build datacube and filter by AOI
    dc = build_datacube(
        images_paths=images, dimensions=DIMENSIONS, fields_def=FIELDS_DEF
    )
    dc_sel = filter_datacube_by_event(dc, event_id, polygons, sref, logger)

    if dc_sel is None:
        logger.warning(f"{country} ({event_id}): No data after AOI filtering.")
        logger.info("==============================================")
        logger.info("==============================================")
        save_indicator_file(
            event_id,
            RESULTS_DIR / "no_data",
            f"Event {event_id} has no data after AOI filtering.\n",
        )
        df_results = pd.read_csv(results_file)
        df_results.loc[df_results["GDACS_ID"] == event_id, "processed"] = True
        df_results.loc[df_results["GDACS_ID"] == event_id, "status"] = "no_data"
        df_results.to_csv(results_file, index=False)
        continue

    # Process event
    process_event(
        event=row,
        algorithm=GFMAlgorithm.ENSEMBLE,
        dcs=dc_sel,
        results_dir=RESULTS_DIR,
        LOGGER=logger,
    )
    logger.info(f"Event {event_id}: Processing completed.")
