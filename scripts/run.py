import logging
from pathlib import Path
from datetime import datetime
from typing import Tuple
from tqdm import tqdm
import pandas as pd
from gdacs_gfm.gfm_index import find_gfm_images
from gdacs_gfm.algorithms import GFMAlgorithm
from gdacs_gfm.config import DIMENSIONS, FL_DEF_DICT
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
GEOJSON_DIR = Path(
    "/eodc/private/tuwgeo/users/mabdelaa/repos/GDACS_Flood_DB/data/aois"
)
RESULTS_DIR = Path(
    "/eodc/private/tuwgeo/users/mabdelaa/repos/gdacs_gfm/results"
)
RESULTS_FILE = RESULTS_DIR / "processing_results.csv"


# -----------------------
# Utility functions
# -----------------------
def save_indicator_file(event_id: str, folder: Path, message: str) -> None:
    """Create a text file indicating an event issue."""
    file_path = folder / f"{event_id}.txt"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(message)


def parse_dates(row: pd.Series) -> Tuple[datetime, datetime]:
    """Parse fromdate and todate from a row."""
    return (
        datetime.strptime(row["fromdate"], "%Y-%m-%dT%H:%M:%S"),
        datetime.strptime(row["todate"], "%Y-%m-%dT%H:%M:%S"),
    )


def update_event_status(
    df_results: pd.DataFrame,
    event_id: str,
    algorithm: GFMAlgorithm,
    status: str,
) -> None:
    """Update processing status for an event."""
    df_results.loc[df_results["GDACS_ID"] == event_id, "processed"] = True
    df_results.loc[
        df_results["GDACS_ID"] == event_id, algorithm.value
    ] = status


def event_already_processed(event_id, selected_algorithm, results_dir: Path) -> bool:
    """Check if an event has already been processed.

    - In `results_dir/no_aoi` and `results_dir/no_data`: check if any file starts with `event_id`.
    - In `results_dir`: check if any file starts with `{event_id}_{selected_algorithm.value}`.
    """
    event_prefix = str(event_id)
    algo_prefix = f"{event_id}_{selected_algorithm.value}"

    # Check event_prefix in no_aoi and no_data
    for subdir in ["no_aoi", "no_data"]:
        directory = results_dir / subdir
        if not directory.exists():
            continue
        for f in directory.iterdir():
            if f.is_file() and f.name.startswith(event_prefix):
                return True

    # Check algo_prefix in main results_dir
    if results_dir.exists():
        for f in results_dir.iterdir():
            if f.is_file() and f.name.startswith(algo_prefix):
                return True

    return False

# -----------------------
# Core processing
# -----------------------
def process_single_event(
    row: pd.Series,
    selected_algorithm: GFMAlgorithm,
    df_results: pd.DataFrame,
) -> None:
    """Process one event and update status."""
    event_id = row["GDACS_ID"]
    country = row["country"]
    equi7grid = row["equi7_grid_code"]
    alert_level = row["alertlevel"]

    logger.info(
        f"Processing event {event_id} in {country} (alert: {alert_level}, grid: {equi7grid})"
    )

    event_start, event_end = parse_dates(row)
    if event_already_processed(event_id, selected_algorithm, RESULTS_DIR):
        logger.info(f"Skipping! Event {event_id} already processed. ")
        return
    

    # -----------------------
    # Load AOI
    # -----------------------
    polygons, sref = load_event_geojson(event_id, GEOJSON_DIR)
    if polygons is None:
        logger.warning(f"{country} ({event_id}): No valid AOI polygon.")
        save_indicator_file(
            event_id,
            RESULTS_DIR / "no_aoi",
            f"Event {event_id} has no valid AOI polygon.\n",
        )
        update_event_status(df_results, event_id, selected_algorithm, "no_aoi")
        return

    # -----------------------
    # Find images
    # -----------------------
    images = find_gfm_images(
        event_start=event_start,
        event_end=event_end,
        equi7_code=equi7grid,
        algorithm=selected_algorithm,
        buffer_days=1,
    )
    logger.info(f"{event_id}: Found {len(images)} images")

    images = [str(img) for img in images]

    # -----------------------
    # Build datacube
    # -----------------------
    sel_fields_def = FL_DEF_DICT[selected_algorithm.value]

    dc = build_datacube(
        images_paths=images,
        dimensions=DIMENSIONS,
        fields_def=sel_fields_def,
    )

    dc_sel = filter_datacube_by_event(dc, event_id, polygons, sref, logger)

    if dc_sel is None:
        logger.warning(f"{country} ({event_id}): No data after AOI filtering.")
        save_indicator_file(
            event_id,
            RESULTS_DIR / "no_data",
            f"Event {event_id} has no data after AOI filtering.\n",
        )
        update_event_status(df_results, event_id, selected_algorithm, "no_data")
        return

    # -----------------------
    # Run processing
    # -----------------------
    try:
        process_event(
            event=row,
            algorithm=selected_algorithm,
            dcs=dc_sel,
            results_dir=RESULTS_DIR,
            LOGGER=logger,
        )
        logger.info(f"{event_id}: Processing completed.")
        update_event_status(df_results, event_id, selected_algorithm, "done")

    except Exception as e:
        logger.exception(f"Error processing event {event_id}: {e}")
        update_event_status(df_results, event_id, selected_algorithm, "error")


# -----------------------
# Main
# -----------------------
def main():
    df = pd.read_csv(DB_PATH)
    logger.info(f"Total number of flood events in DB: {len(df)}")

    df_results = pd.read_csv(RESULTS_FILE)

    selected_algorithm = GFMAlgorithm.LIST
    logger.info(f"Selected GFM Algorithm: {selected_algorithm.value}")

    # Ensure column exists
    if selected_algorithm.value not in df_results.columns:
        df_results[selected_algorithm.value] = ""
    
    for _, row in tqdm(
        df.iterrows(),
        total=len(df),
        desc="Processing Flood Events",
        unit="event"
    ):  
        try:
            
            process_single_event(row, selected_algorithm, df_results)
        except Exception as e:
            logger.warning(f"{e}")

    # Save once at the end
    df_results.to_csv(RESULTS_FILE, index=False)
    logger.info("Processing completed for all events.")


if __name__ == "__main__":
    main()