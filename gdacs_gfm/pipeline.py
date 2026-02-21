import csv
from pathlib import Path
from tqdm import tqdm
import numpy as np
import pandas as pd

from concurrent.futures import ThreadPoolExecutor
import numpy as np
import pandas as pd
import rasterio
from pathlib import Path


def _process_file(fp):
    try:
        with rasterio.open(fp) as src:
            data = src.read(1)
            flooded_pixels = int(np.count_nonzero(data == 1))
            area_km2 = flooded_pixels * 400 / 1e6
            return flooded_pixels, area_km2
    except:
        return None, None


def add_flood_metrics_parallel(df, max_workers=8):
    with ThreadPoolExecutor(max_workers=max_workers) as exe:
        results = list(exe.map(_process_file, df["filepath"]))

    df = df.copy()
    df["pixel_count"] = [r[0] for r in results]
    df["area_km2"] = [r[1] for r in results]

    return df


def add_flood_metrics(df, LOGGER=None):
    """
    Adds pixel_count and area_km2 columns to the DataFrame
    by reading each GeoTIFF in the 'filepath' column.
    Assumes 20 m resolution (400 m² per pixel).
    """

    pixel_counts = []
    areas = []

    for fp in tqdm(df["filepath"]):
        try:
            with rasterio.open(fp) as src:
                data = src.read(1)

                flooded_pixels = int(np.count_nonzero(data == 1))

                # Fixed 20m pixel size
                area_km2 = flooded_pixels * 400 / 1e6

                pixel_counts.append(flooded_pixels)
                areas.append(area_km2)

        except Exception as e:
            if LOGGER:
                LOGGER.warning(f"GeoTIFF error for {fp}: {e}")
            pixel_counts.append(None)
            areas.append(None)

    df = df.copy()
    df["pixel_count"] = pixel_counts
    df["area_km2"] = areas

    return df


def process_event(
    event,
    algorithm,
    dcs,
    results_dir: Path,
    LOGGER,
    parallel=False,
    max_workers=8,
):
    """
    Process a single flood event using file-based metrics.
    Computes flood area per file and saves results to CSV.
    """

    event_id = event["GDACS_ID"]
    country = event["country"]

    LOGGER.info(f"Event {event_id}: Processing started")

    if not dcs:
        LOGGER.warning(f"Event {event_id}: No GFM data available")
        return

    if not isinstance(dcs, list):
        dcs = [dcs]

    # Collect file registers from all AOIs
    dfs = []
    for i, dc in enumerate(dcs, start=1):
        df = dc.file_register.copy()
        df["aoi"] = f"AOI_{i}"
        dfs.append(df)

        LOGGER.info(
            f"Event {event_id}: AOI {i} has {len(df)} images"
        )

    # Merge all AOIs into one dataframe
    event_df = pd.concat(dfs, ignore_index=True)
     
    # Compute flood metrics
    LOGGER.info(f"Event {event_id}: Starting flood metrics computation")

    if parallel:
        LOGGER.info(
            f"Event {event_id}: Running flood metrics in parallel "
            f"(max_workers={max_workers})"
        )
        event_df = add_flood_metrics_parallel(event_df, max_workers=max_workers)
    else:
        LOGGER.info(f"Event {event_id}: Running flood metrics in single-threaded mode")
        event_df = add_flood_metrics(event_df, LOGGER)
    

    # Add event metadata
    event_df["event_id"] = event_id
    event_df["country"] = country

    # Save CSV
    results_dir.mkdir(parents=True, exist_ok=True)
    csv_path = results_dir / f"{event_id}_{algorithm.value}.csv"
    event_df.to_csv(csv_path, index=False)

    # Update processing results table
    results_df_path = results_dir / "processing_results.csv"
    results_df = pd.read_csv(results_df_path)

    if event_df["pixel_count"].sum() == 0:
        status = "missed"
        LOGGER.warning(f"{country} ({event_id}): No flooded pixels detected.")
    else:
        status = "detected"
        LOGGER.info(f"{country} ({event_id}): Flood detected.")

    results_df.loc[results_df["GDACS_ID"] == event_id, "processed"] = True
    results_df.loc[results_df["GDACS_ID"] == event_id, algorithm.value] = status
    results_df.to_csv(results_df_path, index=False)

    LOGGER.info(f"Event {event_id}: Processing completed")

