import csv
from pathlib import Path
from tqdm import tqdm
import numpy as np
import gc


def count_flooded_pixels(dc, timestamp, LOGGER):
    """
    Count flooded pixels for a single timestamp in a DataCube.
    Returns None if no flooded pixels are found.
    """

    try:
        mosaic = dc.select_by_dimension(lambda x: x == timestamp, "time")
        mosaic.read()

        data_arr = mosaic.data_view.rename_vars({1: "flood_extent"})
        data_arr["flood_extent"] = data_arr["flood_extent"].where(
            ~np.isin(data_arr["flood_extent"], [0, 255]), 0
        )

        flooded_pixels = int(np.sum(data_arr["flood_extent"]))

        if flooded_pixels == 0:
            return None

        extent_km2 = (flooded_pixels * 20 * 20) / 1e6

        return {
            "timestamp": timestamp,
            "extent_km2": extent_km2,
            "tile_name": mosaic["tile_name"].values,
        }

    except Exception as e:
        LOGGER.warning(f"DataCube error at {timestamp}: {e}")
        return None

    finally:
        del mosaic
        gc.collect()

def log_event_no_data(event, LOGGER, reason: str):
    LOGGER.info("==============================================")
    LOGGER.warning(f"{event.country} ({event.gdacs_id}): {reason}")
    LOGGER.info("==============================================")




def process_event(
    event,
    algorithm,
    dcs,
    results_dir: Path,
    LOGGER,
):
    """
    Process a single flood event and persist flood statistics to CSV.
    """

    event_id = event.GDACS_ID
    country = event.country

    LOGGER.info(f"Event {event_id}: Processing started")


    if not dcs:
        log_event_no_data(event, LOGGER, "No GFM data available")
        return

    if not isinstance(dcs, list):
        dcs = [dcs]

    results_dir.mkdir(parents=True, exist_ok=True)
    csv_path = results_dir / f"{event_id}_{algorithm.value}.csv"

    write_header = not csv_path.exists()

    with csv_path.open("a", newline="") as f:
        writer = csv.writer(f)

        if write_header:
            writer.writerow(
                ["event_id", "country", "aoi", "timestamp", "extent_km2", "tile_name"]
            )

        for i, dc in enumerate(dcs, start=1):
            timestamps = sorted(set(dc["time"].values))
            LOGGER.info(
                f"Event {event_id}: AOI {i}/{len(dcs)} "
                f"({len(timestamps)} images)"
            )

            for ts in tqdm(timestamps, desc=f"{event_id} AOI {i}", unit="image"):
                stats = count_flooded_pixels(dc, ts, LOGGER)

                if not stats:
                    continue

                writer.writerow(
                    [
                        event_id,
                        country,
                        f"AOI_{i}",
                        stats["timestamp"],
                        stats["extent_km2"],
                        stats["tile_name"].flatten().tolist(),
                    ]
                )

    LOGGER.info(f"Event {event_id}: Processing completed")
