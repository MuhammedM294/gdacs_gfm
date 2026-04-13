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
import gc


def count_flooded_pixels(
    dc,
    timestamp,
    LOGGER,
    exclusion_dc=None,
    tile_threshold=10,
):
    """
    Hybrid flood pixel counting:
    - If tiles <= tile_threshold → process as full mosaic (fast)
    - If tiles > tile_threshold → process tile-wise (memory safe)

    exclusion_dc: pixels == 1 will be excluded.
    """

    flood_data = {"timestamps": [], "extents": [], "tile_name": []}

    try:
        # Select timestamp once
        dc_ts = dc.select_by_dimension(lambda x: x == timestamp, "time")

        tile_names = dc_ts["tile_name"].values
        if tile_names is None or len(tile_names) == 0:
            raise ValueError("No tiles found for timestamp")

        n_tiles = len(tile_names)

        # Prepare exclusion DC (same timestamp)
        ex_dc_ts = None
        if exclusion_dc is not None:
            try:
                ex_dc_ts = exclusion_dc.select_by_dimension(
                    lambda x: x == timestamp, "time"
                )
            except Exception as e:
                LOGGER.warning(f"Exclusion DC selection failed at {timestamp}: {e}")

        # =========================================================
        # CASE 1: SMALL AOI → LOAD FULL MOSAIC
        # =========================================================
        if n_tiles <= tile_threshold:
            try:
                dc_ts.read()
                data = dc_ts.data_view[1]

                valid_flood = (data != 0) & (data != 255)

                if ex_dc_ts is not None:
                    try:
                        ex_dc_ts.read()
                        ex_data = ex_dc_ts.data_view[1]
                        valid_flood &= ex_data != 1
                    except Exception as e:
                        LOGGER.warning(f"Exclusion mosaic failed at {timestamp}: {e}")

                total_pixels = np.count_nonzero(valid_flood)

                tile_name_str = ",".join(sorted(set(map(str, tile_names))))

                del dc_ts
                del ex_dc_ts
                gc.collect()

            except Exception as e:
                LOGGER.warning(f"Mosaic processing failed at {timestamp}: {e}")
                total_pixels = 0
                tile_name_str = ""

        # =========================================================
        # CASE 2: LARGE AOI → TILE-WISE
        # =========================================================
        else:
            total_pixels = 0
            collected_tile_names = []
            tile_names = dc_ts["filepath"].values  # Get filepaths to loop over tiles
            for tile in tile_names:
                try:
                    tile_dc = dc_ts.select_by_dimension(lambda x: x == tile, "filepath")

                    filepath_tile = tile_dc["tile_name"].values[0]
                    tile_dc.read()
                    data = tile_dc.data_view[1]

                    valid_flood = (data != 0) & (data != 255)

                    # Apply exclusion per tile
                    if ex_dc_ts is not None:
                        try:
                            ex_tile_dc = ex_dc_ts.select_by_dimension(
                                lambda x: x == filepath_tile, "tile_name"
                            )

                            ex_tile_dc.read()
                            ex_data = ex_tile_dc.data_view[1]

                            valid_flood &= ex_data != 1

                            del ex_tile_dc

                        except Exception as e:
                            LOGGER.warning(
                                f"Exclusion missing tile {tile} at {timestamp}: {e}"
                            )

                    num_pixels = np.count_nonzero(valid_flood)
                    total_pixels += num_pixels

                    collected_tile_names.append(str(filepath_tile))

                    del tile_dc
                    gc.collect()

                except Exception as e:
                    LOGGER.warning(
                        f"Tile processing failed ({filepath_tile}, {timestamp}): {e}"
                    )

            tile_name_str = ",".join(sorted(set(collected_tile_names)))

        # =========================================================
        # 🔹 FINAL COMPUTATION
        # =========================================================
        flood_area_km2 = total_pixels * 400 / 1e6

        flood_data["timestamps"].append(timestamp)
        flood_data["tile_name"].append(tile_name_str)
        flood_data["extents"].append(flood_area_km2)

        # # Cleanup
        # del dc_ts
        # if ex_dc_ts is not None:
        #     del ex_dc_ts

        gc.collect()

    except Exception as e:
        LOGGER.warning(f"DataCube Error at {timestamp}: {e}")

        flood_data["timestamps"].append(timestamp)
        flood_data["tile_name"].append("")
        flood_data["extents"].append(0.0)

    return flood_data


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
    ex_dcs,
    results_dir: Path,
    LOGGER,
):
    """
    Process a single flood event.
    For each datacube (AOI), creates a dataframe of timestamps,
    computes flood extent, aggregates tile names, and concatenates all AOIs.
    Final dataframe columns: [aoi, timestamp, flood_extent_km2, tile_name, event_id, country]
    """
    event_id = event["GDACS_ID"]
    country = event["country"]

    LOGGER.info(f"Event {event_id}: Processing started")

    if not dcs:
        LOGGER.warning(f"Event {event_id}: No GFM data available")
        return

    if not isinstance(dcs, list):
        dcs = [dcs]
    if ex_dcs is not None and not isinstance(ex_dcs, list):
        ex_dcs = [ex_dcs]

    all_dfs = []

    # Process each AOI separately
    for i, dc in enumerate(dcs, start=1):
        aoi_name = f"AOI_{i}"
        ex_dc = ex_dcs[i - 1] if ex_dcs is not None else None

        try:
            timestamps = dc["time"].values
            timestamps = sorted(set(timestamps))  # deduplicate and sort
            if len(timestamps) == 0:
                LOGGER.warning(f"Event {event_id}: {aoi_name} has no timestamps")
                continue
        except Exception as e:
            LOGGER.warning(
                f"Event {event_id}: Failed reading timestamps for {aoi_name}: {e}"
            )
            continue

        LOGGER.info(f"Event {event_id}: {aoi_name} has {len(timestamps)} timestamps")

        # Create initial dataframe for this AOI
        df = pd.DataFrame(
            {"aoi": [aoi_name] * len(timestamps), "timestamp": timestamps}
        )

        # Compute flood extent and tile names for each row
        flood_extents = []
        tile_names_list = []

        for ts in tqdm(
            df["timestamp"],
            desc=f"Processing for {aoi_name}",
            leave=False,
            unit="timestamp",
        ):
            try:

                result = count_flooded_pixels(dc, ts, LOGGER, exclusion_dc=ex_dc)

                # Flood extent
                flood_extents.append(result["extents"][0])

                # Process tile_name (flatten, deduplicate)
                tnames = result["tile_name"][0]
                flat_tile_names = []
                if tnames is not None:
                    if isinstance(tnames, (list, np.ndarray)):
                        flat_tile_names.extend(np.array(tnames).flatten().astype(str))
                    else:
                        flat_tile_names.append(str(tnames))
                tile_names_list.append(",".join(sorted(set(flat_tile_names))))

            except Exception as e:
                LOGGER.warning(f"Event {event_id}: AOI {aoi_name} failed at {ts}: {e}")
                flood_extents.append(0.0)
                tile_names_list.append("")

        df["flood_extent_km2"] = flood_extents
        df["tile_name"] = tile_names_list
        df["event_id"] = event_id
        df["country"] = country

        all_dfs.append(df)

    if not all_dfs:
        LOGGER.warning(f"Event {event_id}: No results generated")
        return

    # Concatenate all AOI dataframes
    final_df = pd.concat(all_dfs, ignore_index=True)

    # Save CSV
    results_dir.mkdir(parents=True, exist_ok=True)
    csv_path = results_dir / f"{event_id}_{algorithm.value}.csv"
    final_df.to_csv(csv_path, index=False)

    # Update processing results table
    results_df_path = results_dir / "processing_results.csv"
    results_df = pd.read_csv(results_df_path)

    if final_df["flood_extent_km2"].sum() == 0:
        status = "missed"
        LOGGER.warning(f"{country} ({event_id}): No flooded pixels detected.")
    else:
        status = "detected"
        LOGGER.info(f"{country} ({event_id}): Flood detected.")

    results_df.loc[results_df["GDACS_ID"] == event_id, "processed"] = True
    results_df.loc[results_df["GDACS_ID"] == event_id, algorithm.value] = status
    results_df.to_csv(results_df_path, index=False)

    LOGGER.info(f"Event {event_id}: Processing completed")

    return final_df


# def count_flooded_pixels2(dc, timestamp, LOGGER, exclusion_dc=None):
#     """
#     Count flooded pixels for a given timestamp and compute flooded area (km²),
#     optionally masking out pixels from an exclusion datacube (exclusion_dc).

#     Pixels in exclusion_dc == 1 are ignored in the flood count.
#     """

#     flood_data = {"timestamps": [], "extents": [], "tile_name": []}

#     try:
#         # Select and load flood data
#         mosaic = dc.select_by_dimension(lambda x: x == timestamp, "time")
#         mosaic.read()
#         data = mosaic.data_view[1]

#         # Mask valid flood pixels (exclude 0 and 255)
#         valid_flood = (data != 0) & (data != 255)

#         # Apply exclusion mask if provided
#         if exclusion_dc is not None:
#             try:
#                 ex_mosaic = exclusion_dc.select_by_dimension(lambda x: x == timestamp, "time")
#                 ex_mosaic.read()
#                 ex_data = ex_mosaic.data_view[1]

#                 # Exclude pixels where ex_data == 1
#                 valid_flood &= (ex_data != 1)

#                 del ex_mosaic
#             except Exception as e:
#                 LOGGER.warning(f"Exclusion DataCube Error at {timestamp}: {e}")

#         # Count flooded pixels
#         num_flood_pixels = np.count_nonzero(valid_flood)

#         # Compute area (km²)
#         flood_area_km2 = num_flood_pixels * 400 / 1e6

#         # Tile name
#         tile_name = mosaic["tile_name"].values

#         # Always store result
#         flood_data["timestamps"].append(timestamp)
#         flood_data["tile_name"].append(tile_name)
#         flood_data["extents"].append(flood_area_km2)

#         # Cleanup
#         del mosaic
#         gc.collect()

#     except Exception as e:
#         LOGGER.warning(f"DataCube Error at {timestamp}: {e}")
#         flood_data["timestamps"].append(timestamp)
#         flood_data["tile_name"].append(None)
#         flood_data["extents"].append(0.0)

#     return flood_data


# def count_flooded_pixels1(dc, timestamp, LOGGER, exclusion_dc=None):
#     """
#     Memory-efficient flood pixel counting (tile-wise).
#     Optimized: select by timestamp once, then loop over tiles.

#     exclusion_dc: pixels == 1 will be excluded.
#     """

#     flood_data = {"timestamps": [], "extents": [], "tile_name": []}

#     total_flood_pixels = 0
#     collected_tile_names = []

#     try:
#         # Select timestamp ONCE
#         dc_ts = dc.select_by_dimension(lambda x: x == timestamp, "time")

#         # Get tile names
#         tile_names = dc_ts["tile_name"].values

#         if tile_names is None or len(tile_names) == 0:
#             raise ValueError("No tiles found for timestamp")

#         # Prepare exclusion dc (same timestamp)
#         ex_dc_ts = None
#         if exclusion_dc is not None:
#             try:
#                 ex_dc_ts = exclusion_dc.select_by_dimension(
#                     lambda x: x == timestamp, "time"
#                 )

#                 assert len(ex_dc_ts) > 0, "Exclusion DC has no data for timestamp"

#             except Exception as e:
#                 LOGGER.warning(f"Exclusion DC timestamp selection failed at {timestamp}: {e}")


#         # Loop over tiles
#         for tile in tqdm(tile_names , desc=f"Processing tiles for {timestamp}", leave=False, unit="tile"):
#             try:
#                 # Select tile only (timestamp already filtered)
#                 tile_dc = dc_ts.select_by_dimension(
#                     lambda x: x == tile, "filepath"
#                 )
#                 in_dc_tile = tile_dc['tile_name'].values[0]

#                 tile_dc.read()
#                 data = tile_dc.data_view[1]

#                 # Flood mask
#                 valid_flood = (data != 0) & (data != 255)

#                 # Apply exclusion (same tile)
#                 if ex_dc_ts is not None:
#                     try:
#                         ex_tile_dc = ex_dc_ts.select_by_dimension(
#                             lambda x: x == in_dc_tile, "tile_name"
#                         )

#                         ex_tile_dc.read()
#                         ex_data = ex_tile_dc.data_view[1]

#                         valid_flood &= (ex_data != 1)

#                         del ex_tile_dc

#                     except Exception as e:
#                         LOGGER.warning(
#                             f"Exclusion missing tile {tile} at {timestamp}: {e}"
#                         )

#                 # Count pixels
#                 num_pixels = np.count_nonzero(valid_flood)
#                 total_flood_pixels += num_pixels

#                 collected_tile_names.append(str(in_dc_tile))

#                 # Cleanup
#                 del tile_dc

#             except Exception as e:
#                 LOGGER.warning(
#                     f"Tile processing failed ({tile}, {timestamp}): {e}"
#                 )

#         # Compute area
#         flood_area_km2 = total_flood_pixels * 400 / 1e6

#         # Deduplicate tile names
#         tile_name_str = ",".join(sorted(set(collected_tile_names)))

#         flood_data["timestamps"].append(timestamp)
#         flood_data["tile_name"].append(tile_name_str)
#         flood_data["extents"].append(flood_area_km2)

#         # Cleanup
#         del dc_ts
#         if ex_dc_ts is not None:
#             del ex_dc_ts

#         gc.collect()

#     except Exception as e:
#         LOGGER.warning(f"DataCube Error at {timestamp}: {e}")

#         flood_data["timestamps"].append(timestamp)
#         flood_data["tile_name"].append("")
#         flood_data["extents"].append(0.0)

#     return flood_data
