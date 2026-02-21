from datetime import datetime
from pathlib import Path
from typing import Optional, List
import pandas as pd
from pathlib import Path
import shutil
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import pandas as pd
import rasterio
from tqdm import tqdm
import logging



from .datacube import build_datacube
from .gfm_layout import resolve_storage_root
from .algorithms import GFMAlgorithm, filter_algorithm_files
from .gfm_index import iterate_days
from .config import (
    DIMENSIONS,
    FL_DEF_DICT,
    UN_DEF_DICT,
    NRT_EXCLUSION_FIELDS_DEF,
    ARCH_EXCLUSION_FIELDS_DEF,
    NRT_OBS_FIELDS_DEF,
    ARCH_OBS_FIELDS_DEF,
    NRT_ADV_FIELDS_DEF,
    ARCH_ADV_FIELDS_DEF
)

logger = logging.getLogger("gfm_logger")


def get_gfm_layers_dirs(
    start_date: datetime = datetime(2024, 6, 12),
    equi7_grid: Optional[str] = None,
    gfm_algorithm: Optional[GFMAlgorithm] = None,
):
    if equi7_grid is None:
        raise ValueError("equi7_grid must be provided")

    storage_root = resolve_storage_root(start_date)

    # Select correct root based on algorithm
    if gfm_algorithm.value == "ensemble":
        storage_root = storage_root / "layers"
    else:
        storage_root = storage_root / "interim_layers"

    # Main layers
    root_layers = ["flood_extent", "uncertainty"]
    root_dirs = [storage_root / layer / equi7_grid for layer in root_layers]

    # Context layers
    # Observed water naming
    observed_name = (
        "observed_water" if storage_root.parent.name == "output" else "obswater_mask"
    )

    exclusion_name = (
        "exclusion_layer" if storage_root.parent.name == "output" else "exlusion_mask"
    )
    context_layers = [exclusion_name, observed_name, "advisory_flags"]
    context_root = storage_root.parent / "layers"
    context_dirs = [context_root / layer / equi7_grid for layer in context_layers]

    all_dirs = root_dirs + context_dirs

    # Validate existence
    missing = [str(p) for p in all_dirs if not p.exists()]
    if missing:
        raise FileNotFoundError(
            "The following GFM layer directories do not exist:\n" + "\n".join(missing)
        )

    return all_dirs




def find_gfm_layers_images(
    event_start: datetime,
    event_end: datetime,
    equi7_code: str,
    algorithm: GFMAlgorithm,
    buffer_days: int = 0,
) -> list[list[Path]]:
    layers_dirs = get_gfm_layers_dirs(event_start, equi7_code, algorithm)

    # Pre-allocate based on actual number of layers
    layers_images: list[list[Path]] = [[] for _ in layers_dirs]

    # Precompute days once
    days = list(iterate_days(event_start, event_end, buffer_days))

    for i, layer_dir in enumerate(layers_dirs):
        layer_name = layer_dir.parent.name  

        for day in days:
            day_path = (
                layer_dir
                / day.strftime("%Y")
                / day.strftime("%m")
                / day.strftime("%d")
            )

            if not day_path.exists():
                continue

            day_files = list(day_path.iterdir())

            if layer_name in {"flood_extent", "uncertainty"}:
                layers_images[i].extend(
                    filter_algorithm_files(day_files, algorithm)
                )
            else:
                layers_images[i].extend(
                    f for f in day_files if f.suffix == ".tif"
                )

    return layers_images


def select_field_defs(
    algo: GFMAlgorithm,
    uncert_root: str
):
    """Return the appropriate field definitions based on algorithm and storage type."""
    FL_FIELDS_DEF = FL_DEF_DICT[algo.value]

    EX_FIELDS_DEF = NRT_EXCLUSION_FIELDS_DEF if uncert_root != "output" else ARCH_EXCLUSION_FIELDS_DEF
    OBS_FIELDS_DEF = NRT_OBS_FIELDS_DEF if uncert_root != "output" else ARCH_OBS_FIELDS_DEF
    ADV_FIELDS_DEF = NRT_ADV_FIELDS_DEF if uncert_root != "output" else ARCH_ADV_FIELDS_DEF

    if algo.value == "ensemble":
        UN_FIELDS_DEF = UN_DEF_DICT["ensemble"]
    else:
        UN_FIELDS_DEF = UN_DEF_DICT["archive"] if uncert_root == "output" else UN_DEF_DICT["nrt"]
        if algo.value == "list":
            UN_FIELDS_DEF["algorithm"] = {"len": 4}

    return FL_FIELDS_DEF, UN_FIELDS_DEF, EX_FIELDS_DEF, OBS_FIELDS_DEF, ADV_FIELDS_DEF


def copy_files(file_paths, destination_dir , logger=None):
    """
    Copy a list of files to a destination directory.
    
    """
    destination_dir = Path(destination_dir)
    destination_dir.mkdir(parents=True, exist_ok=True)

    for file_path in file_paths:
        src = Path(file_path)

        if src.exists():
            dst = destination_dir / src.name
            shutil.copy2(src, dst)  # preserves metadata
            logger.info(f"Copied Files-> {dst}")
        else:
            logger.warning(f"Missing: {src}")





if __name__ == "__main__":


    DB_PATH = Path(
    "/eodc/private/tuwgeo/users/mabdelaa/repos/GDACS_Flood_DB/data/latest_gdacs_flood_db.csv"
    )
    GEOJSON_DIR = Path("/eodc/private/tuwgeo/users/mabdelaa/repos/GDACS_Flood_DB/data/aois")

    df = pd.read_csv(DB_PATH)
    ALGO = GFMAlgorithm.TUW
    images = find_gfm_layers_images(
         event_start=datetime(2024, 8, 1),
         event_end=datetime(2024, 8, 10),
         equi7_code="AS020M",
         algorithm=ALGO,
         buffer_days=1,
    )
   
    
 
    fl, uncer, exc, obsw, adv = images
    uncert_root = uncer[0].parents[6].name
    fl, uncer, exc, obsw, adv = map(lambda l: [str(p) for p in l], [fl, uncer, exc, obsw, adv])
    
    
    FL_FIELDS_DEF, UN_FIELDS_DEF, EX_FIELDS_DEF, OBS_FIELDS_DEF, ADV_FIELDS_DEF = select_field_defs(ALGO, uncert_root)

  
    fl_dc = build_datacube(
        fl,
        DIMENSIONS,
        FL_FIELDS_DEF,
    )
   

    