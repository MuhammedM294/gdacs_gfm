from enum import Enum
from pathlib import Path


class GFMAlgorithm(Enum):
    ENSEMBLE = "ensemble"
    TUW = "tuw"
    LIST = "list"
    DLR = "dlr"


def filter_algorithm_files(
    files: list[Path],
    algorithm: GFMAlgorithm,
) -> list[Path]:

    tif_files = [f for f in files if f.suffix == ".tif"]

    if algorithm == GFMAlgorithm.ENSEMBLE:
        return tif_files

    if algorithm == GFMAlgorithm.LIST:
        return [f for f in tif_files if f.name.startswith("LIST")]

    if algorithm == GFMAlgorithm.DLR:
        return [f for f in tif_files if f.name.startswith("DLR")]

    if algorithm == GFMAlgorithm.TUW:
        tuw = [f for f in tif_files if f.name.startswith("TUW")]
        if tuw:
            return tuw

        # fallback: FLOOD-HM
        return [f for f in tif_files if f.name.startswith("FLOOD-HM")]

    return []
