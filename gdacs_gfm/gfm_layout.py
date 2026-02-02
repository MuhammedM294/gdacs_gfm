from pathlib import Path
from .algorithms import GFMAlgorithm
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


@dataclass(frozen=True)
class GFMStoragePeriod:
    name: str
    start: datetime
    end: datetime
    root_dir: Path


def get_gfm_storage_periods():
    today = datetime.today()

    return [
        GFMStoragePeriod(
            name="archive",
            start=datetime(2015, 1, 1),
            end=datetime(2022, 12, 31),
            root_dir=Path("/eodc/private/jrc_gfm/gfm_scratch/historical-flood/V02/process/output"),
        ),
        GFMStoragePeriod(
            name="nrt",
            start=datetime(2023, 1, 1),
            end=today,
            root_dir=Path("/eodc/private/jrc_gfm/gfm_scratch/realtime"),
        ),
    ]

def resolve_storage_root(event_start: datetime) -> Path:
    GFM_STORAGE_PERIODS = get_gfm_storage_periods()
    for period in GFM_STORAGE_PERIODS:
        if period.start <= event_start and (
            period.end is None or event_start <= period.end
        ):
            return period.root_dir

    raise ValueError(
        f"No GFM storage period defined for date {event_start}"
    )


def get_algorithm_root(
    event_start: datetime,
    equi7_code: str,
    algorithm: GFMAlgorithm,
) -> Path:

    storage_root = resolve_storage_root(event_start)

    if algorithm == GFMAlgorithm.ENSEMBLE:
        return storage_root / "layers/flood_extent"/ equi7_code

    return storage_root / "interim_layers/flood_extent" / equi7_code


if __name__ == "__main__":
    test = resolve_storage_root(datetime(2025, 6, 15))
    print(test)