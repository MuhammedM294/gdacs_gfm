
from datetime import datetime ,  timedelta
from .algorithms import GFMAlgorithm, filter_algorithm_files
from .gfm_layout import get_algorithm_root
from pathlib import Path


def iterate_days(start, end, buffer_days=0):
    current = start - timedelta(days=buffer_days)
    final = end + timedelta(days=buffer_days)

    while current <= final:
        yield current
        current += timedelta(days=1)



def find_gfm_images(
    event_start: datetime,
    event_end: datetime,
    equi7_code: str,
    algorithm: GFMAlgorithm,
    buffer_days: int = 0,
) -> list[Path]:

    root = get_algorithm_root(
        event_start=event_start,
        equi7_code=equi7_code,
        algorithm=algorithm,
    )

    images: list[Path] = []

    for day in iterate_days(event_start, event_end, buffer_days):
        day_path = (
            root
            / day.strftime("%Y")
            / day.strftime("%m")
            / day.strftime("%d")
        )

        if not day_path.exists():
            continue

        day_files = list(day_path.iterdir())
        images.extend(
            filter_algorithm_files(day_files, algorithm)
        )

    return sorted(images)


if __name__ == "__main__":
    from datetime import datetime

    images = find_gfm_images(
        event_start=datetime(2023, 8, 1),
        event_end=datetime(2023, 8, 10),
        equi7_code="AS020M",
        algorithm=GFMAlgorithm.TUW,
        buffer_days=1,
    )

    
    print("\n".join(str(i) for i in images))
    print(f"Found {len(images)} images:")
