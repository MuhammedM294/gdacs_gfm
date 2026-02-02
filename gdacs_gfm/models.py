from dataclasses import dataclass
from pathlib import Path
from datetime import datetime



@dataclass(frozen=True)
class FloodEvent:
    id: int
    country: str
    fromdate: datetime
    todate: datetime
    equi7code: str
    aoi_path: Path
    continent: str
    country: str
    alert_level: str