from __future__ import annotations
from pathlib import Path
from typing import List, Union, Tuple
from shapely.ops import transform
import pyproj
import json
from shapely.geometry import Polygon, shape, Point
from geospade.crs import SpatialRef
from shapely.geometry import Polygon
import numpy as np
import logging

logger = logging.getLogger("gfm_logger")


def compute_polygons_area_km2(polygons: Union[Polygon, List[Polygon]]) -> float:
    """
    Compute area in km² for a polygon or list of polygons.
    """
    if isinstance(polygons, Polygon):
        polygons = [polygons]

    # Project to Web Mercator (meters)
    project = pyproj.Transformer.from_crs(
        "EPSG:4326", "EPSG:3857", always_xy=True
    ).transform

    total_area_m2 = 0.0
    for poly in polygons:
        poly_m = transform(project, poly)
        total_area_m2 += poly_m.area

    return total_area_m2 / 1_000_000  # km²


def load_event_geojson(
    event_id: str,
    geojson_dir: Union[str, Path],
    point_buffer_radius: float = 0.1,
) -> Tuple[List[Polygon], SpatialRef]:

    geojson_path = Path(geojson_dir) / f"{event_id}.json"
    if not geojson_path.exists():
        raise FileNotFoundError(f"GeoJSON file not found: {geojson_path}")

    with geojson_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    features = data.get("features", [])
    if not features:
        raise ValueError(f"No features found in GeoJSON file: {geojson_path}")

    polygons: List[Polygon] = []
    logger.info(f"Number of features in GeoJSON: {len(features)}")
    for feature in features:
        geom = feature.get("geometry")
        geom_type = geom.get("type") if geom else None
        geom_coords = geom.get("coordinates") if geom else None
        if geom is None:
            continue

        if geom_type == "Point":
            continue

        if geom_type == "Polygon":
            coor = geom_coords
            polygons.append(Polygon((lat, lon) for lon, lat in coor[0]))
            continue

        if geom_type == "MultiPolygon":
            coor = geom_coords
            for poly_coor in coor:
                polygons.append(Polygon((lat, lon) for lon, lat in poly_coor[0]))
            continue

    if not polygons:
        logger.warning(f"Event ({event_id}): No valid polygon found in GeoJSON")
        return None, None

    # GDACS AOIs are WGS84
    sref = SpatialRef(4326)

    return polygons, sref


# --- DATA CUBE FILTERING --->
def filterby_dc_poly(dc, poly, sref, event_id, LOGGER=None):
    try:
        dc_sel = dc.select_polygon(poly, sref)

    except Exception as e:
        if LOGGER:
            LOGGER.info(
                f"Event ({event_id}): polygon selection failed, "
                f"trying simplification. Error: {e}"
            )

        dc_sel = None

        # progressively simplify geometry
        for tolerance in (0.01 * i for i in range(1, 11)):
            try:
                poly_simplified = poly.simplify(tolerance, preserve_topology=False)
                dc_sel = dc.select_polygon(poly_simplified, sref)
                if dc_sel is not None:
                    break
            except Exception:
                continue

        # final fallback → bounding box
        if dc_sel is None:
            if LOGGER:
                LOGGER.warning(
                    f"Event ({event_id}): Simplification failed, using bounding box"
                )

            x, y = poly.exterior.coords.xy
            bbox = Polygon(
                [
                    (min(x), min(y)),
                    (max(x), min(y)),
                    (max(x), max(y)),
                    (min(x), max(y)),
                ]
            )

            try:
                dc_sel = dc.select_polygon(bbox, sref)
            except Exception:
                if LOGGER:
                    LOGGER.warning(f"Event ({event_id}): Bounding box selection failed")
                return None

    # sanity check: selection actually reduced data
    if dc_sel is None or len(dc_sel) == len(dc):
        if LOGGER:
            LOGGER.warning(
                f"Event ({event_id}): AOI selection returned full cube or no data"
            )
        return None

    return dc_sel


if __name__ == "__main__":
    from pprint import pprint

    event_id = "FL-4215"
    geojson_dir = "/eodc/private/tuwgeo/users/mabdelaa/repos/GDACS_Flood_DB/data/aois"

    geometry = load_event_geojson(event_id, geojson_dir)

    pprint(geometry)
    print(compute_polygons_area_km2(geometry))
