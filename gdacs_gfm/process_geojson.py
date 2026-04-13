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

from shapely.geometry import Polygon, Point, mapping
from shapely.ops import transform
import pyproj

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

    # # Keep only the 5 largest polygons if more than 5 exist
    # if len(polygons) > 5:
    #     polygons_with_area = [
    #         (poly, compute_polygons_area_km2(poly)) for poly in polygons
    #     ]
    #     polygons_with_area.sort(key=lambda x: x[1], reverse=True)
    #     polygons = [poly for poly, _ in polygons_with_area[:5]]

    #     logger.info(
    #         f"Event ({event_id}): Reduced polygons to 5 largest by area (km²)"
    #     )

    # # GDACS AOIs are WGS84
    # sref = SpatialRef(4326)

        # Compute areas and remove duplicates based on area
    polygons_with_area = [
        (poly, compute_polygons_area_km2(poly)) for poly in polygons
    ]

    polygons_with_area.sort(key=lambda x: x[1], reverse=True)

    unique = {}
    for poly, area in polygons_with_area:
        # Use rounded area to avoid tiny floating point differences
        key = round(area, 6)
        if key not in unique:
            unique[key] = poly

    polygons = list(unique.values())

    # Keep only the 5 largest polygons if more than 5 exist
    if len(polygons) > 5:
        polygons_with_area = [
            (poly, compute_polygons_area_km2(poly)) for poly in polygons
        ]
        polygons_with_area.sort(key=lambda x: x[1], reverse=True)
        polygons = [poly for poly, _ in polygons[:5]]

        logger.info(
            f"Event ({event_id}): Reduced polygons to 5 largest by area (km²)"
        )

    # GDACS AOIs are WGS84
    sref = SpatialRef(4326)

    return polygons, sref





def load_event_geojson_no_aoi(
    event_id: str,
    geojson_dir: Union[str, Path],
    point_buffer_radius_km: float = 75.0,
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
    points: List[Tuple[float, float]] = []

    logger.info(f"Number of features in GeoJSON: {len(features)}")

    for feature in features:
        geom = feature.get("geometry")
        if geom is None:
            continue
        geom_type = geom.get("type")
        geom_coords = geom.get("coordinates")

        if geom_type == "Polygon":
            polygons.append(Polygon((lat, lon) for lon, lat in geom_coords[0]))
        elif geom_type == "MultiPolygon":
            for poly_coords in geom_coords:
                polygons.append(Polygon((lat, lon) for lon, lat in poly_coords[0]))
        elif geom_type == "Point":
            points.append(tuple(geom_coords))  # (lon, lat)

    # Handle only Point geometries if no polygon was found
    if not polygons and points:
        unique_points = list({pt for pt in points})  # remove duplicates
        logger.info(f"Creating polygons from {len(unique_points)} unique points")

        # WGS84 projection to local meters for accurate buffer
        project_to_m = pyproj.Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True).transform
        project_to_deg = pyproj.Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True).transform

        for lon, lat in unique_points:
            point_geom = Point(lon, lat)
            # Project to meters
            point_m = transform(project_to_m, point_geom)
            # Buffer in meters (50 km = 50_000 m)
            buffered_m = point_m.buffer(point_buffer_radius_km * 1000)
            # Back to degrees
            buffered_deg = transform(project_to_deg, buffered_m)
            polygons.append(buffered_deg)

    if not polygons:
        logger.warning(f"Event ({event_id}): No valid polygon found in GeoJSON")
        return None, None
    
    # GDACS AOIs are WGS84
    sref = SpatialRef(4326)
    # Assuming WGS84
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

    event_id = "FL-1103771"
    geojson_dir = "/eodc/private/tuwgeo/users/mabdelaa/repos/GDACS_Flood_DB/data/aois"

    p,s = load_event_geojson(event_id, geojson_dir)

    print(len(p))
    print(p)
    for pp in p:
        print(compute_polygons_area_km2(pp))
