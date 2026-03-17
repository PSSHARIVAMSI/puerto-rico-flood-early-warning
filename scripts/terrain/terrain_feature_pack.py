"""Terrain Feature Pack v1 for Puerto Rico."""

from __future__ import annotations

import argparse
import importlib.util
import json
import logging
import subprocess
import sys
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.request import urlretrieve


def ensure_packages(packages: dict[str, str]) -> None:
    missing = [spec for module, spec in packages.items() if importlib.util.find_spec(module) is None]
    if missing:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet", *missing])


ensure_packages(
    {
        "yaml": "pyyaml",
        "numpy": "numpy",
        "pandas": "pandas",
        "geopandas": "geopandas",
        "rasterio": "rasterio",
        "rasterstats": "rasterstats",
        "shapely": "shapely",
        "pyproj": "pyproj",
        "scipy": "scipy",
        "pyarrow": "pyarrow",
        "pyogrio": "pyogrio",
    }
)

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
import yaml
from pyproj import CRS
from rasterio.enums import Resampling
from rasterio.merge import merge
from rasterio.vrt import WarpedVRT
from rasterstats import zonal_stats
from scipy.ndimage import uniform_filter
from shapely.ops import unary_union


FIELD_DESCRIPTIONS = {
    "municipio_id": "Puerto Rico municipio identifier from Census county-equivalent boundaries.",
    "municipio_name": "Municipio name from source boundary layer.",
    "municipio_key": "Normalized municipio key for easier joins in future stages.",
    "elevation_mean": "Mean DEM elevation within municipio polygon (meters).",
    "slope_mean": "Mean slope in degrees derived from DEM.",
    "slope_p90": "90th percentile slope in degrees derived from DEM.",
    "local_relief": "Municipio max DEM minus min DEM (meters).",
    "wetness_proxy": "Mean local-depression/inverse-slope terrain wetness proxy (screening metric, not hydraulic depth).",
    "distance_to_stream_km": "Nearest-stream distance in kilometers from municipio centroid.",
    "coastal_inundation_flag": "Binary coastal inundation indicator when coastal source data are present.",
    "coastal_inundation_depth_mean": "Mean coastal inundation depth within municipio where raster depth is available.",
    "soil_runoff_potential": "Area-weighted runoff potential derived from soil hydrologic group mapping.",
    "land_cover_runoff_modifier": "Mean runoff modifier derived from land-cover class mapping.",
    "terrain_data_completeness": "Percent of required terrain fields populated for the municipio (0-100).",
    "terrain_confidence_score": "Weighted terrain-source confidence score based on source availability (0-100).",
    "config_version": "Terrain config version used for the run.",
    "run_timestamp_utc": "UTC timestamp when outputs were generated.",
}


def find_repo_root(start: Path | None = None) -> Path:
    probe = (start or Path.cwd()).resolve()
    for candidate in [probe, *probe.parents]:
        if (candidate / "README.md").exists() and (candidate / "JupyterNotebooks").exists():
            return candidate
    return probe


def slugify(value: str) -> str:
    value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    value = value.lower().replace("&", "and")
    value = "".join(ch if ch.isalnum() else "_" for ch in value)
    return "_".join(part for part in value.split("_") if part)


def read_yaml(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text())


def discover_paths(repo_root: Path, patterns: Iterable[str]) -> list[Path]:
    seen: set[Path] = set()
    matches: list[Path] = []
    for pattern in patterns:
        for match in sorted(repo_root.glob(pattern)):
            if match.is_file() and match not in seen:
                matches.append(match)
                seen.add(match)
    return matches


def warn_and_record(logger: logging.Logger, warnings_list: list[str], message: str) -> None:
    logger.warning(message)
    warnings_list.append(message)


def read_vector(paths: list[Path], layer_name: str | None = None) -> gpd.GeoDataFrame:
    frames: list[gpd.GeoDataFrame] = []
    for path in paths:
        frame = gpd.read_file(path, layer=layer_name) if layer_name else gpd.read_file(path)
        if len(frame):
            frames.append(frame)
    if not frames:
        return gpd.GeoDataFrame(geometry=[], crs=None)
    return gpd.GeoDataFrame(pd.concat(frames, ignore_index=True), geometry="geometry", crs=frames[0].crs)


def read_projected_raster(
    paths: list[Path],
    target_crs: str,
    resampling_name: str = "nearest",
) -> tuple[np.ndarray, Any, Any]:
    resampling = getattr(Resampling, resampling_name, Resampling.nearest)
    opened: list[Any] = []
    warped: list[Any] = []
    try:
        datasets = []
        for path in paths:
            src = rasterio.open(path)
            opened.append(src)
            ds = src
            if src.crs is not None and CRS.from_user_input(src.crs) != CRS.from_user_input(target_crs):
                ds = WarpedVRT(src, crs=target_crs, resampling=resampling)
                warped.append(ds)
            datasets.append(ds)
        mosaic, transform = merge(datasets)
        arr = mosaic[0].astype("float32")
        crs = datasets[0].crs
        nodata = datasets[0].nodata
        if nodata is not None:
            arr = np.where(arr == nodata, np.nan, arr)
        return arr, transform, crs
    finally:
        for ds in warped:
            ds.close()
        for src in opened:
            src.close()


def build_local_mean(array: np.ndarray, kernel_size: int) -> np.ndarray:
    valid = np.isfinite(array)
    filled = np.where(valid, array, 0.0)
    mean_values = uniform_filter(filled, size=kernel_size, mode="nearest")
    mean_mask = uniform_filter(valid.astype("float32"), size=kernel_size, mode="nearest")
    with np.errstate(divide="ignore", invalid="ignore"):
        local_mean = np.where(mean_mask > 0, mean_values / mean_mask, np.nan)
    return local_mean


def derive_slope_degrees(elevation: np.ndarray, transform: Any) -> np.ndarray:
    valid = np.isfinite(elevation)
    if not valid.any():
        return np.full_like(elevation, np.nan, dtype="float32")
    fill_value = float(np.nanmedian(elevation))
    filled = np.where(valid, elevation, fill_value)
    cell_x = abs(float(transform.a))
    cell_y = abs(float(transform.e))
    grad_y, grad_x = np.gradient(filled, cell_y, cell_x)
    slope = np.degrees(np.arctan(np.hypot(grad_x, grad_y))).astype("float32")
    slope[~valid] = np.nan
    return slope


def derive_wetness_proxy(
    elevation: np.ndarray,
    slope_deg: np.ndarray,
    transform: Any,
    neighborhood_m: float,
    min_slope_deg: float,
    clip_percentile: float,
) -> np.ndarray:
    if not np.isfinite(elevation).any():
        return np.full_like(elevation, np.nan, dtype="float32")
    mean_cell_m = max((abs(float(transform.a)) + abs(float(transform.e))) / 2.0, 1.0)
    kernel_size = max(3, int(round(neighborhood_m / mean_cell_m)))
    if kernel_size % 2 == 0:
        kernel_size += 1
    local_mean = build_local_mean(elevation, kernel_size)
    local_depression = np.clip(local_mean - elevation, 0.0, None)
    slope_rad = np.deg2rad(np.clip(slope_deg, min_slope_deg, None))
    with np.errstate(divide="ignore", invalid="ignore"):
        wetness = local_depression / np.tan(slope_rad)
    wetness[~np.isfinite(elevation)] = np.nan
    if np.isfinite(wetness).any():
        clip_value = float(np.nanpercentile(wetness, clip_percentile))
        wetness = np.clip(wetness, 0.0, clip_value)
    return wetness.astype("float32")


def array_for_zonal_stats(array: np.ndarray, sentinel: float = -999999.0) -> tuple[np.ndarray, float]:
    prepared = np.where(np.isfinite(array), array, sentinel).astype("float32")
    return prepared, sentinel


def zonal_metric(
    geoms: Iterable[Any],
    array: np.ndarray,
    affine: Any,
    stats: list[str] | None = None,
    add_stats: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    prepared, nodata = array_for_zonal_stats(array)
    return zonal_stats(
        list(geoms),
        prepared,
        affine=affine,
        nodata=nodata,
        stats=stats or [],
        add_stats=add_stats or {},
        all_touched=False,
    )


def percentile_stat(percentile: float, nodata: float) -> Any:
    def calc(values: np.ndarray) -> float:
        arr = np.asarray(values, dtype="float64")
        arr = arr[np.isfinite(arr)]
        arr = arr[arr != nodata]
        if arr.size == 0:
            return float("nan")
        return float(np.nanpercentile(arr, percentile))

    return calc


def assign_zonal_columns(
    base: pd.DataFrame,
    stats_rows: list[dict[str, Any]],
    mapping: dict[str, str],
) -> None:
    for dest, src in mapping.items():
        base[dest] = [row.get(src, np.nan) for row in stats_rows]


def normalize_soil_group(value: Any, mapping: dict[str, float]) -> float:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return float("nan")
    token = str(value).strip().upper()
    if not token:
        return float("nan")
    if token in mapping:
        return float(mapping[token])
    if "/" in token:
        parts = [p for p in token.split("/") if p]
        scores = [mapping[p] for p in parts if p in mapping]
        if scores:
            return float(np.mean(scores))
    return float("nan")


def find_first_existing_column(columns: Iterable[str], candidates: Iterable[str]) -> str | None:
    lookup = {col.lower(): col for col in columns}
    for candidate in candidates:
        if candidate.lower() in lookup:
            return lookup[candidate.lower()]
    return None


def standardize_municipios(
    boundaries: gpd.GeoDataFrame,
    config: dict[str, Any],
    target_crs: str,
) -> gpd.GeoDataFrame:
    automated = config["boundaries"]["automated_fetch"]
    id_field = automated["id_field"]
    name_field = automated["name_field"]
    frame = boundaries.copy()
    if frame.crs is None:
        frame = frame.set_crs("EPSG:4326")
    frame = frame.to_crs(target_crs)
    frame = frame[frame.geometry.notnull()].copy()
    frame = frame[~frame.geometry.is_empty].copy()
    if "municipio_id" not in frame.columns:
        frame["municipio_id"] = frame[id_field].astype(str)
    else:
        frame["municipio_id"] = frame["municipio_id"].astype(str)
    if "municipio_name" not in frame.columns:
        frame["municipio_name"] = frame[name_field].astype(str)
    else:
        frame["municipio_name"] = frame["municipio_name"].astype(str)
    if "municipio_key" not in frame.columns:
        frame["municipio_key"] = frame["municipio_name"].map(slugify)
    else:
        frame["municipio_key"] = frame["municipio_key"].astype(str)
    keep = ["municipio_id", "municipio_name", "municipio_key", "geometry"]
    return frame[keep].sort_values("municipio_name").reset_index(drop=True)


def load_municipio_boundaries(
    repo_root: Path,
    config: dict[str, Any],
    logger: logging.Logger,
    warnings_list: list[str],
    source_summary: dict[str, Any],
) -> gpd.GeoDataFrame:
    target_crs = config["runtime"]["working_crs"]
    local_patterns = config["boundaries"]["local_patterns"]
    local_paths = discover_paths(repo_root, local_patterns)
    if local_paths:
        selected = local_paths[0]
        source_summary["boundaries"] = {
            "status": "local",
            "path": str(selected.relative_to(repo_root)),
        }
        boundaries = read_vector([selected])
        return standardize_municipios(boundaries, config, target_crs)

    fetch_cfg = config["boundaries"]["automated_fetch"]
    if not fetch_cfg.get("enabled", False):
        raise FileNotFoundError("No local municipio boundary file found and automated fetch is disabled.")

    zip_path = repo_root / fetch_cfg["download_to"]
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    if not zip_path.exists():
        logger.info("Downloading municipio boundaries from %s", fetch_cfg["url"])
        urlretrieve(fetch_cfg["url"], zip_path)

    shapefile_ref = f"zip://{zip_path}!tl_2023_us_county.shp"
    raw = gpd.read_file(shapefile_ref)
    raw = raw[raw[fetch_cfg["state_field"]].astype(str) == str(fetch_cfg["state_code"])].copy()
    standardized = standardize_municipios(raw, config, target_crs)

    cache_path = repo_root / config["runtime"]["default_boundary_cache"]
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    standardized.to_file(cache_path, driver="GeoJSON", engine="pyogrio")

    source_summary["boundaries"] = {
        "status": "fetched",
        "download": fetch_cfg["url"],
        "cache_path": str(cache_path.relative_to(repo_root)),
    }
    warn_and_record(
        logger,
        warnings_list,
        f"Municipio boundaries were fetched automatically and cached at {cache_path.relative_to(repo_root)}.",
    )
    return standardized


def compute_dem_features(
    municipios: gpd.GeoDataFrame,
    repo_root: Path,
    config: dict[str, Any],
    logger: logging.Logger,
    warnings_list: list[str],
    source_summary: dict[str, Any],
) -> pd.DataFrame:
    dem_cfg = config["datasets"]["dem"]
    dem_paths = discover_paths(repo_root, dem_cfg["local_patterns"])
    features = pd.DataFrame(index=municipios.index)
    for col in ["elevation_mean", "slope_mean", "slope_p90", "local_relief", "wetness_proxy"]:
        features[col] = np.nan
    if not dem_paths:
        source_summary["dem"] = {"status": "missing"}
        warn_and_record(logger, warnings_list, "DEM raster not found; DEM-derived terrain features will be null.")
        return features

    logger.info("Reading %d DEM raster(s)", len(dem_paths))
    dem, transform, _ = read_projected_raster(
        dem_paths,
        config["runtime"]["working_crs"],
        resampling_name=dem_cfg.get("resampling", "bilinear"),
    )
    slope = derive_slope_degrees(dem, transform)
    wetness_cfg = config["features"]["wetness_proxy"]
    wetness = derive_wetness_proxy(
        dem,
        slope,
        transform,
        neighborhood_m=float(wetness_cfg["neighborhood_m"]),
        min_slope_deg=float(wetness_cfg["min_slope_deg"]),
        clip_percentile=float(wetness_cfg["clip_percentile"]),
    )

    dem_rows = zonal_metric(municipios.geometry, dem, transform, stats=["mean", "min", "max"])
    slope_rows = zonal_metric(
        municipios.geometry,
        slope,
        transform,
        stats=["mean"],
        add_stats={"p90": percentile_stat(90, array_for_zonal_stats(slope)[1])},
    )
    wet_rows = zonal_metric(municipios.geometry, wetness, transform, stats=["mean"])

    assign_zonal_columns(features, dem_rows, {"elevation_mean": "mean"})
    dem_min = pd.Series([row.get("min", np.nan) for row in dem_rows], index=municipios.index)
    dem_max = pd.Series([row.get("max", np.nan) for row in dem_rows], index=municipios.index)
    features["local_relief"] = dem_max - dem_min
    assign_zonal_columns(features, slope_rows, {"slope_mean": "mean", "slope_p90": "p90"})
    assign_zonal_columns(features, wet_rows, {"wetness_proxy": "mean"})

    source_summary["dem"] = {
        "status": "local",
        "paths": [str(path.relative_to(repo_root)) for path in dem_paths],
    }
    return features


def compute_stream_features(
    municipios: gpd.GeoDataFrame,
    repo_root: Path,
    config: dict[str, Any],
    logger: logging.Logger,
    warnings_list: list[str],
    source_summary: dict[str, Any],
) -> pd.DataFrame:
    streams_cfg = config["datasets"]["streams"]
    stream_paths = discover_paths(repo_root, streams_cfg["local_patterns"])
    features = pd.DataFrame(index=municipios.index, data={"distance_to_stream_km": np.nan})
    if not stream_paths:
        source_summary["streams"] = {"status": "missing"}
        warn_and_record(logger, warnings_list, "Stream vector not found; distance-to-stream will be null.")
        return features

    streams = read_vector(stream_paths, layer_name=streams_cfg.get("layer_name"))
    if streams.empty:
        source_summary["streams"] = {"status": "empty"}
        warn_and_record(logger, warnings_list, "Stream vector was discovered but contains no features.")
        return features

    if streams.crs is None:
        streams = streams.set_crs("EPSG:4326")
    streams = streams.to_crs(config["runtime"]["working_crs"])
    streams = streams[streams.geometry.notnull()].copy()
    streams = streams[~streams.geometry.is_empty].copy()
    stream_union = unary_union(streams.geometry.tolist())
    centroids = municipios.geometry.centroid
    features["distance_to_stream_km"] = centroids.distance(stream_union) / 1000.0
    source_summary["streams"] = {
        "status": "local",
        "paths": [str(path.relative_to(repo_root)) for path in stream_paths],
    }
    return features


def compute_coastal_features(
    municipios: gpd.GeoDataFrame,
    repo_root: Path,
    config: dict[str, Any],
    logger: logging.Logger,
    warnings_list: list[str],
    source_summary: dict[str, Any],
) -> pd.DataFrame:
    coastal_cfg = config["datasets"]["coastal"]
    features = pd.DataFrame(
        index=municipios.index,
        data={"coastal_inundation_flag": np.nan, "coastal_inundation_depth_mean": np.nan},
    )

    raster_paths = discover_paths(repo_root, coastal_cfg["raster_patterns"])
    vector_paths = discover_paths(repo_root, coastal_cfg["vector_patterns"])

    if raster_paths:
        raster, transform, _ = read_projected_raster(
            raster_paths,
            config["runtime"]["working_crs"],
            resampling_name="bilinear",
        )
        rows = zonal_metric(municipios.geometry, raster, transform, stats=["mean"])
        features["coastal_inundation_depth_mean"] = [row.get("mean", np.nan) for row in rows]
        threshold = float(coastal_cfg["inundation_flag_threshold_m"])
        features["coastal_inundation_flag"] = np.where(
            np.isfinite(features["coastal_inundation_depth_mean"]),
            (features["coastal_inundation_depth_mean"] >= threshold).astype(float),
            np.nan,
        )
        source_summary["coastal"] = {
            "status": "local-raster",
            "paths": [str(path.relative_to(repo_root)) for path in raster_paths],
        }
        return features

    if vector_paths:
        coastal = read_vector(vector_paths, layer_name=coastal_cfg.get("vector_layer_name"))
        if not coastal.empty:
            if coastal.crs is None:
                coastal = coastal.set_crs("EPSG:4326")
            coastal = coastal.to_crs(config["runtime"]["working_crs"])
            coastal = coastal[coastal.geometry.notnull()].copy()
            coastal = coastal[~coastal.geometry.is_empty].copy()
            depth_field = find_first_existing_column(coastal.columns, coastal_cfg["depth_field_candidates"])
            overlay = gpd.overlay(
                municipios[["municipio_id", "geometry"]],
                coastal[[depth_field, "geometry"]] if depth_field else coastal[["geometry"]],
                how="intersection",
                keep_geom_type=False,
            )
            if not overlay.empty:
                overlay["overlap_area_m2"] = overlay.geometry.area
                muni_area = municipios.set_index("municipio_id").geometry.area
                ratio = overlay.groupby("municipio_id")["overlap_area_m2"].sum() / muni_area
                features = features.set_index(municipios["municipio_id"])
                features.loc[ratio.index, "coastal_inundation_flag"] = (
                    ratio >= float(coastal_cfg["overlap_flag_threshold_ratio"])
                ).astype(float)
                if depth_field:
                    overlay["weighted_depth"] = pd.to_numeric(overlay[depth_field], errors="coerce") * overlay["overlap_area_m2"]
                    depth_mean = overlay.groupby("municipio_id")["weighted_depth"].sum() / overlay.groupby("municipio_id")[
                        "overlap_area_m2"
                    ].sum()
                    features.loc[depth_mean.index, "coastal_inundation_depth_mean"] = depth_mean
                features = features.reset_index(drop=True)
            source_summary["coastal"] = {
                "status": "local-vector",
                "paths": [str(path.relative_to(repo_root)) for path in vector_paths],
            }
            return features

    source_summary["coastal"] = {"status": "missing"}
    warn_and_record(logger, warnings_list, "Coastal inundation raster/vector not found; coastal fields will be null.")
    return features


def compute_soil_features(
    municipios: gpd.GeoDataFrame,
    repo_root: Path,
    config: dict[str, Any],
    logger: logging.Logger,
    warnings_list: list[str],
    source_summary: dict[str, Any],
) -> pd.DataFrame:
    soils_cfg = config["datasets"]["soils"]
    features = pd.DataFrame(index=municipios.index, data={"soil_runoff_potential": np.nan})
    raster_paths = discover_paths(repo_root, soils_cfg["raster_patterns"])
    vector_paths = discover_paths(repo_root, soils_cfg["vector_patterns"])

    if raster_paths:
        raster, transform, _ = read_projected_raster(
            raster_paths,
            config["runtime"]["working_crs"],
            resampling_name="nearest",
        )
        rows = zonal_metric(municipios.geometry, raster, transform, stats=["mean"])
        features["soil_runoff_potential"] = [row.get("mean", np.nan) for row in rows]
        source_summary["soils"] = {
            "status": "local-raster",
            "paths": [str(path.relative_to(repo_root)) for path in raster_paths],
        }
        return features

    if vector_paths:
        soils = read_vector(vector_paths, layer_name=soils_cfg.get("vector_layer_name"))
        if not soils.empty:
            if soils.crs is None:
                soils = soils.set_crs("EPSG:4326")
            soils = soils.to_crs(config["runtime"]["working_crs"])
            field = find_first_existing_column(soils.columns, soils_cfg["hydrologic_group_field_candidates"])
            if field is None:
                warn_and_record(
                    logger,
                    warnings_list,
                    "Soil vector found but no hydrologic group field matched configured candidates; soil runoff will be null.",
                )
                source_summary["soils"] = {
                    "status": "missing-field",
                    "paths": [str(path.relative_to(repo_root)) for path in vector_paths],
                }
                return features
            overlay = gpd.overlay(
                municipios[["municipio_id", "geometry"]],
                soils[[field, "geometry"]],
                how="intersection",
                keep_geom_type=False,
            )
            if not overlay.empty:
                overlay["area_m2"] = overlay.geometry.area
                overlay["score"] = overlay[field].map(
                    lambda value: normalize_soil_group(value, soils_cfg["hydrologic_group_scores"])
                )
                valid = overlay[np.isfinite(overlay["score"]) & (overlay["area_m2"] > 0)].copy()
                if not valid.empty:
                    grouped = valid.groupby("municipio_id")
                    area_weighted = grouped.apply(
                        lambda frame: float(np.average(frame["score"], weights=frame["area_m2"]))
                    )
                    features = features.set_index(municipios["municipio_id"])
                    features.loc[area_weighted.index, "soil_runoff_potential"] = area_weighted
                    features = features.reset_index(drop=True)
            source_summary["soils"] = {
                "status": "local-vector",
                "paths": [str(path.relative_to(repo_root)) for path in vector_paths],
                "field": field,
            }
            return features

    source_summary["soils"] = {"status": "missing"}
    warn_and_record(logger, warnings_list, "Soil raster/vector not found; soil runoff potential will be null.")
    return features


def compute_land_cover_features(
    municipios: gpd.GeoDataFrame,
    repo_root: Path,
    config: dict[str, Any],
    logger: logging.Logger,
    warnings_list: list[str],
    source_summary: dict[str, Any],
) -> pd.DataFrame:
    lc_cfg = config["datasets"]["land_cover"]
    lc_paths = discover_paths(repo_root, lc_cfg["local_patterns"])
    features = pd.DataFrame(index=municipios.index, data={"land_cover_runoff_modifier": np.nan})
    if not lc_paths:
        source_summary["land_cover"] = {"status": "missing"}
        warn_and_record(logger, warnings_list, "Land-cover raster not found; runoff modifier will be null.")
        return features

    raster, transform, _ = read_projected_raster(
        lc_paths,
        config["runtime"]["working_crs"],
        resampling_name="nearest",
    )
    mapped = np.full(raster.shape, np.nan, dtype="float32")
    for key, value in lc_cfg["class_score_mapping"].items():
        mapped[np.isclose(raster, float(key))] = float(value)
    rows = zonal_metric(municipios.geometry, mapped, transform, stats=["mean"])
    features["land_cover_runoff_modifier"] = [row.get("mean", np.nan) for row in rows]
    source_summary["land_cover"] = {
        "status": "local",
        "paths": [str(path.relative_to(repo_root)) for path in lc_paths],
    }
    return features


def compute_quality_scores(base: pd.DataFrame, config: dict[str, Any]) -> pd.DataFrame:
    fields = config["quality"]["completeness_fields"]
    availability = base[fields].notna().astype(float)
    base["terrain_data_completeness"] = availability.mean(axis=1) * 100.0

    source_weights = config["quality"]["source_weights"]
    dem_available = base[["elevation_mean", "slope_mean", "slope_p90", "local_relief", "wetness_proxy"]].notna().mean(axis=1)
    streams_available = base[["distance_to_stream_km"]].notna().mean(axis=1)
    coastal_available = base[["coastal_inundation_flag", "coastal_inundation_depth_mean"]].notna().mean(axis=1)
    soils_available = base[["soil_runoff_potential"]].notna().mean(axis=1)
    land_cover_available = base[["land_cover_runoff_modifier"]].notna().mean(axis=1)

    confidence = (
        dem_available * float(source_weights["dem"])
        + streams_available * float(source_weights["streams"])
        + coastal_available * float(source_weights["coastal"])
        + soils_available * float(source_weights["soils"])
        + land_cover_available * float(source_weights["land_cover"])
    ) * 100.0
    base["terrain_confidence_score"] = confidence
    return base


def write_output_readme(
    output_dir: Path,
    config_path: Path,
    warnings_list: list[str],
    output_files: dict[str, str],
) -> None:
    lines = [
        "# Terrain Feature Pack v1 Outputs",
        "",
        "This folder contains local, reproducible outputs from the Puerto Rico terrain feature-engineering stage.",
        "",
        "## Files",
        "",
    ]
    for label, relpath in output_files.items():
        lines.append(f"- `{Path(relpath).name}`: {label}")
    lines.extend(
        [
            "",
            "## Field Dictionary",
            "",
        ]
    )
    for field, description in FIELD_DESCRIPTIONS.items():
        lines.append(f"- `{field}`: {description}")
    lines.extend(
        [
            "",
            "## Config Source",
            "",
            f"- `{config_path}`",
            "",
            "## Warnings Captured During Latest Run",
            "",
        ]
    )
    if warnings_list:
        lines.extend([f"- {message}" for message in warnings_list])
    else:
        lines.append("- No warnings recorded.")
    (output_dir / "README.md").write_text("\n".join(lines) + "\n")


def run_terrain_feature_pack(
    repo_root: Path | None = None,
    config_path: Path | None = None,
    log_level: str = "INFO",
) -> dict[str, Any]:
    repo_root = find_repo_root(repo_root)
    config_path = config_path or (repo_root / "config" / "terrain_spec_v1.yaml")
    config = read_yaml(config_path)

    logging.basicConfig(level=getattr(logging, log_level.upper(), logging.INFO), format="%(asctime)s | %(levelname)s | %(message)s")
    logger = logging.getLogger("terrain-feature-pack")

    warnings_list: list[str] = []
    source_summary: dict[str, Any] = {}

    output_dir = repo_root / config["runtime"]["output_dir"]
    output_dir.mkdir(parents=True, exist_ok=True)

    municipios = load_municipio_boundaries(repo_root, config, logger, warnings_list, source_summary)
    base = municipios[["municipio_id", "municipio_name", "municipio_key"]].copy()

    dem_features = compute_dem_features(municipios, repo_root, config, logger, warnings_list, source_summary)
    stream_features = compute_stream_features(municipios, repo_root, config, logger, warnings_list, source_summary)
    coastal_features = compute_coastal_features(municipios, repo_root, config, logger, warnings_list, source_summary)
    soil_features = compute_soil_features(municipios, repo_root, config, logger, warnings_list, source_summary)
    land_cover_features = compute_land_cover_features(municipios, repo_root, config, logger, warnings_list, source_summary)

    terrain = pd.concat(
        [base, dem_features, stream_features, coastal_features, soil_features, land_cover_features],
        axis=1,
    )
    terrain = compute_quality_scores(terrain, config)
    run_timestamp = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    terrain["config_version"] = str(config["version"])
    terrain["run_timestamp_utc"] = run_timestamp

    csv_path = output_dir / "municipio_terrain_features.csv"
    parquet_path = output_dir / "municipio_terrain_features.parquet"
    geojson_path = output_dir / "municipio_terrain_features.geojson"
    metadata_path = output_dir / "run_metadata.json"

    terrain.to_csv(csv_path, index=False)
    terrain.to_parquet(parquet_path, index=False)

    gdf = municipios.merge(terrain, on=["municipio_id", "municipio_name", "municipio_key"], how="left")
    geojson_frame = gdf.copy()
    geojson_frame["coastal_inundation_flag"] = geojson_frame["coastal_inundation_flag"].astype("float64")
    geojson_frame.to_file(geojson_path, driver="GeoJSON", engine="pyogrio")

    output_files = {
        "attribute table (CSV)": str(csv_path.relative_to(repo_root)),
        "attribute table (Parquet)": str(parquet_path.relative_to(repo_root)),
        "geospatial output (GeoJSON)": str(geojson_path.relative_to(repo_root)),
        "run metadata": str(metadata_path.relative_to(repo_root)),
    }

    metadata = {
        "config_version": config["version"],
        "config_path": str(config_path.relative_to(repo_root)),
        "run_timestamp_utc": run_timestamp,
        "row_count": int(len(terrain)),
        "output_files": output_files,
        "sources": source_summary,
        "warnings": warnings_list,
    }
    metadata_path.write_text(json.dumps(metadata, indent=2) + "\n")
    write_output_readme(output_dir, config_path.relative_to(repo_root), warnings_list, output_files)

    logger.info("Terrain feature pack complete: %s", csv_path.relative_to(repo_root))
    return metadata


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Terrain Feature Pack v1 for Puerto Rico.")
    parser.add_argument("--repo-root", default=None, help="Optional repo root override.")
    parser.add_argument("--config", default=None, help="Optional path to terrain config YAML.")
    parser.add_argument("--log-level", default="INFO", help="Python logging level.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    repo_root = Path(args.repo_root).resolve() if args.repo_root else None
    config_path = Path(args.config).resolve() if args.config else None
    metadata = run_terrain_feature_pack(repo_root=repo_root, config_path=config_path, log_level=args.log_level)
    print(json.dumps(metadata, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
