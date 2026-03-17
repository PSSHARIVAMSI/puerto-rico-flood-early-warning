# Terrain Staging Area

This folder is reserved for local terrain inputs used by Terrain Feature Pack v1.

## Why This Folder Is Ignored

Terrain rasters and hydrography layers can be large and are often downloaded locally for notebook processing. The local `.gitignore` in this folder keeps those raw inputs out of Git while allowing this README to stay versioned.

## Expected Layout

- `data/staging/terrain/boundaries/`
- `data/staging/terrain/dem/`
- `data/staging/terrain/streams/`
- `data/staging/terrain/coastal/`
- `data/staging/terrain/soils/`
- `data/staging/terrain/land_cover/`

## Automation Status

Automated in v1:

- Puerto Rico municipio boundaries via Census TIGER fallback download

Manual/local download expected in v1:

- DEM GeoTIFF from USGS 3DEP or NOAA DEM
- NOAA Sea Level Rise Viewer inundation raster/polygon exports
- gSSURGO / SSURGO soil layers
- NOAA C-CAP land-cover raster
- Optional stream network if not already curated by the team

## Expected Filenames / Globs

The stage discovers inputs through glob patterns defined in `config/terrain_spec_v1.yaml`.

Examples:

- `data/staging/terrain/dem/*.tif`
- `data/staging/terrain/streams/*.gpkg`
- `data/staging/terrain/coastal/*.tif`
- `data/staging/terrain/coastal/*.geojson`
- `data/staging/terrain/soils/*.gpkg`
- `data/staging/terrain/land_cover/*.tif`
