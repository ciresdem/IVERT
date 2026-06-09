# ivert database

Manage the local IVERT ICESat-2 photon database. IVERT stores downloaded photon data as NetCDF granule files (`.nc`) indexed by a GeoPackage (`.gpkg`) for fast spatial lookup. The database location is set by `icesat2_granules_directory` in your config (see [ivert options](options.md)).

---

## Subcommands

- [`ivert database download`](#download) — download new ICESat-2 data
- [`ivert database list`](#list) — list what's already downloaded
- [`ivert database size`](#size) — check disk usage
- [`ivert database rebuild`](#rebuild) — rebuild the index from existing files
- [`ivert database delete`](#delete) — remove data from disk

---

## download

Download ICESat-2 photon data for a geographic region and time range.

```
ivert database download BBOX_OR_FILES [OPTIONS]
```

### Specifying the area

Pass either a bounding box or one or more DEM file paths (IVERT reads their extents):

```
# Bounding box: W/E/S/N (default order)
ivert database download -74.0/-73.0/40.5/41.0

# Use --wsen if your numbers are in W/S/E/N order
ivert database download -74.0/40.5/-73.0/41.0 --wsen

# Use DEM extents
ivert database download mydem.tif
ivert database download /data/dems/*.tif
```

Bounding box values are in the projection given by `-p` (default EPSG:4326, i.e. decimal degrees longitude/latitude).

### Date range options

| Flag | Default | Description |
|------|---------|-------------|
| `-ds, --date-start TEXT` | one year and one week ago | Start of the search window. Accepts dateparser formats: `2023-01-01`, `"1 year ago"`, `20230101` |
| `-de, --date-end TEXT` | one week ago | End of the search window |

> **Note:** ATL24 (bathymetry) data is only available through approximately November 2024. For bathymetric validation, use a date range ending at or before `2024-11-07`.

### Photon class options

| Flag | Default | Description |
|------|---------|-------------|
| `-c, --classes TEXT` | `1/6/7/9/40/41/42` | Slash-separated list of photon class codes to download |

Photon class codes:

| Code | Class |
|------|-------|
| `-1` | Unclassified |
| `0` | Noise |
| `1` | Ground |
| `2` | Canopy |
| `3` | Canopy top |
| `6` | Land ice |
| `7` | Buildings |
| `9` | Inland water |
| `40` | Bathymetry floor |
| `41` | Bathymetry / nearshore water surface |
| `42` | Lake surface |

### Quality filtering options

| Flag | Default | Description |
|------|---------|-------------|
| `-cl, --confidence-level N` | `1` | Minimum ATL03 signal confidence (1=keep all, 2=medium, 3=high, 4=very-high) |
| `-bc, --bathy-confidence F` | `0.01` | Minimum ATL24 bathymetry confidence for bathy-floor photons (0.0–1.0) |

### Other options

| Flag | Description |
|------|-------------|
| `-p, --projection TEXT` | Horizontal CRS of the bounding box (default: `EPSG:4326`) |
| `-r, --replace` | Replace any previously downloaded data overlapping this region |
| `-f, --force` | Skip the interactive prompt when the date range extends beyond the ATL24 data cutoff |

---

## list

Show granules currently in the database.

```
ivert database list
ivert database list --all
ivert database list --boxes
```

| Flag | Description |
|------|-------------|
| `-a, --all` | Show all fields instead of the default summary columns |
| `-bo, --boxes` | Print the unique bounding boxes used when building the database |

---

## size

Report the number of files and disk space used by each part of the database.

```
ivert database size
```

Output shows: GeoPackage index (`.gpkg`), blosc index, and raw granule files (`.nc`).

---

## rebuild

Reconstruct the database index by scanning existing `.nc` granule files on disk.

```
ivert database rebuild
```

Use this if the index (`.gpkg` / blosc files) becomes corrupted or out of sync with the granule files — for example after an interrupted download.

---

## delete

Delete the database index files.

```
ivert database delete
ivert database delete --all
```

| Flag | Description |
|------|-------------|
| `-a, --all` | Also delete all `.nc` granule data files (full removal) |
| `-y, --yes` | Skip the confirmation prompt |

Without `--all`, only the index (`.gpkg` and blosc files) is deleted; the granule `.nc` files remain on disk and can be re-indexed with `ivert database rebuild`.

---

## Examples

**Download data for a coastal region (last year):**
```
ivert database download -74.0/-73.0/40.5/41.0
```

**Download for a specific date range:**
```
ivert database download -74.0/-73.0/40.5/41.0 -ds 2023-01-01 -de 2024-01-01
```

**Download only ground photons at high confidence:**
```
ivert database download -74.0/-73.0/40.5/41.0 -c 1 -cl 3
```

**Match the extent of a DEM:**
```
ivert database download mydem.tif
```

**Check what's been downloaded:**
```
ivert database list
ivert database size
```
