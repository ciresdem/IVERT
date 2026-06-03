#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
ivert.cli
~~~~~~~~~
Command-line interface for the ICESat-2 Validation of Elevations Reporting Tool (IVERT).
"""

import glob
import logging
import os

# Set NUMEXPR_MAX_THREADS before any import loads NumExpr, to suppress the
# "safe limit" warning on machines with many cores.
if "NUMEXPR_MAX_THREADS" not in os.environ:
    try:
        from utils.parallel_funcs import physical_cpu_count as _physical_cpu_count
    except ImportError:
        from ivert_utils.parallel_funcs import physical_cpu_count as _physical_cpu_count
    os.environ["NUMEXPR_MAX_THREADS"] = str(_physical_cpu_count())

import click

try:
    from ivert_utils.version import __version__ as ivert_version
except ImportError:
    try:
        from utils.version import __version__ as ivert_version
    except ImportError:
        ivert_version = "unknown"


@click.group()
@click.version_option(version=ivert_version, prog_name="ivert")
def ivert_cli():
    """IVERT: ICESat-2 Validation of Elevations Reporting Tool.

    Run 'ivert <command> --help' for detailed help on any command.
    """
    pass


###############################################################
# setup
###############################################################

@ivert_cli.command("setup")
def setup():
    """Configure IVERT settings and local data directories.

    Typically run once before using IVERT on a new machine, or when
    changing data directory paths or credentials.
    """
    raise click.ClickException("'ivert setup' is not yet implemented.")


###############################################################
# download
###############################################################

@ivert_cli.command("download")
@click.argument("bbox_or_files", nargs=-1, required=True)
@click.option(
    "-ds", "--date-start", "date_start",
    default="one year and one week ago",
    show_default=True,
    help=(
        "Start date for the ICESat-2 data search. Accepts any format supported "
        "by Python's dateparser library (e.g., '2023.01.01', '1 year ago')."
    ),
)
@click.option(
    "-de", "--date-end", "date_end",
    default="one week ago",
    show_default=True,
    help=(
        "End date for the ICESat-2 data search. Must be after --date-start. "
        "The default one-week buffer accounts for processing delays in ICESat-2 "
        "derived products (ATL08, ATL24, etc.)."
    ),
)
@click.option(
    "-p", "--projection",
    default="EPSG:4326",
    show_default=True,
    help="Horizontal projection (EPSG code) that the bounding box coordinates are in.",
)
@click.option(
    "--wsen",
    is_flag=True,
    default=False,
    help=(
        "Treat BBOX as W/S/E/N order (lower-left, upper-right). "
        "Default order is W/E/S/N (Xmin/Xmax/Ymin/Ymax)."
    ),
)
@click.option(
    "-r", "--replace",
    is_flag=True,
    default=False,
    help=(
        "Replace any previously downloaded data that overlaps the requested "
        "region in space and time. Default: keep existing data and only fill gaps."
    ),
)
@click.option(
    "-c", "--classes",
    default="1/6/7/9/40/41/42",
    show_default=True,
    help=(
        "ICESat-2 photon classes to download, slash-separated. "
        "1=ground, 2=canopy, 3=canopy-top, 6=land-ice, 7=buildings, "
        "9=inland-water, 40=bathy-floor, 41=bathy-surface, 42=lake-surface."
    ),
)
@click.option(
    "-cl", "--confidence-level", "confidence_level",
    type=click.IntRange(1, 4),
    default=1,
    show_default=True,
    help=(
        "Minimum ATL03 signal confidence level to save (1–4). Photons below this "
        "level are discarded before writing to the database. "
        "1=low (keep all), 2=medium, 3=high, 4=very-high."
    ),
)
@click.option(
    "-bc", "--bathy-confidence", "bathy_confidence",
    type=click.FloatRange(0.0, 1.0),
    default=0.01,
    show_default=True,
    help=(
        "Minimum ATL24 bathymetry confidence to save (0.0–1.0). "
        "Bathy-floor photons (class 40) below this confidence are discarded "
        "before writing to the database."
    ),
)
def download(bbox_or_files, date_start, date_end, projection, wsen, replace, classes,
             confidence_level, bathy_confidence):
    """Download ICESat-2 photon data for a region of interest.

    BBOX_OR_FILES: A 4-value bounding box in W/E/S/N order (slash-separated,
    e.g., -74.0/-73.0/40.5/41.0), or one or more DEM files whose combined
    extent defines the download region. Use --wsen to switch to W/S/E/N order.

    Example: ivert download -- -74.0/-73.0/40.5/41.0
    """
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    try:
        from ivert import icesat2_database_v2 as is2db_mod
        from ivert.utils import dem_geom
    except ImportError:
        import icesat2_database_v2 as is2db_mod
        from utils import dem_geom

    # --- Parse bbox or files ---
    # Flatten slash-separated tokens into a flat list.
    values = []
    for token in bbox_or_files:
        values.extend(token.split("/"))

    wgs84_bbox = None  # (xmin, xmax, ymin, ymax)

    if len(values) == 4:
        try:
            nums = [float(v) for v in values]
            if wsen:
                # W/S/E/N → reorder to xmin, xmax, ymin, ymax
                xmin, ymin, xmax, ymax = nums
            else:
                xmin, xmax, ymin, ymax = nums
            if projection.upper() in ("EPSG:4326", "4326"):
                wgs84_bbox = (xmin, xmax, ymin, ymax)
            else:
                wgs84_bbox = dem_geom.get_wgs84_bounding_box(
                    (xmin, xmax, ymin, ymax), dem_horz_reference_frame=projection)
        except ValueError:
            pass  # not numeric — fall through to file path handling

    if wgs84_bbox is None:
        # Treat tokens as file paths (glob-expand them).
        expanded = []
        for token in bbox_or_files:
            matches = glob.glob(token)
            expanded.extend(matches if matches else [token])

        missing = [f for f in expanded if not os.path.exists(f)]
        if missing:
            raise click.ClickException(
                "Files not found (and input is not a valid 4-value bbox): "
                + ", ".join(missing))

        xmins, xmaxs, ymins, ymaxs = [], [], [], []
        for fn in expanded:
            bb = dem_geom.get_wgs84_bounding_box(fn)
            xmins.append(bb[0]); xmaxs.append(bb[1])
            ymins.append(bb[2]); ymaxs.append(bb[3])
        wgs84_bbox = (min(xmins), max(xmaxs), min(ymins), max(ymaxs))

    # --- Parse dates and classes ---
    db = is2db_mod.IS2Database()

    try:
        tmin = db.convert_date_to_yyyymmdd(date_start)
        tmax = db.convert_date_to_yyyymmdd(date_end)
    except Exception as exc:
        raise click.ClickException(f"Could not parse date: {exc}") from exc

    if tmin >= tmax:
        raise click.ClickException(
            f"--date-start ({tmin}) must be before --date-end ({tmax}).")

    class_list = tuple(int(c) for c in classes.split("/"))

    full_bbox = (wgs84_bbox[0], wgs84_bbox[1], wgs84_bbox[2], wgs84_bbox[3], tmin, tmax)

    if not is2db_mod.IS2Database.bbox_valid(full_bbox):
        raise click.ClickException(
            f"Invalid bounding box: xmin < xmax, ymin < ymax required. Got {full_bbox[:4]}.")

    db.download_new_granules(full_bbox, classes_to_keep=class_list,
                             min_confidence_level=confidence_level,
                             min_bathy_confidence=bathy_confidence)


###############################################################
# validate
###############################################################

def _run_validate(files_or_directory, vdatum, region_name, include_photons,
                  measure_coverage, band_num, outlier_sd_threshold, buildings,
                  confidence_level, bathy_confidence, verbose=True):
    """Branch to validate_dem or validate_list_of_dems based on the number of input files."""
    try:
        from ivert import validate_dem as vd_module
        from ivert import validate_dem_collection as vdc_module
    except ImportError:
        import validate_dem as vd_module
        import validate_dem_collection as vdc_module

    # Expand any glob patterns the shell left unexpanded (e.g., quoted patterns).
    expanded = []
    for f in files_or_directory:
        matches = glob.glob(f)
        expanded.extend(matches if matches else [f])

    if not expanded:
        raise click.ClickException("No input files or directory found.")

    classes = [1, 6, 40]
    if buildings:
        classes = sorted(classes + [7])

    if len(expanded) == 1 and os.path.isfile(expanded[0]):
        kwargs = dict(
            dem_name=expanded[0],
            classes=classes,
            band_num=band_num,
            outliers_sd_threshold=outlier_sd_threshold,
            include_photon_level_validation=include_photons,
            location_name=region_name,
            measure_coverage=measure_coverage,
            min_confidence_level=confidence_level,
            min_bathy_confidence=bathy_confidence,
            verbose=verbose,
        )
        if vdatum != "NONE_PROVIDED":
            kwargs["dem_vertical_datum"] = vdatum
        vd_module.validate_dem(**kwargs)
    else:
        dem_input = expanded[0] if len(expanded) == 1 else expanded
        kwargs = dict(
            dem_list_or_dir=dem_input,
            classes=classes,
            band_num=band_num,
            place_name=region_name,
            include_photon_validation=include_photons,
            measure_coverage=measure_coverage,
            outliers_sd_threshold=outlier_sd_threshold,
            min_confidence_level=confidence_level,
            min_bathy_confidence=bathy_confidence,
            verbose=verbose,
        )
        if vdatum != "NONE_PROVIDED":
            kwargs["input_vdatum"] = vdatum
        vdc_module.validate_list_of_dems(**kwargs)


@ivert_cli.command("validate")
@click.argument("files_or_directory", nargs=-1, required=True)
@click.option(
    "-V", "--vdatum",
    default="NONE_PROVIDED",
    show_default=True,
    help=(
        "Vertical datum of the input DEM(s), as an EPSG number or short name "
        "(e.g., 'EPSG:5703', 'navd88'). If omitted, IVERT reads it from the DEM "
        "metadata header. Run 'vdatums --list-epsg' for available options."
    ),
)
@click.option(
    "-n", "--name", "--region-name", "region_name",
    default=None,
    show_default=False,
    help=(
        "Name of the region being validated. For a single DEM, appears on its "
        "plot (defaults to the DEM filename). For a collection, appears only on "
        "the collection-level summary plot; individual DEM plots always use "
        "their filenames."
    ),
)
@click.option(
    "-ph", "--include-photons", "include_photons",
    is_flag=True,
    default=False,
    help=(
        "Return a point database of individual ICESat-2 photons used to validate "
        "each DEM, in addition to the normal .h5 and .tif results outputs."
    ),
)
@click.option(
    "-mc", "--measure-coverage", "measure_coverage",
    is_flag=True,
    default=False,
    help=(
        "Measure relative photon coverage per grid cell (fraction of 15x15 "
        "sub-regions containing photons). Useful for post-processing "
        "coarse-resolution DEMs where sampling bias may matter."
    ),
)
@click.option(
    "-bn", "--band-num", "band_num",
    type=int,
    default=1,
    show_default=True,
    help="Raster band to validate in each DEM (1-indexed). Other bands are ignored.",
)
@click.option(
    "-sd", "--outlier-sd", "outlier_sd_threshold",
    type=float,
    default=2.5,
    show_default=True,
    help=(
        "Standard-deviation threshold for outlier filtering. Errors more than "
        "this many SDs from the mean are treated as noise and removed. "
        "Use -1 to disable outlier filtering."
    ),
)
@click.option(
    "-b", "--buildings",
    is_flag=True,
    default=False,
    help="Include building-class photons in validation.",
)
@click.option(
    "-cl", "--confidence-level", "confidence_level",
    type=click.IntRange(1, 4),
    default=1,
    show_default=True,
    help=(
        "Minimum ATL03 signal confidence level to use (1–4). Photons below this "
        "level are excluded from validation. "
        "1=low (keep all), 2=medium, 3=high, 4=very-high."
    ),
)
@click.option(
    "-bc", "--bathy-confidence", "bathy_confidence",
    type=click.FloatRange(0.0, 1.0),
    default=0.75,
    show_default=True,
    help=(
        "Minimum ATL24 bathymetry confidence to use (0.0–1.0). "
        "Bathy-floor photons (class 40) below this confidence are excluded "
        "from validation."
    ),
)
@click.option(
    "-v", "--verbose",
    is_flag=True,
    default=False,
    help="Print progress messages during validation.",
)
def validate(files_or_directory, vdatum, region_name, include_photons,
             measure_coverage, band_num, outlier_sd_threshold, buildings,
             confidence_level, bathy_confidence, verbose):
    """Validate one or more DEMs against ICESat-2 photon data.

    FILES_OR_DIRECTORY can be one or more GeoTIFF paths, a directory
    (all *.tif files are used), or a glob pattern (e.g., 'data/ncei*.tif').

    Example: ivert validate mydem.tif -V EPSG:5703 -n "Oregon Coast"
    """
    _run_validate(files_or_directory, vdatum, region_name, include_photons,
                  measure_coverage, band_num, outlier_sd_threshold, buildings,
                  confidence_level, bathy_confidence, verbose)


###############################################################
# upgrade
###############################################################

@ivert_cli.command("upgrade")
def upgrade():
    """Upgrade IVERT to the latest available version."""
    try:
        from ivert import client_upgrade
    except ImportError:
        import client_upgrade
    client_upgrade.upgrade()


if __name__ == "__main__":
    ivert_cli()
