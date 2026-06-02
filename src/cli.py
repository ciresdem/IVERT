#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
ivert.cli
~~~~~~~~~
Command-line interface for the ICESat-2 Validation of Elevations Reporting Tool (IVERT).
"""

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
    "-DS", "--date-start", "date_start",
    default="one year and one week ago",
    show_default=True,
    help=(
        "Start date for the ICESat-2 data search. Accepts any format supported "
        "by Python's dateparser library (e.g., '2023.01.01', '1 year ago')."
    ),
)
@click.option(
    "-DE", "--date-end", "date_end",
    default="one week ago",
    show_default=True,
    help=(
        "End date for the ICESat-2 data search. Must be after --date-start. "
        "The default one-week buffer accounts for processing delays in ICESat-2 "
        "derived products (ATL08, ATL24, etc.)."
    ),
)
@click.option(
    "-P", "--projection",
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
    "-R", "--replace",
    is_flag=True,
    default=False,
    help=(
        "Replace any previously downloaded data that overlaps the requested "
        "region in space and time. Default: keep existing data and only fill gaps."
    ),
)
@click.option(
    "-C", "--classes",
    default="1/6/7/9/40/41/42",
    show_default=True,
    help=(
        "ICESat-2 photon classes to download, slash-separated. "
        "1=ground, 2=canopy, 3=canopy-top, 6=land-ice, 7=buildings, "
        "9=inland-water, 40=bathy-floor, 41=bathy-surface, 42=lake-surface."
    ),
)
def download(bbox_or_files, date_start, date_end, projection, wsen, replace, classes):
    """Download ICESat-2 photon data for a region of interest.

    BBOX_OR_FILES: A 4-value bounding box in W/E/S/N order (slash-separated,
    e.g., -74.0/-73.0/40.5/41.0), or one or more DEM files whose combined
    extent defines the download region. Use --wsen to switch to W/S/E/N order.

    Example: ivert download -- -74.0/-73.0/40.5/41.0
    """
    raise click.ClickException("'ivert download' is not yet implemented.")


###############################################################
# validate
###############################################################

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
    default="DEMs",
    show_default=True,
    help=(
        "Name of the region being validated. Appears in the validation summary "
        "plot when validating more than one file."
    ),
)
@click.option(
    "-ph", "--include-photons", "include_photons",
    is_flag=True,
    default=False,
    help=(
        "Return a point database of individual ICESat-2 photons used to validate "
        "each DEM, in addition to the normal .h5 and .tif outputs."
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
    "-B", "--buildings",
    is_flag=True,
    default=False,
    help="Include building-class photons in validation.",
)
def validate(files_or_directory, vdatum, region_name, include_photons,
             measure_coverage, band_num, outlier_sd_threshold, buildings):
    """Validate one or more DEMs against ICESat-2 photon data.

    FILES_OR_DIRECTORY can be one or more GeoTIFF paths, a directory
    (all *.tif files are used), or a glob pattern (e.g., 'data/ncei*.tif').

    Example: ivert validate mydem.tif -V EPSG:5703 -n "Oregon Coast"
    """
    raise click.ClickException("'ivert validate' is not yet implemented.")


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
