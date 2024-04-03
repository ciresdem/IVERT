"""generate_granule_lines.py -- A script to generate a spatial file of lines from a section of the ICESat-2 database.
This allows easy visualization of the orbit tracks in the ICESat-2 database.

NOT COMPLETELY IMPLEMENTED."""

import argparse
import geopandas
import shapely.geometry

import icesat2_photon_database


def get_polygon_from_dem_files(dem_filenames: list,
                               convert_to_latlon: bool = True) -> shapely.geometry.Polygon:
    """Generate a bounding box from a list of DEM filenames.

    Bounding boxes of inputs DEMs will be combined.

    Args:
        dem_filenames (list): A list of DEM filenames.
        convert_to_latlon (bool, optional): Whether to convert the bounding box to WGS84 (EPSG:4326) lat/lon. Defaults to True.

    Returns:
        shapely.geometry.Polygon: A bounding box of the DEMs.
    """
    pass


def generate_granule_lines_from_icesat2_database(polygon, output_filename, tolerance_deg=0.00001) -> geopandas.GeoDataFrame:
    """Generate a spatial file of orbit track lines from a section of the ICESat-2 database.

    Args:
        polygon (shapely.geometry.Polygon): The bounding polygon of the ICESat-2 database to use. Must be in WGS84 (EPSG:4326) lat/lon.
        output_filename (str): The name of the output spatial file. Currently supports .gpkg or .shp.
        tolerance_deg (float, optional): When simplifying the lines for export, tolerance in degrees for the output lines from the higher-density "true" line.
                                         Setting to None disables simplification and returns lines containing all points (NOT RECOMMENDED).
                                         Defaults to 0.00001 (approximately 1.1 m at the equator, smaller at higher latitudes).

    Returns:
        geopandas.GeoDataFrame: A GeoDataFrame containing simplified granule linestrings from the ICESat-2 database."""
    pass


def import_and_parse_args():
    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument("dem_filenames", type=str, nargs='*', default=None,
                        help="One or more input DEM(s) to define the bounding box. If no filenames are provided, "
                             "the --bbox argument must be used to define an area.")

    parser.add_argument("-b", "--bbox", "--bounding_box", type=str, dest="bbox",
                        help="The bounding box of the ICESat-2 database to use, in xmin/xmax/ymin/ymax format. "
                             "Ignored if a DEM is provided as input.")

    parser.add_argument("-o", "--output", type=str, dest="output", default="photon_lines.gpkg",
                        help="The name of the output spatial file. Currently supports .gpkg or .shp. "
                             "(Default: photon_lines.gpkg)")

    return parser.parse_args()


if __name__ == "__main__":

    args = import_and_parse_args()
