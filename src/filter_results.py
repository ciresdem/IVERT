
import argparse
from osgeo import gdal
import subprocess




def define_and_parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("results_files_or_folder", type=str, nargs="+", required=True)
    parser.add_argument("-x", "--exclude_mask", dest="exclude_mask", type=str)
    parser.add_argument("-i", "--include_mask", dest="include_mask", type=str)
    parser.add_argument("--replot", type=bool, default=True)
    parser.add_argument("--update_coastline_mask", type=bool, default=True)
    parser.add_argument("--update_icesat2_raster", type=bool, default=True)
    return parser.parse_args()