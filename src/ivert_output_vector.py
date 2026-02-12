# IVERT results files are cranked out in HDF format on a cell-by-cell basis.
# This script converts them to vector files (xyz, shp, or gpkg) for easier viewing in a GIS.

import argparse
import geopandas
import numpy
import os
import pandas
import sys
import xarray


def gather_input_files(files_or_folder: list,
                       format: str='hdf') -> list:
    """Return a list of HDF files that match the existing search query and exist on disk."""
    hdf_list = []

    for pathname in files_or_folder:
        if not os.path.exists(pathname):
            continue

        if os.path.isdir(pathname):
            # If it's a directory, find all the files with that extension in that directory, add them to the path.
            fnames = [os.path.join(pathname, fn) for fn in os.listdir(pathname) if os.path.splitext(fn)[1].lstrip(".").lower() == format]
            hdf_list.extend(fnames)

        elif os.path.splitext(pathname)[1].lstrip(".").lower() == format:
            hdf_list.append(pathname)

    return hdf_list


def convert_netcdf_granules(filenames, output_format, output_dir, overwrite: bool = False):
    """Convert a list of existing NetCDF ICESat-2 IVERT granules into point vector files."""
    for fname in filenames:
        print()
        print(os.path.basename(fname))
        if output_dir is None:
            fname_out = os.path.splitext(fname)[0] + "." + output_format
        else:
            fname_out = os.path.join(output_dir, os.path.splitext(os.path.basename(fname))[0] + "." + output_format)

        if os.path.exists(fname_out):
            if overwrite:
                os.remove(fname_out)
            else:
                print("Skipping existing", os.path.basename(fname_out))

        df = xarray.open_dataset(fname).to_dataframe()
        print(df)
        classes_list = sorted(df["class_code"].unique())
        print(classes_list)
        for ccode in classes_list:
            print(f"{ccode}: {numpy.count_nonzero(df["class_code"] == ccode)} photons.")

        continue


def convert_hdf_result_files(filenames, output_format, output_dir, overwrite: bool = False):
    raise NotImplementedError("Function not yet implemented.")


def define_and_parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser("Convert IVERT HDF results files to geolocated vector formats for viewing.")
    parser.add_argument("files_or_folder", nargs="+",
                        help="Name of one or more file(s) to parse."
                             " If a directory, look for all .hdf files in that directory (non-recursive).")
    parser.add_argument("-if", "--input_format", dest="input_format", default="HDF",
                        help="Input format of files to search for. Right now enables 'hdf' (for IVERT output results files), or "
                             "'cdf'/'netcdf' for IVERT ICESat-2 database granules. Default: hdf")
    parser.add_argument("-of", "--output_format", dest="output_format", default="GPKG",
                        help="Vector format for outputs. Can be 'shp', 'gpkg', or 'xyz'. Case-insensitive. Default: gpkg")
    parser.add_argument("--output_dir", default=None,
                        help="Directory to put the output files. If not specified, they'll go in the same directory alongside the input files.")
    parser.add_argument("-o", "--overwrite", dest="overwrite", action="store_true", default=False,
                        help="Whether to overwrite vector files that already exist on disk. Default: False")

    return parser.parse_args()

if __name__ == "__main__":
    args = define_and_parse_args()

    input_format = args.input_format.strip().lstrip('.').lower()
    if input_format == "netcdf":
        input_format = "cdf"

    assert input_format in ['cdf', 'hdf']

    output_format = args.output_format.strip().lstrip('.').lower()
    assert output_format in ['shp', 'gpkg', 'xyz']

    files_to_process = gather_input_files(args.files_or_folder, format=input_format)

    if (args.output_dir is not None) and (not os.path.exists(args.output_dir)):
        raise FileNotFoundError(f"Output directory {args.output_dir} does not exist on disk.")

    if len(files_to_process) == 0:
        print(f"No '.{input_format}' files found in the location(s) given.")
        sys.exit(0)

    if input_format == "cdf":
        convert_netcdf_granules(files_to_process, output_format, args.output_dir, args.overwrite)
    elif input_format == "hdf":
        convert_hdf_result_files(files_to_process, output_format, args.output_dir, args.overwrite)