# -*- coding: utf-8 -*-

"""validate_dem_collection.py
Code for validating and summarizing an entire list or directory of DEMs.
"""

import ast
import os
import pandas
import numpy
# from osgeo import gdal, osr
import typing
import argparse
import re

####################################
# # Include the base /src/ directory of thie project, to add all the other modules.
# import import_parent_dir; import_parent_dir.import_src_dir_via_pythonpath()
####################################
import validate_dem as validate_dem
# import nsidc_download as nsidc_download
import icesat2_photon_database as icesat2_photon_database
# import classify_icesat2_photons as classify_icesat2_photons
import plot_validation_results as plot_validation_results
# import coastline_mask as coastline_mask


def write_summary_csv_file(total_results_df_or_file: typing.Union[pandas.DataFrame, str],
                           csv_name: str,
                           verbose: bool = True) -> pandas.DataFrame:
    """Write a summary csv of all the results in a collection, after they've been run."""
    if type(total_results_df_or_file) is str:
        total_df = pandas.read_hdf(total_results_df_or_file)
    else:
        assert isinstance(total_results_df_or_file, pandas.DataFrame)
        total_df = total_results_df_or_file

    if 'filename' not in total_df.columns:
        raise ValueError("total_df must have a 'filename' column.")

    unique_files = total_df['filename'].unique()
    N = len(unique_files)
    means = numpy.empty((N,), dtype=float)
    stds = numpy.empty((N,), dtype=float)
    rmses = numpy.empty((N,), dtype=float)
    n_cells = numpy.empty((N,), dtype=int)
    photons_per_cell = numpy.empty((N,), dtype=float)
    canopy_mean = numpy.empty((N,), dtype=float)
    canopy_mean_gt0 = numpy.empty((N,), dtype=float)

    for i, fname in enumerate(unique_files):
        temp_df = total_df[total_df['filename'] == fname]
        means[i] = temp_df['diff_mean'].mean()
        stds[i] = temp_df['diff_mean'].std()
        rmses[i] = (sum((temp_df['diff_mean'] ** 2)) / (len(temp_df) - 1)) ** 0.5
        n_cells[i] = len(temp_df)
        photons_per_cell[i] = temp_df['numphotons_intd'].mean()
        canopy_mean[i] = temp_df['canopy_fraction'].mean()
        canopy_mean_gt0[i] = temp_df[temp_df['canopy_fraction'] > 0]['canopy_fraction'].mean()

    output_df = pandas.DataFrame(data={'filename': unique_files,
                                       "rmse": rmses,
                                       "mean_bias": means,
                                       "stddev_from_mean": stds,
                                       "n_cells_validated": n_cells,
                                       "mean_photons_per_cell": photons_per_cell,
                                       "canopy_mean": canopy_mean,
                                       "canopy_mean_gt0": canopy_mean_gt0
                                       }
                                 )

    output_df.to_csv(csv_name, index=False)
    if verbose:
        print(csv_name, "written.")

    return output_df

def validate_list_of_dems(dem_list_or_dir,
                          output_dir=None,
                          fname_filter=r"\.tif\Z",
                          fname_omit=None,
                          ivert_job_name=None,
                          band_num: int=1,
                          input_vdatum="wgs84",
                          output_vdatum="wgs84",
                          overwrite=False,
                          place_name=None,
                          mask_buildings=True,
                          use_urban_mask=False,
                          create_individual_results = False,
                          delete_datafiles=False,
                          include_photon_validation=True,
                          write_result_tifs=False,
                          omit_bad_granules = True,
                          write_summary_csv = True,
                          measure_coverage = False,
                          outliers_sd_threshold=2.5,
                          verbose=True):
    """Take a list of DEMs, presumably in a single area, and output validation files for those DEMs.

    DEMs should encompass a contiguous area so as to use the same set of ICESat-2 granules for
    validation."""
    # TODO: If ivert_job_name is defined, then use it to update the database with file statuses as the job goes on.

    if output_dir is None:
        if os.path.isdir(dem_list_or_dir):
            stats_and_plots_dir = dem_list_or_dir
        elif type(dem_list_or_dir) == str:
            stats_and_plots_dir = os.path.dirname(dem_list_or_dir)
        else:
            dem_list_fitting_filter = [fn for fn in dem_list_or_dir if (((fname_filter is None) or (re.search(fname_filter, os.path.split(fn)[1]) != None)) \
                                                                        and \
                                                                        ((fname_omit is None) or (re.search(fname_omit, os.path.split(fn)[1]) == None)))]
            stats_and_plots_dir = os.path.dirname(dem_list_fitting_filter[0])
    else:
        if os.path.isdir(output_dir):
            stats_and_plots_dir = output_dir
        else:
            # If the output dir appears to be a relative path, then join it with the input dir.
            if type(dem_list_or_dir) == str:
                stats_and_plots_dir = os.path.join(os.path.dirname(dem_list_or_dir), output_dir)
            else:
                dem_list_fitting_filter = [fn for fn in dem_list_or_dir if (((fname_filter is None) or (re.search(fname_filter, os.path.split(fn)[1]) != None)) \
                                                                            and \
                                                                            ((fname_omit is None) or (re.search(fname_omit, os.path.split(fn)[1]) == None)))]
                stats_and_plots_dir = os.path.join(os.path.dirname(dem_list_fitting_filter[0]), output_dir)

    # If a place name wasn't provided, just use "summary_results"
    if place_name is None:
        stats_and_plots_base = "summary_results"
    else:
        stats_and_plots_base = place_name.replace(" ", "_") + "_results"

    statsfile_name = os.path.join(stats_and_plots_dir, stats_and_plots_base + ".txt")
    plot_file_name = os.path.join(stats_and_plots_dir, stats_and_plots_base + ".png")
    results_h5 = os.path.join(stats_and_plots_dir, stats_and_plots_base + ".h5")

    # If the .h5 results file already exists but not the other files, just
    # create them and exit.
    if (not overwrite) and (results_h5 is not None) and os.path.exists(results_h5):
        results_df = None

        if not os.path.exists(statsfile_name):
            results_df = pandas.read_hdf(results_h5)
            if verbose:
                print(results_df, "read.")
            validate_dem.write_summary_stats_file(results_df,
                                                  statsfile_name,
                                                  verbose=verbose)

        if not os.path.exists(plot_file_name):
            if results_df is None:
                results_df = pandas.read_hdf(results_h5)
                if verbose:
                    print(results_df, "read.")
            plot_validation_results.plot_histogram_and_error_stats_4_panels(results_df,
                                                                            plot_file_name,
                                                                            place_name=place_name,
                                                                            verbose=verbose)
        if (results_df is None) and verbose:
            print("Files '" + results_h5 + "',",
                  "'" + statsfile_name + "', and '",
                  plot_file_name + "' are all already written. There's nothing left to do here.\n",
                  "To recompute them, run with --overwrite enabled, or delete output files as needed and re-run to create them again.\n",
                  "Exiting.")
        return

    path = dem_list_or_dir
    # If we have a one-item list here, get the item in that list.
    if type(path) in (list, tuple) and len(path) == 1:
        path = path[0]

    if (type(path) in (list, tuple)) and (len(path) > 1):
        dem_list = path
    elif os.path.isdir(path):
        dem_list = sorted([os.path.join(path, fname) for fname in os.listdir(path)])
    else:
        assert os.path.exists(path)
        dem_list = [path]

    # Filter for needed strings in filenames, such as "_wgs84.tif"
    if fname_filter is not None:
        # Include only filenames that MATCH the match string.
        dem_list = [fn for fn in dem_list if (re.search(fname_filter, fn) is not None)]

    # Filter out unwanted filename strings.
    if fname_omit is not None:
        # Only include filenames that DO NOT MATCH the omission string.
        dem_list = [fn for fn in dem_list if (re.search(fname_omit, fn) is None)]

    # if use_icesat2_photon_database:
    # Generate a single photon database object and pass it repeatedly to all the objects.
    # This saves us a lot of re-reading the geodataframe repeatedly.
    photon_db_obj = icesat2_photon_database.ICESat2_Database()

    files_to_export = []
    list_of_results_dfs = []

    # For each DEM, validate it.
    for i, dem_path in enumerate(dem_list):
        if verbose:
            print("\n=======", os.path.split(dem_path)[1], "(" + str(i + 1), "of", str(len(dem_list)) + ")", "=======")

        if output_dir is None:
            this_output_dir = os.path.split(dem_path)[0]
        elif os.path.isdir(output_dir):
            this_output_dir = output_dir
        else:
            # If it's a relative dir, append it to where the dems are.
            this_output_dir = os.path.join(os.path.dirname(dem_list[0]), output_dir)
            if not os.path.exists(this_output_dir):
                os.mkdir(this_output_dir)

        results_h5_file = os.path.join(this_output_dir, os.path.splitext(os.path.split(dem_path)[1])[0] + "_results.h5")

        # Do the validation.
        # Note: We automatically skip the icesat-2 download here because we already downloaded it above for the whole directory.
        retfiles = validate_dem.validate_dem_parallel(dem_path,
                                                      output_dir,
                                                      band_num=band_num,
                                                      icesat2_photon_database_obj=photon_db_obj,
                                                      ivert_job_name=ivert_job_name,
                                                      dem_vertical_datum=input_vdatum,
                                                      output_vertical_datum=output_vdatum,
                                                      interim_data_dir=this_output_dir,
                                                      overwrite=overwrite,
                                                      delete_datafiles=delete_datafiles,
                                                      write_result_tifs=write_result_tifs,
                                                      mask_out_buildings=mask_buildings,
                                                      mask_out_urban=use_urban_mask,
                                                      write_summary_stats=create_individual_results,
                                                      include_photon_level_validation=include_photon_validation,
                                                      plot_results=create_individual_results,
                                                      outliers_sd_threshold=outliers_sd_threshold,
                                                      mark_empty_results=True,
                                                      omit_bad_granules=omit_bad_granules,
                                                      measure_coverage=measure_coverage,
                                                      quiet=not verbose)

        files_to_export.extend(retfiles)

        if os.path.exists(results_h5_file):
            # Append the filename as a column
            list_of_results_dfs.append(results_h5_file)

    # An extra newline is appreciated here just for readability's sake.
    print()

    if len(list_of_results_dfs) == 0:
        if verbose:
            print("No results dataframes generated. Aborting.")
        return

    # Generate the overall summary stats file.
    total_results_df = plot_validation_results.get_data_from_h5_or_list(list_of_results_dfs,
                                                                        orig_filenames=dem_list,
                                                                        include_filenames=True,
                                                                        verbose=verbose)

    if write_summary_csv:
        summary_csv_name = os.path.join(stats_and_plots_dir, stats_and_plots_base + ".csv")
        write_summary_csv_file(total_results_df, summary_csv_name,
                               verbose=verbose)

        files_to_export.append(summary_csv_name)

    # Output the statistics summary file.
    validate_dem.write_summary_stats_file(total_results_df,
                                          statsfile_name,
                                          verbose=verbose)

    files_to_export.append(statsfile_name)

    # Output the validation results plot.
    plot_validation_results.plot_histogram_and_error_stats_4_panels(total_results_df,
                                                                    plot_file_name,
                                                                    place_name=place_name,
                                                                    verbose=verbose)

    files_to_export.append(plot_file_name)

    if results_h5 is not None:
        total_results_df.to_hdf(results_h5, key="results", complib="zlib", complevel=3)
        if verbose:
            print(results_h5, "written.")

    files_to_export.append(results_h5)

    return files_to_export

def define_and_parse_args():
    parser = argparse.ArgumentParser(
        description="Tool for validating a list or directory of DEMs against ICESat-2 photon data.")

    parser.add_argument("directory_or_files", type=str, nargs='+',
        help="A directory path, or a list of individual DEM tiles. Defaults to the same as the input directory, or the directory in which the first DEM resides.")

    parser.add_argument("--fname_filter", "-ff", type=str, default=r"\.tif\Z",
        help=r"A regex string to search for in all DEM file names, to use as a filter. Defaults to r'\.tif\Z', indicating .tif at the end of the file name. Helps elimiate files that shouldn't be considered.")

    parser.add_argument("--fname_omit", "-fo", type=str, default=None,
        help="A regex string to search for and OMIT if it contains a match in the file name. Useful for avoiding derived datasets (such as converted DEMs) in the folder.")

    parser.add_argument("--output_dir", "-od", type=str, default=None,
        help="Directory to output results. Default to the a sub-directory named 'icesat2' within the input directory.")

    parser.add_argument("--input_vdatum", "-ivd", default="wgs84",
        help="The vertical datum of the input DEMs. [TODO: List possibilities here.] Default: 'wgs84'")

    parser.add_argument("--output_vdatum", "-ovd", default="wgs84",
        help="The vertical datume of the output analysis. Must be a vdatum compatible with Icesat-2 granules. Default: Use the same vdatum as the input files.")

    parser.add_argument("--place_name", "-name", type=str, default=None,
        help="Readable name of the location being validated. Will be used in output summary plots and validation report.")

    parser.add_argument("--overwrite", "-o", action="store_true", default=False,
        help="Overwrite all files, including intermittent data files. Default: False (skips re-computing already-computed reseults.")

    parser.add_argument("--create_folders", action="store_true", default=False,
        help="Create folders specified in -output_dir and -data_dir, as well as the full path to -photon_h5, if they do not already exist. Default: Raise errors if paths don't already exist.")

    parser.add_argument('--use_urban_mask', action='store_true', default=False,
        help="Use the WSL 'Urban Area' mask rather than OSM building footprints to mask out IceSat-2 data. Useful over lower-resolution (10m or coarser) dems, which tend to be bigger than building footprints.")

    parser.add_argument("--individual_results", "-ind", action="store_true", default=False,
        help="By default, a summary plot and text file are generated for the dataset. If this is selected, they will be generated for each individual DEM as well. Files will be placed in the -output_dir directory.")

    parser.add_argument("--include_photon_validation", "-ph", action="store_true", default=False,
        help="Produce a photon database (stored in '*_photon_level_results.h5') with errors on a photon-level (not cell-level) scale. Useful for identifying bad ICESat-2 granules.")

    parser.add_argument("--delete_datafiles", "-del", action="store_true", default=False,
        help="By default, all data files generted in this process are kept. If this option is chosen, delete them.")

    parser.add_argument("--outlier_sd_threshold", default="2.5",
        help="Number of standard-deviations away from the mean to omit outliers. Default 2.5. May choose 'None' if no filtering is requested.")

    parser.add_argument("--measure_coverage", "-mc", action="store_true", default=False,
        help="Measure the coverage %age of icesat-2 data in each of the output DEM cells.")

    parser.add_argument("--write_result_tifs", action='store_true', default=False,
        help="""Write output geotiff with the errors in cells that have ICESat-2 photons, NDVs elsewhere.""")

    parser.add_argument("--write_summary_csv", action='store_true', default=False,
        help="Write a CSV with summary results of each individual DEM.")

    parser.add_argument("--quiet", "-q", action="store_true", default=False,
        help="Suppress output.")

    return parser.parse_args()

def main():
    args = define_and_parse_args()

    if args.output_dir is None:
        # Default to data dir or directory of first data file, in the 'icesat2' sub-directory.
        path = args.directory_or_files[0]
        if type(path) is str and os.path.isdir(path):
            args.output_dir = os.path.join(path, "icesat2")
        else:
            args.output_dir = os.path.join(os.path.split(path)[0], "icesat2")

        assert type(args.output_dir) == str

    if (type(args.fname_filter) in (list, tuple)) and (len(args.fname_filter) == 1):
        args.fname_filter = args.fname_filter[0]

    # Check for the existence of appropriate output & data directories.

    output_dir = os.path.abspath(args.output_dir)
    if not os.path.exists(output_dir):
        if args.create_folders:
            os.makedirs(output_dir)
        else:
            raise FileNotFoundError("Output directory '{0}' does not exist. Create directory or use the --create_folders flag upon execution.".format(args.output_dir))

    # NOTE: This code assumes that if we create the directory here, it will
    # not be erased befor the code gets to putting files there later. Seems
    # like a generally safe assumption, and behavior is okay if another process
    # or the user manually deletes directories during execusion, it will cause
    # the program to crash when it tries to write files there. That's a user error.

    validate_list_of_dems(args.directory_or_files,
                          fname_filter=args.fname_filter,
                          fname_omit=args.fname_omit,
                          output_dir=args.output_dir,
                          input_vdatum=args.input_vdatum,
                          output_vdatum=args.output_vdatum,
                          overwrite=args.overwrite,
                          place_name=args.place_name,
                          create_individual_results=args.individual_results,
                          delete_datafiles=args.delete_datafiles,
                          include_photon_validation=args.include_photon_validation,
                          write_result_tifs=args.write_result_tifs,
                          use_urban_mask=args.use_urban_mask,
                          omit_bad_granules=True,
                          measure_coverage=args.measure_coverage,
                          write_summary_csv=args.write_summary_csv,
                          outliers_sd_threshold=ast.literal_eval(args.outlier_sd_threshold),
                          verbose=not args.quiet)

if __name__ == "__main__":
    main()
