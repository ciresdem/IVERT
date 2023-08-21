#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun 22 16:06:21 2021

@author: mmacferrin
"""

# try:
#     # We don't actually use the cudem modules here, but we make command-line
#     # calls to "waffles", so check here to make sure cudem is installed on this
#     # machine. If we can import the waffles module, we can use it from the command-line.
#     from cudem import waffles
# except:
#     raise ModuleNotFoundError("Module 'cudem/waffles.py' required. Update paths, or refer to https://github.com/ciresdem/cudem for installation instructions.")
# EMPTY_VAL = -9999

####################################3
# Include the base /src/ directory of thie project, to add all the other modules.
import import_parent_dir; import_parent_dir.import_src_dir_via_pythonpath()
####################################3
import utils.progress_bar as progress_bar
import utils.parallel_funcs as parallel_funcs
import utils.configfile
import etopo.convert_vdatum as convert_vdatum
import etopo.coastline_mask as coastline_mask
# import icesat2.icepyx_download as icepyx_download
import icesat2.nsidc_download as nsidc_download
import icesat2.plot_validation_results as plot_validation_results
import icesat2.classify_icesat2_photons as classify_icesat2_photons
import icesat2.icesat2_photon_database as icesat2_photon_database
import icesat2.find_bad_icesat2_granules as find_bad_icesat2_granules

# import subprocess
from osgeo import gdal, osr
import os
import argparse
import numpy
import pandas
import multiprocessing as mp
import time
import numexpr
import pyproj

etopo_config = utils.configfile.config()
EMPTY_VAL = etopo_config.etopo_ndv

# 1: DEM Preprocessing:
    # a) For Worldview, apply the bitmask and matchtag filters to get rid of noise
    # b) Get an ocean/land mask for it
        # i) Use the CUDEM "waffles" command to do this.
    # c) Generate a bounding-box for the ICESat-2 data

def read_dataframe_file(df_filename):
    """Read a dataframe file, either from a picklefile, HDF, or a CSV.

    (Can handle other formats by adding more "elif ..." statements in the function.)
    """
    assert os.path.exists(df_filename)
    base, ext = os.path.splitext(df_filename)
    ext = ext.lower()
    if ext == ".pickle":
        dataframe = pandas.read_pickle(df_filename)
    elif ext in (".h5", ".hdf"):
        dataframe = pandas.read_hdf(df_filename, mode="r")
    elif ext in (".csv", ".txt"):
        dataframe = pandas.read_csv(df_filename)
    else:
        print(f"ERROR: Unknown dataframe file extension '{ext}'. (Currently supporting .pickle, .h5, .hdf, .csv, or .txt)")

    return dataframe

def collect_raw_photon_data(dem_bbox,
                            photon_dataframe_file,
                            granule_ids,
                            dem_bbox_converter = None,
                            overwrite=False,
                            verbose=True):
    """Get the photon data (lat, lon, elev, code) of the ICESat-2.

    Several options here, but only one data source is needed.

    If dem_bbox (only) is provided and photon_dataframe_files exists on disk (and not overwrite):
        - Read that dataframe from disk.
        - Crop out photons that don't lie within the bounding box
        - Return the pandas dataframe subset.

    If photon_dataframe_file does not exist on disk (or overwrite is true) and granule_ids is None:
        - Get a list of ICESat-2 granules within the bounding box (check for existing files in the directory).
        - Process the data inside the granules, save it to a pandas dataframe.
        - Save that dataframe to disk.
        - Crop out photons that don't lie within the bounding box
        - Return the pandas dataframe subset.

    If granule_ids are provided:
        - Read the icesat-2 granules (download ones that don't exist in the icesat-2 folder)
        - Process the data inside the granules, save it to a pandas dataframe.
        - Save that dataframe to disk.
        - Crop out photons that don't lie within the bounding box
        - Return the pandas dataframe subset.

    If dem_bbox_converter is provided, it should be an instantiated instance of osgeo.osr.CoordinateTransformation.
    That class module is used to convert icesat-2 points from their original projection into the
    projection that the DEM is using.
    """
    # Get icesat-2 land/veg photon data within the bounding-box of our DEM.
    dataframe = None

    if os.path.exists(photon_dataframe_file) and not overwrite:
        dataframe = read_dataframe_file(photon_dataframe_file)
        # dataframe = dataframe.loc()

    else:

        dataframe = classify_icesat2_photons.get_photon_data_multiple_granules(granule_ids,
                                                                               bounding_box=dem_bbox,
                                                                               bbox_converter = dem_bbox_converter,
                                                                               return_type=pandas.DataFrame,
                                                                               verbose = verbose)

        base, ext = os.path.splitext(photon_dataframe_file)
        # if ext.lower() in (".gz", ".bz2", ".zip", ".xz"):
        #     compression = "infer"
        # else:
        #     compression = "zip"
        dataframe.to_hdf(photon_dataframe_file, "icesat2", complib="zlib", mode='w')
        if verbose:
            print(photon_dataframe_file, "written.")


    if dataframe is None:
        print("No dataframe read. 'collect_raw_photon_data() returning None.")

    return dataframe

def validate_dem_child_process(input_heights, input_i, input_j, photon_codes, connection, photon_limit=None,
                               measure_coverage=False, input_x=None, input_y=None, num_subdivisions=15):
    """A child process for running the DEM validation in parallel.

    It takes the input_height (m) and the dem_indices (flattened), as well
    as a duplexed multiprocessing.connection.Connection object (i.e. an open pipe)
    for processing it. It reads the arrays into local memory, then uses the connection
    to pass data back and forth until getting a "STOP" command over the connection.

    'measure_coverage' is a boolean parameter to measure how well a given pixel is covered by ICESat-2 photons.
    We'll measure a couple of different measures (centrality and coverage), and insert those parameters in the output."""
    # Copy the "input_heights" array to local memory.
    # Loop until it's not conflicting with another process.
    # NOTE: This could potentially cause a blocking issue of ConnectionRefusedError happens
    # for other reasons (some sort of failure in the system). A way around this is to enable
    # MAX_TRIES cutoffs, but this inserts the risk of accidentally "cutting off" valid data
    # if you have a lot of competing processes going after the same resource that take a
    # while to get out of each others' way. I will have to think about the best way to
    # intelligently handle both these potential scenarios.
    connection_success = False
    # MAX_TRIES = 1000
    # counter = 0
    while not connection_success:
        # if counter >= MAX_TRIES:
        #     print(f"validate_dem_child_process: Counter exceeded {MAX_TRIES} attempts to get 'input_heights'. Aborting subprocess.")
        #     return
        try:
            heights = numpy.array(input_heights[:])
            connection_success = True
        except ConnectionRefusedError:
            pass
            # counter += 1

    # Get the dem_indices array into local memory.
    # Loop until it's not conflicting with another process)
    connection_success = False
    # counter = 0
    while not connection_success:
        # if counter >= MAX_TRIES:
        #     print(f"validate_dem_child_process: Counter exceeded {MAX_TRIES} attempts to get 'input_i'. Aborting subprocess.")
        #     return
        try:
            photon_i = numpy.array(input_i[:])
            connection_success = True
        except ConnectionRefusedError:
            pass
            # counter += 1

    # Get the dem_indices array into local memory.
    # Loop until it's not conflicting with another process)
    connection_success = False
    # counter = 0
    while not connection_success:
        # if counter >= MAX_TRIES:
        #     print(f"validate_dem_child_process: Counter exceeded {MAX_TRIES} attempts to get 'input_j'. Aborting subprocess.")
        #     return
        try:
            photon_j = numpy.array(input_j[:])
            connection_success = True
        except ConnectionRefusedError:
            pass
            # counter += 1

    # Get the photon_codes array into local memory.
    # Loop until it's not conflicting with another process)
    connection_success = False
    # counter = 0
    while not connection_success:
        # if counter >= MAX_TRIES:
        #     print(f"validate_dem_child_process: Counter exceeded {MAX_TRIES} attempts to get 'photon_codes'. Aborting subprocess.")
        #     return
        try:
            ph_codes = numpy.array(photon_codes[:])
            connection_success = True
        except ConnectionRefusedError:
            pass
            # counter += 1

    # If we're measuring the coverage of the photons, we need the latitudes and longitudes as well to get the
    # photon locations within a grid-cell.
    if measure_coverage:
        assert (input_x is not None) and (input_y is not None)

        connection_success = False
        # counter = 0
        while not connection_success:
            # if counter >= MAX_TRIES:
            #     print(f"validate_dem_child_process: Counter exceeded {MAX_TRIES} attempts to get 'photon_codes'. Aborting subprocess.")
            #     return
            try:
                ph_x = numpy.array(input_x[:])
                connection_success = True
            except ConnectionRefusedError:
                pass

        connection_success = False
        # counter = 0
        while not connection_success:
            # if counter >= MAX_TRIES:
            #     print(f"validate_dem_child_process: Counter exceeded {MAX_TRIES} attempts to get 'photon_codes'. Aborting subprocess.")
            #     return
            try:
                ph_y = numpy.array(input_y[:])
                connection_success = True
            except ConnectionRefusedError:
                pass

        assert len(heights) == len(ph_x) == len(ph_y)

    assert len(heights) == len(photon_i) == len(photon_j) == len(ph_codes)

    # Just keep looping and checking the connection pipe. When we get
    # a stop command, return from the function.
    while True:
        if connection.poll():
            if measure_coverage:
                # If we're measuring the coverage, also give us the bounding boxes of the grid cells
                dem_i_list, \
                dem_j_list, \
                dem_elev_list, \
                cell_xmin_list, \
                cell_xmax_list, \
                cell_ymin_list, \
                cell_ymax_list = connection.recv()
            else:
                dem_i_list, \
                dem_j_list, \
                dem_elev_list = connection.recv()

            # Break out of the infinite loop and return when we get a "STOP" message.
            if (type(dem_i_list) == str) and (dem_i_list == "STOP"):
                return

            assert len(dem_i_list) == len(dem_j_list)
            N = len(dem_i_list)

            # Do the work.
            r_mean = numpy.zeros((N,), dtype=float)
            r_median = numpy.zeros((N,), dtype=float)
            r_numphotons = numpy.zeros((N,), dtype=numpy.uint32)
            r_numphotons_intd = r_numphotons.copy()
            r_std = numpy.zeros((N,), dtype=float)
            r_interdecile = numpy.zeros((N,), float)
            r_range = numpy.zeros((N,), heights.dtype)
            r_10p = numpy.zeros((N,), float)
            r_90p = numpy.zeros((N,), float)
            r_canopy_fraction = numpy.zeros((N,), numpy.float16)
            r_dem_elev = numpy.zeros((N,), dtype=float)
            r_mean_diff = numpy.zeros((N,), dtype=float)
            r_med_diff = numpy.zeros((N,), dtype=float)

            if measure_coverage:
                # r_min_distance_to_center = numpy.zeros((N,), dtype=float)
                r_coverage_frac = numpy.zeros((N,), dtype=float)

            for counter,(i,j) in enumerate(zip(dem_i_list, dem_j_list)):
                # Using numexpr.evaluate here is far more memory-and-time efficient than just doing it with the numpy arrays.
                ph_subset_mask = numexpr.evaluate("(photon_i == i) & (photon_j == j)")
                # Generate a small pandas dataframe from the subset
                subset_df = pandas.DataFrame({'height': heights[ph_subset_mask],
                                              'ph_code': ph_codes[ph_subset_mask]})

                # Define and compute measures of centrality & coverage here.
                if measure_coverage:
                    # Add the x and y coords to the dataframe
                    subset_df['xcoord'] = ph_x[ph_subset_mask]
                    subset_df['ycoord'] = ph_y[ph_subset_mask]

                    cell_xmin = cell_xmin_list[counter]
                    cell_xmax = cell_xmax_list[counter]
                    cell_ymin = cell_ymin_list[counter]
                    cell_ymax = cell_ymax_list[counter]
                    assert (cell_xmax > cell_xmin) and (cell_ymax > cell_ymin)

                    cell_xstep = (cell_xmax - cell_xmin) / num_subdivisions
                    # Equal to the geotransform, the y-value starts at the top (max) and iterate downward (negative step.)
                    cell_ystep = (cell_ymin - cell_ymax) / num_subdivisions

                    assert (cell_xstep > 0) and (cell_ystep < 0)

                    subset_df['subset_i'] = numpy.floor((subset_df.ycoord - cell_ymax) / cell_ystep).astype(int)
                    subset_df['subset_j'] = numpy.floor((subset_df.xcoord - cell_xmin) / cell_xstep).astype(int)
                    # These are good assupmtions but the "numpy.all() command slows things down for big datasets, and
                    # we've never needed it so it's probably unnecessary. So skip it.
                    # assert numpy.all((subset_df.subset_i >= 0) & (subset_df.subset_i < num_subdivisions))
                    # assert numpy.all((subset_df.subset_j >= 0) & (subset_df.subset_j < num_subdivisions))

                    # NOTE: I had been calculating the minimum distance of the closest photon to the center of the grid-cell,
                    # but this proves not to be a good measure of coverage. For efficiency's sake, omit it now.

                    # cell_xcenter = cell_xmin + ((cell_xmax - cell_xmin) / 2)
                    # cell_ycenter = cell_ymin + ((cell_ymax - cell_ymin) / 2)

                    # Calcluate minimum distance from cell center, i.e. distance of the "closest photon."
                    # cell_min_distance_from_center = numpy.power(numpy.power(subset_df.xcoord - cell_xcenter, 2) + \
                    #                                             numpy.power(subset_df.ycoord - cell_ycenter, 2),
                    #                                             0.5).min()
                    #
                    # r_min_distance_to_center[counter] = cell_min_distance_from_center

                    # By taking i * (number_of_rows) + j, we come up with unique single values for the sub-cell this is in.
                    subset_df['subset_ij'] = (subset_df.subset_i * num_subdivisions) + subset_df.subset_j
                    # Count how many unique subset-cells are covered and divide by the number of total sub-cells.
                    cell_fraction_covered = len(subset_df.subset_ij.unique()) / (num_subdivisions ** 2)
                    r_coverage_frac[counter] = cell_fraction_covered

                # After calculating the coverage, if we want to limit the number of photons we're dealing with total,
                # do it here.
                if photon_limit is not None and len(subset_df) > photon_limit:
                    assert photon_limit >= 2
                    subset_df = subset_df.sample(n=photon_limit)

                r_numphotons[counter] = len(subset_df)
                if len(subset_df) > 0:
                    r_canopy_fraction[counter] = (subset_df.ph_code >= 2).sum() / len(subset_df)
                else:
                    r_canopy_fraction[counter] = EMPTY_VAL

                r_dem_elev[counter] = dem_elev_list[counter]

                ground_only_df = subset_df[subset_df.ph_code == 1]
                if len(ground_only_df) < 3:
                    r_range[counter] = EMPTY_VAL
                    r_10p[counter] = EMPTY_VAL
                    r_90p[counter] = EMPTY_VAL
                    r_interdecile[counter] = EMPTY_VAL
                    r_numphotons_intd[counter] = len(ground_only_df)
                    r_mean[counter] = EMPTY_VAL
                    r_median[counter] = EMPTY_VAL
                    r_std[counter] = EMPTY_VAL
                    r_mean_diff[counter] = EMPTY_VAL
                    r_med_diff[counter] = EMPTY_VAL
                else:
                    height_desc = ground_only_df.height.describe(percentiles=[0.10, 0.90])
                    r_range[counter] = height_desc['max'] - height_desc['min']
                    # zp10, zp90 = numpy.percentile(cph_z, [10,90])
                    zp10 = height_desc['10%']
                    zp90 = height_desc['90%']
                    r_10p[counter], r_90p[counter] = zp10, zp90
                    r_interdecile[counter] = zp90 - zp10
                    # Get only the photons within the inter-decile range
                    # cph_z_intd = cph_z[(cph_z >= zp10) & (cph_z <= zp90)]
                    df_intd = ground_only_df[(ground_only_df.height >= zp10) & (ground_only_df.height <= zp90)]
                    r_numphotons_intd[counter] = len(df_intd)
                    if len(df_intd) >= 1:
                        height_intd_desc = df_intd.height.describe()

                        r_mean[counter] = height_intd_desc['mean']
                        r_median[counter] = height_intd_desc['50%']
                        r_std[counter] = height_intd_desc['std']
                        r_mean_diff[counter] = dem_elev_list[counter] - r_mean[counter]
                        r_med_diff[counter] = dem_elev_list[counter] - r_median[counter]

                    else:
                        r_mean[counter]      = EMPTY_VAL
                        r_median[counter]    = EMPTY_VAL
                        r_std[counter]       = EMPTY_VAL
                        r_mean_diff[counter] = EMPTY_VAL
                        r_med_diff[counter]  = EMPTY_VAL

            # Generate a little dataframe of the outputs for all the different grid cells to return.
            results_df = pandas.DataFrame({"i": dem_i_list,
                                           "j": dem_j_list,
                                           "mean": r_mean,
                                           "median": r_median,
                                           "stddev": r_std,
                                           "numphotons": r_numphotons,
                                           "numphotons_intd": r_numphotons_intd,
                                           "interdecile_range": r_interdecile,
                                           "range": r_range,
                                           "10p": r_10p,
                                           "90p": r_90p,
                                           "canopy_fraction": r_canopy_fraction,
                                           "dem_elev": r_dem_elev,
                                           "diff_mean": r_mean_diff,
                                           "diff_median": r_med_diff})\
                        .set_index(["i", "j"])

            if measure_coverage:
                # Add columns for centrality measurements here.
                # results_df["min_dist_from_center"] = r_min_distance_to_center
                results_df["coverage_frac"] = r_coverage_frac

            connection.send(results_df)

    return


def clean_procs_and_pipes(procs, pipes1, pipes2):
    """Join all processes and close all pipes.

    Useful for cleaning up after multiprocessing."""
    for pr in procs:
        if isinstance(pr, mp.Process):
            if pr.is_alive():
                pr.kill()
            pr.join()
    for p1 in pipes1:
        if isinstance(p1, mp.connection.Connection):
            p1.close()
    for p2 in pipes2:
        if isinstance(p2, mp.connection.Connection):
            p2.close()
    return

def kick_off_new_child_process(height_array,
                               i_array,
                               j_array,
                               code_array,
                               photon_limit=None,
                               measure_coverage=False,
                               input_x=None,
                               input_y=None,
                               num_subdivisions=15):
    """Start a new subprocess to handle and process data."""
    pipe_parent, pipe_child = mp.Pipe(duplex=True)
    proc = mp.Process(target=validate_dem_child_process,
                      args=(height_array,
                            i_array,
                            j_array,
                            code_array,
                            pipe_child),
                      kwargs={"measure_coverage": measure_coverage,
                              "input_x": input_x,
                              "input_y": input_y,
                              "photon_limit": photon_limit,
                              "num_subdivisions": num_subdivisions}
                      )
    proc.start()
    return proc, pipe_parent, pipe_child


def validate_dem_parallel(dem_name,
                          photon_dataframe_name=None,
                          use_icesat2_photon_database=True,
                          icesat2_photon_database_obj=None,
                          dem_vertical_datum="egm2008",
                          output_vertical_datum="egm2008",
                          granule_ids=None,
                          results_dataframe_file=None,
                          icesat2_date_range=["2021-01-01", "2021-12-31"],
                          interim_data_dir=None,
                          overwrite=False,
                          delete_datafiles=False,
                          mask_out_lakes=True,
                          mask_out_buildings=True,
                          use_osm_planet=False,
                          mask_out_urban=False,
                          include_gmrt_mask=False,
                          write_result_tifs=True,
                          write_summary_stats=True,
                          skip_icesat2_download=True,
                          include_photon_level_validation=False,
                          plot_results=True,
                          location_name=None,
                          mark_empty_results=True,
                          omit_bad_granules=True,
                          measure_coverage=False,
                          max_photons_per_cell=None,
                          numprocs=parallel_funcs.physical_cpu_count(),
                          quiet=False):
    """The main function. Do it all here. But do it on more than one processor.
    TODO: Document all these method parameters. There are a bunch and they need better explanation.
    """
    # Just get this variable defined so all the code-branches can use it.
    dem_ds = None

    # Get the results dataframe filename (if not already set)
    if (results_dataframe_file is None) or (results_dataframe_file == ""):
        results_dataframe_file = os.path.splitext(dem_name)[0] + "_results.h5"

    # Get the interim data directory (if not already set)
    if interim_data_dir is None:
        interim_data_dir = os.path.split(results_dataframe_file)[0]

    if mark_empty_results:
        base, ext = os.path.splitext(results_dataframe_file)
        empty_results_filename = base + "_EMPTY.txt"

    if write_summary_stats:
        summary_stats_filename = os.path.splitext(results_dataframe_file)[0] + "_summary_stats.txt"

    if write_result_tifs:
        result_tif_filename = os.path.splitext(results_dataframe_file)[0] + "_ICESat2_error_map.tif"

    if plot_results:
        plot_filename = os.path.splitext(results_dataframe_file)[0] + "_plot.png"


    # If the output file already exists and we aren't overwriting, create any un-created datasets
    # if they're requested, and then just return.
    if not overwrite:

        if os.path.exists(results_dataframe_file):

            results_dataframe = None

            if write_summary_stats and not os.path.exists(summary_stats_filename):
                if results_dataframe is None:
                    if not quiet:
                        print("Reading", results_dataframe_file, '...', end="")
                    results_dataframe = read_dataframe_file(results_dataframe_file)
                    if not quiet:
                        print("done.")

                write_summary_stats_file(results_dataframe, summary_stats_filename, verbose=not quiet)

            if write_result_tifs and not os.path.exists(result_tif_filename):
                if dem_ds is None:
                    dem_ds = gdal.Open(dem_name, gdal.GA_ReadOnly)

                if results_dataframe is None:
                    if not quiet:
                        print("Reading", results_dataframe_file, '...', end="")
                    results_dataframe = read_dataframe_file(results_dataframe_file)
                    if not quiet:
                        print("done.")

                generate_result_geotiffs(results_dataframe, dem_ds, results_dataframe_file, verbose=not quiet)

            if plot_results and not os.path.exists(plot_filename):
                if location_name is None:
                    location_name = os.path.split(dem_name)[1]

                if results_dataframe is None:
                    if not quiet:
                        print("Reading", results_dataframe_file, '...', end="")
                    results_dataframe = read_dataframe_file(results_dataframe_file)
                    if not quiet:
                        print("done.")

                plot_validation_results.plot_histogram_and_error_stats_4_panels(results_dataframe,
                                                                            plot_filename,
                                                                            place_name=location_name,
                                                                            verbose=not quiet)

            if (results_dataframe is None) and not quiet:
                print("Everything seems already done here. Moving on...")

            return

        elif mark_empty_results and os.path.exists(empty_results_filename):
            if not quiet:
                print("No valid data produced during previous ICESat-2 analysis of", dem_name + ". Returning.")
            return

    # Collect the metadata from the DEM.
    dem_ds, dem_array, dem_bbox, dem_epsg, dem_step_xy, \
        coastline_mask_filename, coastline_mask_array = \
        coastline_mask.get_coastline_mask_and_other_dem_data(dem_name,
                                                             mask_out_lakes = mask_out_lakes,
                                                             mask_out_buildings = mask_out_buildings,
                                                             mask_out_urban = mask_out_urban,
                                                             use_osm_planet = use_osm_planet,
                                                             include_gmrt = include_gmrt_mask,
                                                             target_fname_or_dir = interim_data_dir)


    # Test for compound CRSs. If it's that, just get the horizontal crs from it.
    dem_crs_obj = pyproj.CRS.from_epsg(dem_epsg)
    if dem_crs_obj.is_compound:
        dem_epsg = dem_crs_obj.sub_crs_list[0].to_epsg()

    # The dem_array and the coastline_mask_array should have the same shape
    try:
        assert coastline_mask_array.shape == dem_array.shape
    except AssertionError as e:
        print("dem file:", dem_name)
        print("coastline file:", coastline_mask_filename)
        print("coastline:", coastline_mask_array.shape, "dem:", dem_array.shape)
        raise e
    # If the coastline mask is all 1's, warn about that.
    if coastline_mask_array.size == numpy.count_nonzero(coastline_mask_array):
        print("WARNING: Coastline mask file", coastline_mask_filename, "contains all 1's.")

    # Assert that the both the dem vertical datum and the output vertical datum are valid values.
    if type(dem_vertical_datum) == str:
        dem_vertical_datum = dem_vertical_datum.strip().lower()
    # assert dem_vertical_datum in convert_vdatum.SUPPORTED_VDATUMS
    if type(output_vertical_datum) == str:
        output_vertical_datum = output_vertical_datum.strip().lower()
    assert output_vertical_datum in convert_vdatum.SUPPORTED_VDATUMS

    # Convert the vdatum of the input dem to be the same as the output process.
    if dem_vertical_datum != output_vertical_datum:
        dem_base, dem_ext = os.path.splitext(os.path.split(dem_name)[1])
        converted_dem_name = os.path.join(interim_data_dir, dem_base + "_" + output_vertical_datum + dem_ext)

        if not os.path.exists(converted_dem_name):
            retval = convert_vdatum.convert_vdatum(dem_name,
                                                   converted_dem_name,
                                                   input_vertical_datum=dem_vertical_datum,
                                                   output_vertical_datum=output_vertical_datum,
                                                   verbose=not quiet)
            # This is the old convert_vdatum code.
            # subprocess.run([os.path.join(os.path.split(__file__)[0], "convert_vdatum.py"),
            #                 dem_name,
            #                 "-output_dem", converted_dem_name,
            #                 "-input_vdatum", dem_vertical_datum,
            #                 "-output_vdatum", output_vertical_datum,
            #                 "-tempdir", os.path.split(dem_base)[0],
            #                 "-interp_method", "cubic",
            #                 "" if delete_datafiles else "--keep_grids",
            #                 "--quiet" if quiet else "",
            #                 ])
            if (retval != 0) or (not os.path.exists(converted_dem_name)):
                raise FileNotFoundError(f"{dem_name} not converted correctly to {converted_dem_name}. Aborting.")

        # Get the dem array from the new dataset.
        dem_ds = None
        dem_ds = gdal.Open(converted_dem_name, gdal.GA_ReadOnly)
        dem_array = dem_ds.GetRasterBand(1).ReadAsArray()
    else:
        converted_dem_name = None

    # If we've been provided an open dataframe rather than just the name of the file, simply use it.
    if photon_dataframe_name is None and use_icesat2_photon_database == False:
        raise ValueError("Input error: must specify either use_icesat2_photon_database or provide a photon dataframe name.")

    elif isinstance(photon_dataframe_name, pandas.DataFrame):
        photon_df = photon_dataframe_name

    elif use_icesat2_photon_database:
        if icesat2_photon_database_obj is None:
            icesat2_photon_database_obj = icesat2_photon_database.ICESat2_Database()

        photon_df = icesat2_photon_database_obj.get_photon_database(dem_bbox,
                                                                    build_tiles_if_nonexistent = True,
                                                                    verbose = not quiet)
        if photon_df is None:
            if mark_empty_results:
                with open(empty_results_filename, 'w') as f:
                    f.close()
                if not quiet:
                    print("Created", empty_results_filename, "to indicate no valid ICESat-2 data was returned here.")

            return None


    # If the photon dataframe file containing all the photons in this tile already exists, just use it.
    elif skip_icesat2_download and os.path.exists(photon_dataframe_name) and overwrite==False:
        if not quiet:
            print("Reading", photon_dataframe_name + "...", end="")
        photon_df = pandas.read_hdf(photon_dataframe_name)
        if not quiet:
            print("Done.")

    elif granule_ids is None:
        # If the granules already exist in the directory and we've planned to skip
        # re-downloading them, then skip them!
        granules_existing_in_directory = [os.path.join(interim_data_dir, fname) for fname in os.listdir(interim_data_dir) if (os.path.splitext(fname)[1].lower() == ".h5" and fname.upper().find("ATL") >= 0)]
        if skip_icesat2_download and (len(granules_existing_in_directory) > 0):
            atl03_granules_list = [fname for fname in granules_existing_in_directory if fname.upper().find("ATL03") >= 0]
            atl08_granules_list = [fname for fname in granules_existing_in_directory if fname.upper().find("ATL08") >= 0]

        else:
            # If the DEM is in a projection other than WGS84, get the bbox coordinates and convert to WGS84
            # NOTE: This will break in polar stereo projections, and perhaps others.
            # TODO: Find a more elegant way of doing this that doesn't fuck up polar projections.
            if dem_epsg != 4326:
                icesat2_srs = osr.SpatialReference()
                icesat2_srs.SetWellKnownGeogCS("EPSG:4326")
                dem_srs = osr.SpatialReference(wkt=dem_ds.GetProjection())
                # Convert bbox points from DEM projection into
                proj_to_wgs84 = osr.CoordinateTransformation(dem_srs, icesat2_srs)

                # Create a list of bbox points in counter-clockwise order.
                xmin, ymin, xmax, ymax = dem_bbox
                points = [(xmin, ymin), (xmin, ymax), (xmax, ymax), (xmax, ymin), (xmin, ymin)]
                output_points = proj_to_wgs84.TransformPoints(points)
                bbox_wgs84 = [(p[0], p[1]) for p in output_points]
            else:
                bbox_wgs84 = dem_bbox

            granules_list = nsidc_download.main(short_name=["ATL03","ATL08"],
                                                region=bbox_wgs84,
                                                local_dir=interim_data_dir,
                                                version=etopo_config.nsidc_atl_version,
                                                dates = icesat2_date_range,
                                                fname_python_regex="\.h5\Z",
                                                force=overwrite,
                                                quiet=quiet)
            # Just get the .h5 files from the query (skip the .xml's)
            atl03_granules_list = [fn for fn in granules_list if os.path.split(fn)[1].find("ATL03") > -1]
            atl08_granules_list = [fn for fn in granules_list if os.path.split(fn)[1].find("ATL08") > -1]

        common_granule_ids = []
        for atl03_gid in atl03_granules_list:
            fpath, fname = os.path.split(atl03_gid)
            if os.path.exists(os.path.join(fpath, fname.replace("ATL03","ATL08"))):
                common_granule_ids.append(atl03_gid)

        if len(common_granule_ids) < 0.5*len(atl03_granules_list):
            raise UserWarning("ICESat-2 ATL03 granules IDs are less than 50% in common with ATL08 granule ids over " + \
                              "bounding box {0}, {1} of {2} matching.".format(dem_bbox,
                                                                              len(common_granule_ids),
                                                                              len(atl03_granules_list)))

        photon_df = None
    else:
        common_granule_ids = granule_ids
        photon_df = None

    # If the DEM is not in WGS84 coordaintes, create a conversion funtion to pass to sub-functions.
    if dem_epsg != 4326:
        dem_proj_wkt = dem_ds.GetProjection()
        # print(dem_proj_wkt)
        icesat2_srs = osr.SpatialReference()
        icesat2_srs.SetWellKnownGeogCS("EPSG:4326")
        dem_srs = osr.SpatialReference(wkt=dem_proj_wkt)
        is2_to_dem = osr.CoordinateTransformation(icesat2_srs, dem_srs)
    else:
        is2_to_dem = None

    if photon_df is None:
        # Get the photon data from the dataframe.
        photon_df = collect_raw_photon_data(dem_bbox,
                                            photon_dataframe_name,
                                            granule_ids=common_granule_ids,
                                            overwrite=overwrite,
                                            dem_bbox_converter=is2_to_dem,
                                            verbose=not quiet)

    if not quiet:
        print("{0:,}".format(len(photon_df)), "ICESat-2 photons present in photon dataframe.")

    # Filter out to keep only the highest-quality photons.
    # quality_ph == 0 ("nominal") and "conf_land" == 4 ("high") and/or "conf_land_ice" == 4 ("high")
    # Using photon_df.eval() is far more efficient for complex expressions than a boolean python expression.
    good_photon_mask = photon_df.eval("(quality_ph == 0) & ((conf_land == 4) | (conf_land_ice == 4))")
    photon_df = photon_df[good_photon_mask].copy()

    if len(photon_df) == 0:
        if mark_empty_results:
            # Just create an empty file to makre this dataset as done.
            with open(empty_results_filename, 'w') as f:
                f.close()
            if not quiet:
                print("Created", empty_results_filename, "to indicate no data was returned here.")
        return None

    # If the DEM coordinate system isn't WGS84 lat/lon, convert the icesat-2
    # lat/lon data coordinates into the same CRS as the DEM
    if dem_epsg != 4326:
        lon_x = photon_df["longitude"]
        lat_y = photon_df["latitude"]
        latlon_array = numpy.array([lon_x, lat_y]).transpose()

        points = numpy.array(is2_to_dem.TransformPoints(latlon_array))
        p_x = points[:, 0]
        p_y = points[:, 1]
        photon_df["dem_x"] = p_x
        photon_df["dem_y"] = p_y

        ph_xcoords = p_x
        ph_ycoords = p_y

    # Subset the dataframe to photons within the DEM bounding box.
    # Also, filter out all noise photons.
    else:
        ph_xcoords = photon_df["longitude"]
        ph_ycoords = photon_df["latitude"]
        if measure_coverage:
            # If we're measuring the coverage, we'll just use the "dem_x" and "dem_y" values.
            photon_df["dem_x"] = ph_xcoords
            photon_df["dem_y"] = ph_ycoords

    # Compute the (i,j) indices into the array of all the photons collected.
    # Transform photon lat/lons into DEM indices.
    xstart, xstep, _, ystart, _, ystep = dem_ds.GetGeoTransform()
    xend = xstart + (xstep * dem_array.shape[1])
    yend = ystart + (ystep * dem_array.shape[0])

    # Clip to the bounding box.
    minx = min(xstart, xend)
    maxx = max(xstart, xend)
    miny = min(ystart, yend)
    maxy = max(ystart, yend)
    df_class_code = photon_df["class_code"].to_numpy()
    # Again, using a numexpr expression here is far more time-and-memory efficient than doing all these compound boolean
    # operations on the numpy arrays in a Python expression.
    ph_bbox_mask = numexpr.evaluate("(ph_xcoords >= minx) & " + \
                                    "(ph_xcoords < maxx) & " + \
                                    "(ph_ycoords > miny) & " + \
                                    "(ph_ycoords <= maxy) & " + \
                                    "(df_class_code >= 1)")
        #            (ph_xcoords >= min(xstart, xend)) & \
        #            (ph_xcoords < max(xstart, xend)) & \
        #            (ph_ycoords > min(ystart, yend)) & \
        #            (ph_ycoords <= max(ystart, yend)) & \
        #            (photon_df["class_code"] >= 1)

    # Subset the dataframe to only provide pixels in-bounds for our DEM
    photon_df = photon_df[ph_bbox_mask].copy() # Create a copy so as not to be assinging to a slice of the full dataframe.
    ph_xcoords = ph_xcoords[ph_bbox_mask]
    ph_ycoords = ph_ycoords[ph_bbox_mask]

    # Omit any photons from "bad granules" found from find_bad_icesat2_granules.py
    # NOTE: After we've filtered out bad granules from the ICESat-2 database, we can
    # un-set the "omit_bad_granules" flag because the database will have already globally been
    # filtered out of bad-granule entries.
    if omit_bad_granules:
        bad_granules_gid_list = \
            find_bad_icesat2_granules.get_list_of_granules_to_reject(refind_bad_granules = False,
                                                                     return_as_gid_numbers = True,
                                                                     verbose=not quiet)

        if len(bad_granules_gid_list) > 0:
            ph_bad_granule_mask = None
            for gid1, gid2 in bad_granules_gid_list:
                bad_g_mask = (photon_df["granule_id1"] == gid1) & (photon_df["granule_id2"] == gid2)
                if ph_bad_granule_mask is None:
                    ph_bad_granule_mask = bad_g_mask
                elif numpy.count_nonzero(bad_g_mask) > 0:
                    ph_bad_granule_mask = ph_bad_granule_mask | bad_g_mask

            # If we found some bad photons in bad granules (bad granules!), mask them out.
            if (ph_bad_granule_mask is not None) and (numpy.count_nonzero(ph_bad_granule_mask) > 0):
                n_ph_bad_granules_mask = ~ph_bad_granule_mask
                photon_df = photon_df[n_ph_bad_granules_mask].copy()
                ph_xcoords = ph_xcoords[n_ph_bad_granules_mask]
                ph_ycoords = ph_ycoords[n_ph_bad_granules_mask]


    photon_df["i"] = numpy.floor((ph_ycoords - ystart) / ystep).astype(int)
    photon_df["j"] = numpy.floor((ph_xcoords - xstart) / xstep).astype(int)

    # Get the nodata value for the array, if it exists.
    dem_ndv = dem_ds.GetRasterBand(1).GetNoDataValue()
    # Generate a mask of photons that are (1) on land, (2) within the bounding box, and (3) not no-data
    if dem_ndv is None:
        dem_goodpixel_mask = (coastline_mask_array > 0)
    else:
        dem_goodpixel_mask = (coastline_mask_array > 0) & (dem_array != dem_ndv)

    # Create an (i,j) multi-index into the array.
    photon_df = photon_df.set_index(["i", "j"], drop=False) #.sort_index()

    # Make sure that we only look at cells that have at least 1 ground photon in them.
    ph_mask_ground_only = (photon_df["class_code"] == 1)
    # ph_in_bounds = (photon_df.i >= 0) & (photon_df.i < dem_array.shape[0]) & \
    #                (photon_df.j >= 0) & (photon_df.j < dem_array.shape[1])

    dem_mask_w_ground_photons = numpy.zeros(dem_array.shape, dtype=bool)
    dem_mask_w_ground_photons[photon_df.i[ph_mask_ground_only],
                              photon_df.j[ph_mask_ground_only]] = 1
    # dem_mask_w_ground_photons[photon_df.i[ph_mask_ground_only & ph_in_bounds],
    #                           photon_df.j[ph_mask_ground_only & ph_in_bounds]] = 1

    dem_overlap_mask = dem_goodpixel_mask & dem_mask_w_ground_photons


    dem_overlap_i, dem_overlap_j = numpy.where(dem_overlap_mask)
    dem_overlap_elevs = dem_array[dem_overlap_mask]

    if measure_coverage:
        dem_overlap_xmin = xstart + (xstep * dem_overlap_j)
        dem_overlap_xmax = dem_overlap_xmin + xstep
        dem_overlap_ymax = ystart + (ystep * dem_overlap_i)
        dem_overlap_ymin = dem_overlap_ymax + ystep
    N = len(dem_overlap_i)

    if not quiet:
        num_goodpixels = numpy.count_nonzero(dem_goodpixel_mask)
        print("{:,}".format(num_goodpixels), "nonzero land cells exist in the DEM.")
        if num_goodpixels == 0:
            print("No land cells found in DEM with overlapping ICESat-2 data. Stopping and moving on.")

            if mark_empty_results:
                # Just create an empty file to makre this dataset as done.
                with open(empty_results_filename, 'w') as f:
                    f.close()
                if not quiet:
                    print("Created", empty_results_filename, "to indicate no data was returned here.")
            return None
        else:
            print("{:,} ICESat-2 photons overlap".format(len(photon_df)),
              "{:,}".format(N),
              "DEM cells ({:0.2f}% of total DEM data).".format(numpy.count_nonzero(dem_overlap_mask) * 100 / num_goodpixels))

    # If we have no data overlapping the valid land DEM cells, just return None
    if numpy.count_nonzero(dem_overlap_mask) == 0:
        if not quiet:
            print("No overlapping ICESat-2 data with valid land cells. Stopping and moving on.")

        if mark_empty_results:
            # Just create an empty file to makre this dataset as done.
            with open(empty_results_filename, 'w') as f:
                f.close()
            if not quiet:
                print("Created", empty_results_filename, "to indicate no data was returned here.")

        return None

    # If requested, perform a validation on a photon-by-photon basis, in addition to the grid-cell
    # analysis peformed later on down. First, create a dataframe with just the DEM elevations.
    if include_photon_level_validation:
        if not quiet:
            print("Performing photon-level validation...")

        if not quiet:
            print("\tSubsetting ground-only photons... ", end="")
        # Get the subset of the dataframe with ground-only photons.
        photon_df_ground_only = photon_df[ph_mask_ground_only]
        if not quiet:
            print("Done.")

        if not quiet:
            print("\tGenerating DEM elevation dataframe... ", end="")

        # # Generate a dataframe of the dem elevations, indexed by their i,j coordinates.
        dem_elev_df = pandas.DataFrame({"dem_elevation": dem_overlap_elevs},
                                       index = pandas.MultiIndex.from_arrays((dem_overlap_i, dem_overlap_j),
                                                                             names=("i", "j"))
                                       )
        if not quiet:
            print("Done with {0} records.".format(len(dem_elev_df)))

        # print(dem_elev_df)

        # Join the dataframes by their i,j values, which will add the "dem_i", "dem_j",
        # and "dem_elevations" columns to the photon dataframe.
        # This could take a while to run, depending on the sizes of the dataframes.
        # Both dataframes have (i,j) as their index, so this should be good, I shouldn't need to specify the "on=" parameter.
        if not quiet:
            print("\tJoining photon_df and DEM elevation tables... ", end="")
        photon_df_with_dem_elevs = photon_df_ground_only.join(dem_elev_df, how='left') # on=('i','j')
        # Then, drop all photons that didn't line up with a valid land cell according to the coastline mask (dem_elevation values would be NaN)
        photon_df_with_dem_elevs = photon_df_with_dem_elevs[pandas.notna(photon_df_with_dem_elevs["dem_elevation"])]
        if not quiet:
            print("Done with {0} records.".format(len(photon_df_with_dem_elevs)))

        # Get the correct height field from the database.
        if output_vertical_datum in ("ellipsoid", "wgs84"):
            height_field = photon_df_with_dem_elevs.h_ellipsoid
        elif output_vertical_datum in ("geoid", "egm2008"):
            height_field = photon_df_with_dem_elevs.h_geoid
        elif output_vertical_datum == "meantide":
            height_field = photon_df_with_dem_elevs.h_meantide
        else:
            raise ValueError("Should not have gotten here. Unhandled vdatum: {}".format(output_vertical_datum))

        # Subtract the elevations and give us a photon_level error bar.
        # This is a single-column subtraction, should be pretty quick.
        if not quiet:
            print("\tCalculating elevation differences... ", end="")
        photon_df_with_dem_elevs["dem_minus_is2_m"] = photon_df_with_dem_elevs["dem_elevation"] - height_field
        if not quiet:
            print("Done.")

        # Write out the photon level elevation difference dataset.
        base, ext = os.path.splitext(results_dataframe_file)
        photon_results_dataframe_file = base + "_photon_level_results" + ext
        if not quiet:
            print("\tWriting", os.path.split(photon_results_dataframe_file)[1] + "... ", end="")
        photon_df_with_dem_elevs.to_hdf(photon_results_dataframe_file, "icesat2", complib="zlib", complevel=3)
        if not quiet:
            # Add an extra newline at the end to visually separate it from the next set of steps.
            print("Done.\n")

    if not quiet:
        if max_photons_per_cell is not None:
            print("Limiting processing to {0} photons per grid cell.".format(max_photons_per_cell))

        print("Performing ICESat-2/DEM cell validation...")

    # Gather a list of all the little results mini-dataframes from all the sub-processes running.
    # Concatenate them into a master results dataframe at the end.
    results_dataframes_list = []

    t_start = time.perf_counter()

    # Set up subprocessing data structures for parallelization.
    cpu_count = numprocs
    dt_dict = parallel_funcs.dtypes_dict
    with mp.Manager() as manager:

        # Create a multiprocessing shared-memory objects for photon heights, i, j. and codes.
        if output_vertical_datum in ("ellipsoid", "wgs84"):
            height_field = photon_df.h_ellipsoid
        elif output_vertical_datum in ("geoid", "egm2008"):
            height_field = photon_df.h_geoid
        elif output_vertical_datum == "meantide":
            height_field = photon_df.h_meantide
        else:
            raise ValueError("Should not have gotten here. Unhandled vdatum: {}".format(output_vertical_datum))

        height_array = manager.Array(dt_dict[height_field.dtype], height_field)
        i_array = manager.Array(dt_dict[photon_df.i.dtype], photon_df.i)
        j_array = manager.Array(dt_dict[photon_df.j.dtype], photon_df.j)
        code_array = manager.Array(dt_dict[photon_df.class_code.dtype], photon_df.class_code)

        if measure_coverage:
            x_array = manager.Array(dt_dict[photon_df.dem_x.dtype], photon_df.dem_x)
            y_array = manager.Array(dt_dict[photon_df.dem_y.dtype], photon_df.dem_y)
        else:
            x_array = None
            y_array = None

        running_procs     = [None] * cpu_count
        open_pipes_parent = [None] * cpu_count
        open_pipes_child  = [None] * cpu_count

        counter_started = 0 # The number of data cells handed off to child processes.
        counter_finished = 0 # The number of data cells completed by child processes.
        num_chunks_started = 0
        num_chunks_finished = 0
        items_per_process_chunk = 20

        # Set up the processes and pipes, then start them.
        try:
            # First, set up each child process and start it (importing arguments)
            for i in range(cpu_count):
                if counter_started >= N:
                    # DEBUG print statement
                    # print(counter_started, N, "Delegated all the data before finishing setting up processes, now will go on to process it.")
                    # Shorten the list of processes we are using.
                    running_procs     = running_procs[:i]
                    open_pipes_parent = open_pipes_parent[:i]
                    open_pipes_child  = open_pipes_child[:i]
                    break

                # Generate a new parallel subprocess to handle the data.
                running_procs[i], open_pipes_parent[i], open_pipes_child[i] = \
                    kick_off_new_child_process(height_array, i_array, j_array, code_array,
                                               photon_limit=max_photons_per_cell,
                                               measure_coverage=measure_coverage,
                                               input_x=x_array,
                                               input_y=y_array)

                # Send the first batch of (i,j) pixel locations & elevs to processes now.
                # Kick off the computations.
                counter_chunk_end = min(counter_started + items_per_process_chunk, N)
                if measure_coverage:
                    open_pipes_parent[i].send((dem_overlap_i[counter_started: counter_chunk_end],
                                               dem_overlap_j[counter_started: counter_chunk_end],
                                               dem_overlap_elevs[counter_started: counter_chunk_end],
                                               dem_overlap_xmin[counter_started: counter_chunk_end],
                                               dem_overlap_xmax[counter_started: counter_chunk_end],
                                               dem_overlap_ymin[counter_started: counter_chunk_end],
                                               dem_overlap_ymax[counter_started: counter_chunk_end]))
                else:
                    open_pipes_parent[i].send((dem_overlap_i[counter_started: counter_chunk_end],
                                               dem_overlap_j[counter_started: counter_chunk_end],
                                               dem_overlap_elevs[counter_started: counter_chunk_end]))
                counter_started = counter_chunk_end
                num_chunks_started += 1

            # Delegate the work. Keep looping through until all processes have finished up.
            # When everything is done, just send "STOP" connections to the remaining processes.
            # while counter_finished < N:
            while num_chunks_finished < num_chunks_started:
                # First, look for processes that have returned values.
                for i, (proc, pipe, pipe_child) in enumerate(zip(running_procs, open_pipes_parent, open_pipes_child)):

                    if proc is None:
                        continue

                    elif not proc.is_alive():
                        # If for some reason the child process has terminated (likely from an error), join it and kick off a new one.
                        if not quiet:
                            # raise UserWarning("Sub-process terminated unexpectedly. Some data may be missing. Restarting a new process.")
                            print("\nSub-process terminated unexpectedly. Some data may be missing. Restarting a new process.")
                        # Close out the dead process and its pipes
                        proc.join()
                        pipe.close()
                        pipe_child.close()
                        # Kick off a shiny new process
                        proc, pipe, pipe_child = kick_off_new_child_process(height_array, i_array, j_array, code_array,
                                                                            photon_limit=max_photons_per_cell,
                                                                            measure_coverage=measure_coverage,
                                                                            input_x=x_array,
                                                                            input_y=y_array)
                        # Put that process and pipes into the lists of active procs & pipes.
                        running_procs[i] = proc
                        open_pipes_parent[i] = pipe
                        open_pipes_child[i] = pipe_child

                        num_chunks_finished += 1

                    # Check to see if our receive pipe has any data sitting in it.
                    if pipe.poll():
                        # Get the data from the pipe.
                        chunk_result_df = pipe.recv()

                        # Advance the "finished" counter.
                        counter_finished += len(chunk_result_df)
                        num_chunks_finished += 1
                        results_dataframes_list.append(chunk_result_df)
                        if not quiet:
                            progress_bar.ProgressBar(counter_finished, N, suffix=("{0:>" +str(len(str(N))) + "d}/{1:d}").format(counter_finished, N))
                            # DEBUG statements
                            # print()
                            # print("chunk_results_df", chunk_result_df)
                            # print("num_chunks_finished", num_chunks_finished, "num_chunks_started", num_chunks_started)

                        # If we still have more data to process, send another chunk along.
                        if counter_started < N:
                            # Send a new task to the child process, consisting of the i,j pairs to process now.
                            counter_chunk_end = min(counter_started + items_per_process_chunk, N)
                            # DEBUG statement
                            # print("counter_started:", counter_started, "counter_chunk_end", counter_chunk_end)
                            if measure_coverage:
                                pipe.send((dem_overlap_i[counter_started: counter_chunk_end],
                                           dem_overlap_j[counter_started: counter_chunk_end],
                                           dem_overlap_elevs[counter_started: counter_chunk_end],
                                           dem_overlap_xmin[counter_started: counter_chunk_end],
                                           dem_overlap_xmax[counter_started: counter_chunk_end],
                                           dem_overlap_ymin[counter_started: counter_chunk_end],
                                           dem_overlap_ymax[counter_started: counter_chunk_end]))
                            else:
                                pipe.send((dem_overlap_i[counter_started: counter_chunk_end],
                                           dem_overlap_j[counter_started: counter_chunk_end],
                                           dem_overlap_elevs[counter_started: counter_chunk_end]))
                            # Increment the "started" counter. Let it run free on the data.
                            counter_started = counter_chunk_end
                            num_chunks_started += 1
                        else:
                            # Nothing more to send. Send a "STOP" command to the child proc.
                            if measure_coverage:
                                pipe.send(("STOP", None, None, None, None, None, None))
                            else:
                                pipe.send(("STOP", None, None))
                            proc.join()
                            pipe.close()
                            pipe_child.close()
                            running_procs[i] = None
                            open_pipes_parent[i] = None
                            open_pipes_child[i] = None

                    # else:
                        # DEBUG print statement
                        # print("Waiting on proc #", i)

        except Exception as e:
            if not quiet:
                print("\nException encountered in ICESat-2 processing loop. Exiting.")
            clean_procs_and_pipes(running_procs, open_pipes_parent, open_pipes_child)
            print(e)
            return None

    t_end = time.perf_counter()
    if not quiet:
        total_time_s = t_end - t_start
        total_time_m = 0
        # If there's 100 or more seconds, state the time with minutes.
        if total_time_s >= 100:
            total_time_m = int(total_time_s / 60)
            partial_time_s = total_time_s % 60
            print("{0:d} minute".format(total_time_m) + ("s" if total_time_m > 1 else "") + " {0:0.1f} seconds total, ({1:0.4f} s/iteration)".format(partial_time_s, ( (total_time_s/N) if N>0 else 0)))
        else:
            print("{0:0.1f} seconds total, ({1:0.4f} s/iteration)".format(total_time_s, ( (total_time_s/N) if N>0 else 0)))

    clean_procs_and_pipes(running_procs, open_pipes_parent, open_pipes_child)
    # Concatenate all the results dataframes
    # If there were no overlappying photons, then just return none.
    if len(results_dataframes_list) == 0:
        return None

    results_dataframe = pandas.concat(results_dataframes_list)
    # Subset for only valid results out. Eliminate useless nodata values.
    results_dataframe = results_dataframe[results_dataframe["mean"] != EMPTY_VAL].copy()
    if not quiet:
        print("{0:,} valid interdecile photon records in {1:,} DEM cells.".format(results_dataframe["numphotons_intd"].sum(), len(results_dataframe)))

    if len(results_dataframe) == 0:
        if not quiet:
            print("No valid results in results dataframe. No outputs computed.")
        if mark_empty_results:
            # Just create an empty file to makre this dataset as done.
            with open(empty_results_filename, 'w') as f:
                f.close()
            if not quiet:
                print("Created", empty_results_filename, "to indicate no data was returned here.")
        return

    else:
        # Write out the results dataframe. Method depends upon the file type. Can be .csv, .txt, .h5 (assumed default of not one of the text files.)
        base, ext = os.path.splitext(results_dataframe_file)
        ext = ext.lower().strip()
        if ext in (".txt", ".csv"):
            results_dataframe.to_csv(results_dataframe_file)
        else:
            results_dataframe.to_hdf(results_dataframe_file, "icesat2", complib="zlib", mode='w')

        if not quiet:
            print(results_dataframe_file, "written.")

    if write_summary_stats:
        write_summary_stats_file(results_dataframe,
                                 summary_stats_filename,
                                 verbose=not quiet)

    if write_result_tifs:
        if dem_ds is None:
            dem_ds = gdal.Open(dem_name, gdal.GA_ReadOnly)
        generate_result_geotiffs(results_dataframe,
                                 dem_ds,
                                 results_dataframe_file,
                                 result_tif_filename,
                                 verbose=not quiet)

    if plot_results:
        if location_name is None:
            location_name = os.path.split(dem_name)[1]

        plot_validation_results.plot_histogram_and_error_stats_4_panels(results_dataframe,
                                                                        plot_filename,
                                                                        place_name=location_name,
                                                                        verbose=not quiet)

    if delete_datafiles:
        del dem_ds
        if not quiet:
            print("Cleaning up...", end="")
        if os.path.exists(coastline_mask_filename):
            os.remove(coastline_mask_filename)
        if converted_dem_name != None & os.path.exists(converted_dem_name):
            os.remove(converted_dem_name)
        for granule_fname in atl03_granules_list:
            if os.path.exists(granule_fname): os.remove(granule_fname)
        for granule_fname in atl08_granules_list:
            if os.path.exists(granule_fname): os.remove(granule_fname)
        if os.path.exists(photon_dataframe_name):
            os.remove(photon_dataframe_name)

        if not quiet:
            print("done.")

    return

def write_summary_stats_file(results_df, statsfile_name, verbose=True):

    if len(results_df) == 0:
        if verbose:
            print("write_summary_stats_file(): No stats to compute in results dataframe. Returning")
        return

    lines = []
    lines.append("Number of DEM cells validated (cells): {0}".format(len(results_df)))
    lines.append("Total number of ground photons used to validate this DEM (photons): {0}".format(results_df["numphotons_intd"].sum()))
    lines.append("Mean number of photons used to validate each cell (photons): {0}".format(results_df["numphotons_intd"].mean()))

    mean_diff = results_df["diff_mean"]

    lines.append("Mean bias error (ICESat-2 - DEM) (m): {0}".format(mean_diff.mean()))
    lines.append("RMSE error (m): {0}".format(numpy.sqrt(numpy.mean(numpy.power(mean_diff, 2)))))
    lines.append("== Decile ranges of errors (ICESat-2 - DEM) (m) (Look for long-tails, indicating possible artifacts.) ===")

    percentile_levels = [0,1,10,20,30,40,50,60,70,80,90,99,100]
    percentile_values = numpy.percentile(mean_diff, percentile_levels)
    for l,v in zip(percentile_levels, percentile_values):
        lines.append("    {0:>3d}-th percentile error level (m): {1}".format(l, v))

    lines.append("Mean canopy cover (% cover): {0:0.02f}".format(results_df["canopy_fraction"].mean()*100))
    lines.append("% of cells with >0 measured canopy (%): {0}".format(len(results_df.canopy_fraction > 0.0) / len(results_df)))
    lines.append("Mean canopy cover in cells containing >0 canopy (% cover among 'wooded' cells): {0}".format(results_df[results_df["canopy_fraction"] > 0].mean()))
    lines.append("Mean roughness (std. dev. of photon elevations within each cell (m)): {0}".format(results_df["stddev"].mean()))

    out_text = "\n".join(lines)
    with open(statsfile_name, 'w') as outf:
        outf.write(out_text)

    if verbose:
        if os.path.exists(statsfile_name):
            print(statsfile_name, "written.")
        else:
            print(statsfile_name, "NOT written.")

    return

def generate_result_geotiffs(results_dataframe, dem_ds, results_dataframe_filename, result_tif_filename, verbose=True):
    """Given the results in the dataframe, output geotiffs to visualize these.

    Name the geotiffs after the dataframe: [original_filename]_<tag>.tif

    Geotiff tags will include:
        - mean_diff
        # TODO: FINISH THIS LIST
    """
    gt = dem_ds.GetGeoTransform()
    projection = dem_ds.GetProjection()
    xsize, ysize = dem_ds.RasterXSize, dem_ds.RasterYSize
    emptyval = float(EMPTY_VAL)
    result_array = numpy.zeros([ysize, xsize], dtype=float) + emptyval

    indices = results_dataframe.index.to_numpy()
    ivals = [idx[0] for idx in indices]
    jvals = [idx[1] for idx in indices]
    # Insert the valid values.
    result_array[ivals, jvals] = results_dataframe["diff_mean"]

    driver = gdal.GetDriverByName("GTiff")
    out_ds = driver.Create(result_tif_filename,
                           xsize=xsize,
                           ysize=ysize,
                           bands=1,
                           eType=gdal.GDT_Float32,
                           options= ["COMPRESS=DEFLATE", "PREDICTOR=2"])
    out_ds.SetProjection(projection) # Might need to add .ExportToWkt()
    out_ds.SetGeoTransform(gt)
    band = out_ds.GetRasterBand(1)
    band.WriteArray(result_array)
    band.SetNoDataValue(emptyval)
    band.GetStatistics(0,1)
    band = None
    out_ds = None
    if verbose:
        print(result_tif_filename, "written.")
    return


def read_and_parse_args():
    # Collect and process command-line arguments.
    parser = argparse.ArgumentParser(description='Use ICESat-2 photon data to validate a DEM and generate statistics.')
    parser.add_argument('input_dem', type=str,
                        help='The input DEM.')
    parser.add_argument('output_h5', type=str, nargs="?", default="",
                        help='A .h5 file to put the output summary results. Default: Will put in the same directory & filename as the input_dem, just with .h5 instead of .tif.')
    parser.add_argument('-photon_h5', type=str, default="",
                        help='The .h5 files where the ICESat-2 photon data resides. If this file exists (and --overwrite is not selected), this file will be read rather than generated. Otherwise, photon data will be put in here.')
    parser.add_argument('-input_vdatum','-ivd', type=str, default="wgs84",
                        help="Input DEM vertical datum. (Default: 'wgs84')" + \
                        " Currently supported datum arguments, not case-sensitive: ({})".format(",".join([str(vd) for vd in convert_vdatum.SUPPORTED_VDATUMS]))
                        )
    parser.add_argument('-output_vdatum','-ovd', type=str, default="wgs84",
                        help="Output vertical datum. (Default: 'wgs84')" + \
                        " Supports same datum list as input_vdatum, except for egm96 and equivalent.")
    parser.add_argument('-datadir', type=str, default="",
                        help="A scratch directory to write interim data files. Useful if user would like to save temp files elsewhere. Defaults to the output_h5 directory.")
    parser.add_argument('-interp_method', type=str, default="cubic",
                        help="Interpolation method passed to gdal_warp for vertical datum conversions. Default 'cubic'. Call 'gdal_warp -h' for complete list of options.")
    parser.add_argument('-band_num', type=int, default=1,
                        help="The band number (1-indexed) of the input_dem. (Default: 1)")
    parser.add_argument('-date_range', type=str, default="",
                        help='The date range in which to search for signal photons, comma-separated. Ex: 2021-01-01,2021-12-31 (Default)')
    parser.add_argument('-place_name', '-name', type=str, default=None,
                        help='A text name of the location, to put in the title of the plot (if --plot_results is selected)')
    parser.add_argument('--use_icesat2_photon_database', action='store_true', default=False,
                        help="Use the optimized ICESat-2 photon database rather than downloading granules separately. This can save time & memory if the database has already been built on this machine.")
    parser.add_argument('--delete_datafiles', action='store_true', default=False,
                        help='Delete the interim data files generated. Reduces storage requirements. (Default: keep them all.)')
    parser.add_argument('--use_urban_mask', action='store_true', default=False,
                        help="Use the WSL 'Urban Area' mask rather than OSM building footprints to mask out IceSat-2 data. Useful over lower-resolution (10m or coarser) dems, which tend to be bigger than building footprints.")
    parser.add_argument('--write_result_tifs', action='store_true', default=False,
                        help=""""Write output geotiff with the errors in cells that have ICESat-2 photons, NDVs elsewhere.""")
    parser.add_argument('--skip_icesat2_download', action="store_true", default=False,
                        help="Skip ICESat-2 granule downloads. Get existing granules files from -datadir. Usefuil if you've already downloaded the needed data from NSIDC.")
    parser.add_argument('--plot_results', action="store_true", default=False,
                        help="Make summary plots of the validation statistics.")
    parser.add_argument('--overwrite', action='store_true', default=False,
                        help='Overwrite all interim and output files, even if they already exist. Default: Use interim files to compute results, saving time.')
    parser.add_argument('--quiet', action='store_true', default=False,
                        help='Suppress output messaging, including error messages (just fail quietly without errors, return status 1).')

    return parser.parse_args()


# def args_from_script():
#     """For running from an editor, just provide the args manually here."""
#     args = argparse.Namespace
#     args.input_dem = "../../"

if __name__ == "__main__":
    args = read_and_parse_args()

    # Create photon .h5 filename if not provided.
    if args.datadir == "":
        args.datadir = os.path.split(args.output_h5)[0]

    if args.photon_h5 == "":
        base = os.path.splitext(os.path.split(args.output_h5)[1])[0]
        args.photon_h5 = os.path.join(args.datadir, base + "_photons.h5")

    if (args.date_range == "") or (args.date_range == None):
        icesat2_date_range = ["2021-01-01", "2021-12-31"]
    else:
        assert type(args.date_range) == str
        icesat2_date_range = args.date_range.split(",")

    if ((args.output_h5 == "") or (args.output_h5 == None)):
        if ((args.datadir != "") and (args.datadir != None)):
            base, fname = os.path.split(args.input_dem)
            output_h5 = os.path.join(args.datadir, os.path.splitext(fname)[0] + "_results.h5")
        else:
            output_h5 = os.path.splitext(args.input_dem)[0] + "_results.h5"
    else:
        output_h5 = args.output_h5

    kwargs = {}

    validate_dem_parallel(args.input_dem,
                          args.photon_h5,
                          use_icesat2_photon_database = args.use_icesat2_photon_database,
                          dem_vertical_datum = args.input_vdatum,
                          output_vertical_datum = args.output_vdatum,
                          results_dataframe_file = output_h5,
                          icesat2_date_range = icesat2_date_range,
                          interim_data_dir = (None if (args.datadir=="") else args.datadir),
                          overwrite=args.overwrite,
                          delete_datafiles=args.delete_datafiles,
                          write_result_tifs = args.write_result_tifs,
                          skip_icesat2_download = args.skip_icesat2_download,
                          plot_results = args.plot_results,
                          location_name = args.place_name,
                          mask_out_buildings=not args.use_urban_mask,
                          mask_out_urban=args.use_urban_mask,
                          quiet=args.quiet)
    # # fname = '/home/mmacferrin/Research/DATA/CopernicusDEM/data/30m/COP30_hh/Copernicus_DSM_COG_10_N26_00_W079_00_DEM.tif'
    # # output_file = "../data/freeport_coastline"
    # # fname = '/home/mmacferrin/Research/DATA/WorldView Images/Bahamas_Freeport/WV01_20080108_10200100014B9500_10200100015DC600/WV01_20080108_10200100014B9500_10200100015DC600_2m_lsf_seg1_dem.tif'
    # # output_file = "../data/freeport_WV01"
    # # create_coastline_mask(fname, output_file)
    # subprocess.call(["python","freeport_bahamas_code.py"])
