#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun 22 16:06:21 2021

@author: mmacferrin
"""


try:
    # We don't actually use the cudem modules here, but we make command-line
    # calls to "waffles", so check here to make sure cudem is installed on this
    # machine. If we can import the waffles module, we can use it from the command-line.
    from cudem import waffles
except:
    raise ModuleNotFoundError("Module 'cudem/waffles.py' required. Update paths, or refer to https://github.com/ciresdem/cudem for installation instructions.")
# EMPTY_VAL = -9999

import argparse
import ast
import multiprocessing as mp
import multiprocessing.shared_memory as shared_memory
import numexpr
import numpy
from osgeo import gdal, osr
import os
import pandas
import pyproj
import re
import signal
import subprocess
import time
import typing

import utils.progress_bar as progress_bar
import utils.parallel_funcs as parallel_funcs
import utils.configfile
import utils.pickle_blosc
import utils.split_dem
import convert_vdatum
import coastline_mask
import plot_validation_results
import jobs_database
import server_file_export
import icesat2_photon_database
import find_bad_icesat2_granules
import ivert_server_job_manager

# # Just for debugging memory issues. TODO: REmove later.
# import psutil
# import utils.sizeof_format as sizeof


# NOTE: This eliminates a Deprecation error in GDAL v3.x. In GDAL 4.0, they will use Exceptions by default and this
# command will be unnecessary.
gdal.UseExceptions()
osr.UseExceptions()

ivert_config = utils.configfile.config()
EMPTY_VAL = ivert_config.dem_default_ndv


def read_dataframe_file(df_filename: str) -> pandas.DataFrame:
    """Read a dataframe file, either from a picklefile, HDF, CSV, or feather.

    (Can handle other formats by adding more "elif ..." statements in the function.)
    """
    assert os.path.exists(df_filename)
    ext = os.path.splitext(df_filename)[1]
    ext = ext.lower()
    if ext == ".pickle":
        dataframe = pandas.read_pickle(df_filename)
    elif ext in (".h5", ".hdf"):
        dataframe = pandas.read_hdf(df_filename, mode="r")
    elif ext in (".csv", ".txt"):
        dataframe = pandas.read_csv(df_filename)
    elif ext == ".feather":
        dataframe = pandas.read_feather(df_filename)
    elif ext == ".blosc2":
        dataframe = utils.pickle_blosc.read(df_filename)
    else:
        raise NotImplementedError(f"ERROR: Unknown dataframe file extension '{ext}'. (Currently supporting .pickle, .h5, .hdf, .csv, .txt, .feather, or .blosc2)")

    return dataframe


def validate_dem_child_process(height_array_name,
                               height_dtype,
                               i_array_name,
                               i_dtype,
                               j_array_name,
                               j_dtype,
                               code_array_name,
                               code_dtype,
                               array_shape,
                               connection,
                               photon_limit=None,
                               measure_coverage=False,
                               x_array_name=None,
                               x_dtype=None,
                               y_array_name=None,
                               y_dtype=None,
                               num_subdivisions=15):
    """A child process for running the DEM validation in parallel.

    It takes the input_height (m) and the dem_indices (flattened), as well
    as a duplexed multiprocessing.connection.Connection object (i.e. an open pipe)
    for processing it. It reads the arrays into local memory, then uses the connection
    to pass data back and forth until getting a "STOP" command over the connection.

    'measure_coverage' is a boolean parameter to measure how well a given pixel is covered by ICESat-2 photons.
    We'll measure a couple of different measures (centrality and coverage), and insert those parameters in the output."""

    # Define shared memory arrays here.
    h_shm = shared_memory.SharedMemory(name=height_array_name)
    heights = numpy.ndarray(array_shape, dtype=height_dtype, buffer=h_shm.buf)

    pi_shm = shared_memory.SharedMemory(name=i_array_name)
    photon_i = numpy.ndarray(array_shape, dtype=i_dtype, buffer=pi_shm.buf)

    pj_shm = shared_memory.SharedMemory(name=j_array_name)
    photon_j = numpy.ndarray(array_shape, dtype=j_dtype, buffer=pj_shm.buf)

    pc_shm = shared_memory.SharedMemory(name=code_array_name)
    ph_codes = numpy.ndarray(array_shape, dtype=code_dtype, buffer=pc_shm.buf)

    if measure_coverage:
        x_shm = shared_memory.SharedMemory(name=x_array_name)
        ph_x = numpy.ndarray(array_shape, dtype=x_dtype, buffer=x_shm.buf)

        y_shm = shared_memory.SharedMemory(name=y_array_name)
        ph_y = numpy.ndarray(array_shape, dtype=y_dtype, buffer=y_shm.buf)
    else:
        x_shm = None
        y_shm = None
        ph_x = None
        ph_y = None

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

                cell_xmin_list = None
                cell_ymin_list = None
                cell_xmax_list = None
                cell_ymax_list = None

            # Upon the "STOP" mesage, break the loop, close the shared memory objects, and return.
            if (type(dem_i_list) is str) and (dem_i_list == "STOP"):
                h_shm.close()
                pi_shm.close()
                pj_shm.close()
                pc_shm.close()
                if measure_coverage:
                    x_shm.close()
                    y_shm.close()
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
                r_coverage_frac = numpy.zeros((N,), dtype=float)
            else:
                r_coverage_frac = None

            for counter, (i, j) in enumerate(zip(dem_i_list, dem_j_list)):
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

    raise RuntimeError("Something went wrong in dem_validate child process. Should not get here.")


def clean_procs_and_pipes(procs, pipes1, pipes2, memory_objs):
    """Join all processes and close all pipes.

    Useful for cleaning up after multiprocessing."""
    # Close up all processes.
    for pr in procs:
        if isinstance(pr, mp.Process):
            if pr.is_alive():
                pr.kill()
            pr.join()

    # Close all pipes.
    for p1 in pipes1:
        if isinstance(p1, mp.connection.Connection):
            p1.close()
    for p2 in pipes2:
        if isinstance(p2, mp.connection.Connection):
            p2.close()

    # Clean up shared memory objoects.
    for smo in memory_objs:
        smo.close()
        try:
            smo.unlink()
        except FileNotFoundError:
            pass

    return


def kick_off_new_child_process(height_array_name,
                               height_dtype,
                               i_array_name,
                               i_dtype,
                               j_array_name,
                               j_dtype,
                               code_array_name,
                               code_dtype,
                               array_shape,
                               photon_limit=None,
                               measure_coverage=False,
                               x_array_name=None,
                               x_dtype=None,
                               y_array_name=None,
                               y_dtype=None,
                               num_subdivisions=15):
    """Start a new subprocess to handle and process data."""
    pipe_parent, pipe_child = mp.Pipe(duplex=True)
    proc = mp.Process(target=validate_dem_child_process,
                      args=(height_array_name,
                            height_dtype,
                            i_array_name,
                            i_dtype,
                            j_array_name,
                            j_dtype,
                            code_array_name,
                            code_dtype,
                            array_shape,
                            pipe_child),
                      kwargs={"measure_coverage": measure_coverage,
                              "x_array_name": x_array_name,
                              "x_dtype": x_dtype,
                              "y_array_name": y_array_name,
                              "y_dtype": y_dtype,
                              "photon_limit": photon_limit,
                              "num_subdivisions": num_subdivisions}
                      )
    proc.start()
    return proc, pipe_parent, pipe_child


def subdivide_dem(dem_name: str,
                  factor: int = 2,
                  output_dir: typing.Union[str, None] = None,
                  verbose: bool = False) -> list[str]:
    """Split a DEM into 4 smaller parts.

    Also attempt to subdivide its coastline mask if it already exists."""

    # First make sure the dem exists, and check to see if the coastline mask exists.
    if not os.path.exists(dem_name):
        raise FileNotFoundError(f"DEM {dem_name} does not exist.")

    sub_dems = utils.split_dem.split(dem_name, factor=factor, output_dir=output_dir, verbose=verbose)

    # If the coastline mask exists, sub-divide that into 4 parts too.
    # Don't need to keep track of the coastline files, it'll be automatically detected as existing by validate_dem_parallel.
    dem_base, ext = os.path.splitext(dem_name)
    cmask_name = dem_base + "_coastline_mask" + ext

    if not os.path.exists(cmask_name):
        if os.path.exists(os.path.join(output_dir, cmask_name)):
            cmask_name = os.path.join(output_dir, cmask_name)

    if os.path.exists(cmask_name):
        cmask_names = utils.split_dem.split(cmask_name, factor=factor, verbose=verbose)
        # Gotta change the filenames though. "_coastline_mask" should be at the end.
        for cname in cmask_names:
            cdir, cbase = os.path.split(cname)
            cbase, cext = os.path.splitext(cbase)
            new_cbase = cbase.replace("_coastline_mask", "") + "_coastline_mask" + cext
            new_cname = os.path.join(cdir, new_cbase)
            if verbose:
                print(os.path.basename(cname), "->", os.path.basename(new_cname))
            os.rename(cname, new_cname)

    return sub_dems

def reset_results_indexes_after_merge(sub_results_df: pandas.DataFrame,
                                      sub_dem_fname: str,
                                      parent_dem_fname: str) -> pandas.DataFrame:
    """DEM results dataframes are indexed by (i, j).  Reset the index after merging."""

    sub_geotransform = gdal.Open(sub_dem_fname).GetGeoTransform()
    parent_geotransform = gdal.Open(parent_dem_fname).GetGeoTransform()

    x_step = sub_geotransform[1]
    y_step = sub_geotransform[5]
    assert x_step == parent_geotransform[1]
    assert y_step == parent_geotransform[5]

    x_offset = int((sub_geotransform[0] - parent_geotransform[0]) / x_step)
    y_offset = int((sub_geotransform[3] - parent_geotransform[3]) / y_step)

    # Assign a dem (i,j) index location for each photon. We use this for computing.
    sub_results_df["i"] = sub_results_df.index.get_level_values("i") + y_offset
    sub_results_df["j"] = sub_results_df.index.get_level_values("j") + x_offset

    # Re-create an (i,j) multi-index into the array.
    sub_results_df.set_index(["i", "j"], drop=True, inplace=True)

    return sub_results_df



def validate_dem(dem_name: str,
                 output_dir: typing.Union[str, None] = None,
                 shared_ret_values: typing.Union[dict, None] = None,
                 icesat2_photon_database_obj: typing.Union[icesat2_photon_database.ICESat2_Database, None] \
                         = None,  # Used only if we've already created this, for efficiency.
                 band_num: int = 1,
                 dem_vertical_datum: typing.Union[str, int] = "egm2008",
                 output_vertical_datum: typing.Union[str, int] = "egm2008",
                 ivert_job_name: typing.Union[str, None] = None,
                 interim_data_dir: typing.Union[str, None] = None,
                 overwrite: bool = False,
                 delete_datafiles: bool = False,
                 mask_out_lakes: bool = True,
                 mask_out_buildings: bool = True,
                 use_osm_planet: bool = False,
                 mask_out_urban: bool = False,
                 include_gmrt_mask: bool = False,
                 write_result_tifs: bool = True,
                 write_summary_stats: bool = True,
                 export_coastline_mask: bool = True,
                 outliers_sd_threshold: float = 2.5,
                 include_photon_level_validation: bool = False,
                 plot_results: bool = True,
                 location_name: typing.Union[str, None] = None,
                 mark_empty_results: bool = True,
                 omit_bad_granules: bool = True,
                 measure_coverage: bool = False,
                 max_photons_per_cell: typing.Union[int, None] = None,
                 numprocs: int = parallel_funcs.physical_cpu_count(),
                 max_subdivides: int = 4,
                 subdivision_number: int = 0,
                 orig_dem_name: typing.Union[str, None] = None,
                 verbose: bool = True):
    """Validate a DEM and produce output results.

    Most of this work is done in validate_dem_parallel. This function is a wrapper that calls validate_dem_parallel as
    a sub-function and tests whether it dies because of RAM limitations. If that happens, sub-divide the DEM in quarters
    and re-try, to a max recursion depth of max_subdivides.

    Args:
        dem_name (str): Name of the DEM file to validate.
        output_dir (str): Output directory for results.
        shared_ret_values (dict): Shared return values from validate_dem_parallel.
        icesat2_photon_database_obj (icesat2_photon_database.ICESat2_Database): icesat-2 photon database object. Only
            used if we've already created one, such as in validate_dem_collection. Typically ignored for a single DEM.
        ivert_job_name (str): The name of an ivert_server job to update file status and export results.
        interim_data_dir (str): Output directory for intermediate data. Defaults to the same as the output_dir.
        overwrite (bool): Overwrite existing files.
        delete_datafiles (bool): Delete intermediate data files after validation is complete.
        mask_out_lakes (bool): Mask out lakes using NHD and HydroLakes.
        mask_out_buildings (bool): Mask out buildings using OpenStreetMap.
        use_osm_planet (bool): Use Open Street Map "whole planet" data. Typically ignored.
        mask_out_urban (bool): Mask out urban areas using World Settlement Footprint (WSF).
        include_gmrt_mask (bool): Include GMRT mask for coastline masking (typically not used).
        write_result_tifs (bool): Write result geotifs with ICESat-2 derived errors.
        write_summary_stats (bool): Write summary statistics of results to a textfile.
        export_coastline_mask (bool): Export a geotiff of the coastline mask along with results.
        outliers_sd_threshold (float): Threshold for outlier detection in errors. Defaults to 2.5.
        include_photon_level_validation (bool): Include photon level validation (not just cell-level validation).
        plot_results (bool): Plot results.
        location_name (str): Name of the location being validated.
        mark_empty_results (bool): Mark results that are empty in an "_EMPTY.txt" file.
        omit_bad_granules (bool): Check for 'bad_granules' in the photon database, and exclude them before validation.
        measure_coverage (bool): Measure the coverage of ICESat-2 photons within each grid-cell.
        max_photons_per_cell (int): Maximum number of photons per cell.
        numprocs (int): Number of processes to use for parallelized validation.
        max_subdivides (int): Maximum number of times to subdivide the DEM in quarters before giving up.
        subdivision_number (int): The current recursion depth of this subdivision. Will not subdivide further if
            subdivision_number == max_subdivides.
        orig_dem_name (str): Name of the original DEM file. Only used for error messages.
        verbose (bool): Be verbose.
    """
    if ivert_job_name:
        ivert_jobs_db = jobs_database.JobsDatabaseServer() # Jobs database object.
        ivert_exporter = server_file_export.IvertExporter(s3_manager=None, jobs_db=ivert_jobs_db)  # Exporter object
        ivert_username = server_file_export.get_username(ivert_job_name)
        ivert_job_id = server_file_export.get_job_id(ivert_job_name)

        ivert_jobs_db.update_file_status(ivert_username, ivert_job_id, os.path.basename(dem_name), "processing",
                                         upload_to_s3=True)
    else:
        ivert_jobs_db = None
        ivert_exporter = None
        ivert_username = None
        ivert_job_id = None

    if shared_ret_values is None:
        shared_ret_values = {}

    manager = mp.Manager()
    sub_shared_ret_values = manager.dict()

    subproc = mp.Process(target=validate_dem_parallel,
                         args=(dem_name,),
                         kwargs={'output_dir': output_dir,
                                 'shared_ret_values': sub_shared_ret_values,
                                 'icesat2_photon_database_obj': icesat2_photon_database_obj,
                                 'band_num': band_num,
                                 'dem_vertical_datum': dem_vertical_datum,
                                 'output_vertical_datum': output_vertical_datum,
                                 'ivert_job_name': ivert_job_name,
                                 'interim_data_dir': interim_data_dir,
                                 'overwrite': overwrite,
                                 'delete_datafiles': delete_datafiles,
                                 'mask_out_lakes': mask_out_lakes,
                                 'mask_out_buildings': mask_out_buildings,
                                 'use_osm_planet': use_osm_planet,
                                 'mask_out_urban': mask_out_urban,
                                 'include_gmrt_mask': include_gmrt_mask,
                                 'write_result_tifs': write_result_tifs,
                                 'write_summary_stats': write_summary_stats,
                                 'export_coastline_mask': export_coastline_mask,
                                 'outliers_sd_threshold': outliers_sd_threshold,
                                 'include_photon_level_validation': include_photon_level_validation,
                                 'plot_results': plot_results,
                                 'location_name': location_name,
                                 'mark_empty_results': mark_empty_results,
                                 'omit_bad_granules': omit_bad_granules,
                                 'measure_coverage': measure_coverage,
                                 'max_photons_per_cell': max_photons_per_cell,
                                 'numprocs': numprocs,
                                 'verbose': verbose},
                         )

    subproc.start()
    # while subproc.is_alive():
    #     time.sleep(0.1)
    subproc.join(timeout=None)
    exitcode = subproc.exitcode
    subproc.close()

    if orig_dem_name is None:
        orig_dem_name = dem_name

    if exitcode == 0:
        shared_ret_values.update(sub_shared_ret_values)

        if ivert_job_name is not None and subdivision_number == 0:
            ivert_jobs_db.update_file_status(ivert_username, ivert_job_id,
                                             os.path.basename(dem_name), "processed", upload_to_s3=False)

        return list(shared_ret_values.values())

    elif exitcode == -signal.SIGKILL:
        # The job was killed by the operating system. This happens with a Memory Error. Divvy the file up and try again.

        # Unless we've already hit max recursion. In that case, error-out.
        if subdivision_number == max_subdivides:
            if ivert_job_name is not None and subdivision_number == 0:
                ivert_jobs_db.update_file_status(ivert_username, ivert_job_id,
                                                 os.path.basename(orig_dem_name), "error", upload_to_s3=False)

            raise MemoryError(f"validate_dem.validate_dem('{orig_dem_name}', ...) was terminated, likely due to a memory error.")

        # Make sure the DEM exists that we're trying to sub-divide
        if not os.path.exists(dem_name):
            if ivert_job_name is not None and subdivision_number == 0:
                ivert_jobs_db.update_file_status(ivert_username, ivert_job_id,
                                                 os.path.basename(orig_dem_name), "error", upload_to_s3=False)

            raise FileNotFoundError(f"validate_dem.validate_dem_parallell({orig_dem_name},...) could not find {dem_name}.")

        # Split up the DEM into 4 parts.
        sub_dem_names = subdivide_dem(dem_name, factor=2, output_dir=output_dir, verbose=verbose)

        sub_shared_ret_values = [manager.dict() for i in range(len(sub_dem_names))]
        assert len(sub_shared_ret_values) == len(sub_dem_names) == 4

        # Pre-read the photon database. This is easier than reading it in 4 separate times.
        if icesat2_photon_database_obj is None:
            icesat2_photon_database_obj = icesat2_photon_database.ICESat2_Database()
            icesat2_photon_database_obj.get_gdf(verbose=verbose)

        for sub_dem_name, sub_shared_ret_dict in zip(sub_dem_names, sub_shared_ret_values):
            validate_dem(sub_dem_name,
                         output_dir=output_dir,
                         shared_ret_values=sub_shared_ret_dict,
                         icesat2_photon_database_obj=icesat2_photon_database_obj,
                         band_num=band_num,
                         dem_vertical_datum=dem_vertical_datum,
                         output_vertical_datum=output_vertical_datum,
                         ivert_job_name=None, # Don't use an ivert_job_name to export sub-results.
                         interim_data_dir=interim_data_dir,
                         overwrite=overwrite,
                         delete_datafiles=delete_datafiles,
                         mask_out_lakes=mask_out_lakes,
                         mask_out_buildings=mask_out_buildings,
                         use_osm_planet=use_osm_planet,
                         mask_out_urban=mask_out_urban,
                         include_gmrt_mask=include_gmrt_mask,
                         write_result_tifs=False, # No need to write the results tifs for subsets.
                         write_summary_stats=False, # Not need to write the summary stats file for subsets.
                         export_coastline_mask=export_coastline_mask,
                         outliers_sd_threshold=None, # Don't filter outliers until we get all the results back.
                         include_photon_level_validation=include_photon_level_validation,
                         plot_results=False, # Don't bother plotting the sub-results.
                         location_name=location_name,
                         mark_empty_results=mark_empty_results,
                         omit_bad_granules=omit_bad_granules,
                         measure_coverage=measure_coverage,
                         max_photons_per_cell=max_photons_per_cell,
                         numprocs=numprocs,
                         verbose=verbose,
                         max_subdivides=max_subdivides,
                         orig_dem_name=orig_dem_name,
                         subdivision_number=subdivision_number + 1)

        # Now we gotta merge all the results.
        # Get a set of common keys:
        common_keys = set(list(sub_shared_ret_values[0].keys()) + list(sub_shared_ret_values[1].keys())
                          + list(sub_shared_ret_values[2].keys()) + list(sub_shared_ret_values[3].keys()))

        shared_results_df = None

        # First, merge the results dataframes.
        if "results_dataframe_file" in common_keys:
            common_key = "results_dataframe_file"
            all_fnames = [sub_shared_ret_values[i][common_key] for i in range(len(sub_shared_ret_values)) if
                          common_key in sub_shared_ret_values[i]]
            # Concatenate the results dataframes.
            output_dfs = []
            for fname in all_fnames:
                dem_results_df = pandas.read_hdf(fname)
                # Now I gotta reset the i,j indexes.
                sub_dem_name = fname.replace("_results.h5", ".tif")
                parent_dem_name = dem_name
                dem_results_df = reset_results_indexes_after_merge(dem_results_df, sub_dem_name, parent_dem_name)
                output_dfs.append(dem_results_df)

            shared_results_df = pandas.concat(output_dfs, ignore_index=False, axis=0)
            # After we've combined all the resutls, *then* filter out outliers if they exist.
            if outliers_sd_threshold is not None:
                assert type(outliers_sd_threshold) in (int, float)
                diff_mean = shared_results_df["diff_mean"]
                meanval, stdval = diff_mean.mean(), diff_mean.std()
                low_cutoff = meanval - (stdval * outliers_sd_threshold)
                hi_cutoff = meanval + (stdval * outliers_sd_threshold)
                valid_mask = (diff_mean >= low_cutoff) & (diff_mean <= hi_cutoff)
                shared_results_df = shared_results_df[valid_mask].copy()
                if verbose:
                    print("{0:,} DEM cells after removing outliers.".format(len(shared_results_df)))

            output_fname = os.path.join(output_dir, os.path.splitext(os.path.basename(dem_name))[0] + "_results.h5")
            shared_results_df.to_hdf(output_fname, key="icesat2", complib="zlib", mode='w')

            shared_ret_values[common_key] = output_fname
            if ivert_job_name is not None and subdivision_number == 0:
                ivert_exporter.upload_file_to_export_bucket(ivert_job_name, output_fname, upload_to_s3=False)

            if verbose:
                print(os.path.basename(output_fname), "written and exported.")

        # Second, create the results geotiff from the dataframe.
        if write_result_tifs and (shared_results_df is not None) and subdivision_number == 0:
            common_key = "result_tif_filename"
            output_fname = os.path.join(output_dir, os.path.splitext(os.path.basename(dem_name))[0] + "_ICESat2_error_map.tif")
            generate_result_geotiff(shared_results_df, gdal.Open(dem_name, gdal.GA_ReadOnly),
                                    output_fname, verbose=verbose)

            shared_ret_values[common_key] = output_fname
            if ivert_job_name is not None and subdivision_number == 0:
                ivert_exporter.upload_file_to_export_bucket(ivert_job_name, output_fname, upload_to_s3=False)

        # Merge the coastline masks, only if it needs to be exported and the parent coastline mask hasn't already been created.
        output_coastline_fname = os.path.join(output_dir,
                                              os.path.splitext(os.path.basename(dem_name))[0] + "_coastline_mask.tif")

        # Third, merge the coastline masks if they were called to be returned.
        # This first block probably won't be entered because the coastline mask was already created, but in case not,
        # re-create it.
        if (("coastline_mask_filename" in common_keys)
                and export_coastline_mask
                and not os.path.exists(output_coastline_fname)):
            common_key = "coastline_mask_filename"
            all_fnames = [sub_shared_ret_values[i][common_key] for i in range(len(sub_shared_ret_values)) if
                          common_key in sub_shared_ret_values[i]]
            gdal_cmd = ["gdal_merge.py",
                        "-of", "GTiff",
                        "-o", output_coastline_fname,
                        "-co", "COMPRESS=DEFLATE",
                        "-co", "PREDICTOR=2",
                        "-co", "TILED=YES"] + all_fnames

            if verbose:
                print(" ".join(gdal_cmd))

            subprocess.run(gdal_cmd, capture_output=not verbose, check=True)

        # Export the coastline mask if it was called to be returned.
        if export_coastline_mask:
            shared_ret_values["coastline_mask_filename"] = output_coastline_fname
            if ivert_job_name is not None and subdivision_number == 0:
                ivert_exporter.upload_file_to_export_bucket(ivert_job_name, output_coastline_fname, upload_to_s3=False)

        # If we're doing an empty results file, create one in the output directory if no results were returned.
        if mark_empty_results and (shared_results_df is None) and subdivision_number == 0:
            # If any of the results existed, we don't need to do this just because one sub-result doesn't exist.
            empty_fname = os.path.join(output_dir, os.path.splitext(os.path.basename(dem_name))[0] + "_EMPTY.txt")
            with open(empty_fname, "w") as f:
                f.write(os.path.basename(dem_name) + f" had no IVERT results.")
            shared_ret_values["empty_results_filename"] = empty_fname
            if ivert_job_name is not None and subdivision_number == 0:
                ivert_exporter.upload_file_to_export_bucket(ivert_job_name, empty_fname, upload_to_s3=False)

        # Create the overall summary stats text file.
        if write_summary_stats and shared_results_df is not None and subdivision_number == 0:
            # Generate a new summary stats file only if we have results and if the recursion depth is zero.
            output_fname = os.path.join(output_dir,
                                        os.path.splitext(os.path.basename(dem_name))[0] + "_summary_stats.txt")
            write_summary_stats_file(shared_results_df, output_fname, verbose=verbose)
            shared_ret_values["summary_stats_filename"] = output_fname
            if ivert_job_name is not None and subdivision_number == 0:
                ivert_exporter.upload_file_to_export_bucket(ivert_job_name, output_fname, upload_to_s3=False)

        # Export the photon dataframe if it was called to be returned.
        if "photon_results_dataframe_file" in common_keys:
            common_key = "photon_results_dataframe_file"
            all_fnames = [sub_shared_ret_values[i][common_key] for i in range(len(sub_shared_ret_values)) if
                          common_key in sub_shared_ret_values[i]]
            results_df = pandas.concat([pandas.read_hdf(fname) for fname in all_fnames], ignore_index=True, axis=0)
            output_fname = os.path.join(output_dir, os.path.splitext(os.path.basename(dem_name))[0] + "_photons.h5")
            results_df.to_hdf(output_fname, key="icesat2", complib="zlib", mode='w')
            shared_ret_values[common_key] = output_fname
            if ivert_job_name is not None and subdivision_number == 0:
                ivert_exporter.upload_file_to_export_bucket(ivert_job_name, output_fname, upload_to_s3=False)

        # Plot the results.
        if plot_results and (shared_results_df is not None) and (subdivision_number == 0):
            output_fname = os.path.join(output_dir, os.path.splitext(os.path.basename(dem_name))[0] + "_plot.png")
            plot_validation_results.plot_histogram_and_error_stats_4_panels(shared_results_df,
                                                                            output_fname,
                                                                            place_name=location_name,
                                                                            verbose=verbose)
            shared_ret_values["plot_filename"] = output_fname
            if ivert_job_name is not None and subdivision_number == 0:
                ivert_exporter.upload_file_to_export_bucket(ivert_job_name, output_fname, upload_to_s3=False)

        # Update the job status.
        if ivert_job_name is not None and subdivision_number == 0:
            ivert_jobs_db.update_file_status(ivert_username, ivert_job_id, os.path.basename(orig_dem_name),
                                             "processed", upload_to_s3=True)

        return list(shared_ret_values.values())

    else:
        raise RuntimeError(f"validate_dem.validate_dem({orig_dem_name},...) exited with exitcode {exitcode}.")


def validate_dem_parallel(dem_name: str,
                          output_dir: typing.Union[str, None] = None,
                          shared_ret_values: typing.Union[dict, None] = None,
                          icesat2_photon_database_obj: typing.Union[icesat2_photon_database.ICESat2_Database, None] \
                                                      = None, # Used only if we've already created this, for efficiency.
                          band_num: int = 1,
                          dem_vertical_datum: typing.Union[str, int] = "egm2008",
                          output_vertical_datum: typing.Union[str, int] = "egm2008",
                          ivert_job_name: typing.Union[str, None] = None,
                          interim_data_dir: typing.Union[str, None] = None,
                          overwrite: bool = False,
                          delete_datafiles: bool = False,
                          mask_out_lakes: bool = True,
                          mask_out_buildings: bool = True,
                          use_osm_planet: bool = False,
                          mask_out_urban: bool = False,
                          include_gmrt_mask: bool = False,
                          write_result_tifs: bool = True,
                          write_summary_stats: bool = True,
                          export_coastline_mask: bool = True,
                          outliers_sd_threshold: float = 2.5,
                          include_photon_level_validation: bool = False,
                          plot_results: bool = True,
                          location_name: typing.Union[str, None] = None,
                          mark_empty_results: bool = True,
                          omit_bad_granules: bool = True,
                          measure_coverage: bool = False,
                          max_photons_per_cell: typing.Union[int, None] = None,
                          numprocs: int = parallel_funcs.physical_cpu_count(),
                          verbose: bool = True):
    """Validate a single DEM.

    Parameters are described above in the vdalite_dem() docstring."""
    # If we don't have the input dem, raise an error.
    if not os.path.exists(dem_name):
        file_not_found_msg = f"Could not find file {dem_name}."
        raise FileNotFoundError(file_not_found_msg)

    if ivert_job_name:
        ivert_exporter = server_file_export.IvertExporter()
    else:
        ivert_exporter = None

    if shared_ret_values is None:
        shared_ret_values = {}

    # Just get this variable defined so all the code-branches can use it.
    dem_ds = None

    if not output_dir:
        output_dir = os.path.dirname(os.path.abspath(dem_name))
    if not os.path.exists(output_dir):
        if verbose:
            print("Creating output directory", output_dir)
        os.makedirs(output_dir)

    # Get the results dataframe filename
    results_dataframe_file = os.path.join(output_dir, os.path.splitext(os.path.basename(dem_name))[0] + "_results.h5")

    # Get the interim data directory (if not already set)
    if interim_data_dir is None:
        interim_data_dir = output_dir
    if not os.path.exists(interim_data_dir):
        if verbose:
            print("Creating interim data directory", interim_data_dir)
        os.makedirs(interim_data_dir)

    empty_results_filename = ""
    if mark_empty_results:
        base, ext = os.path.splitext(results_dataframe_file)
        empty_results_filename = base + "_EMPTY.txt"

    summary_stats_filename = ""
    if write_summary_stats:
        summary_stats_filename = re.sub(r"_results\.h5\Z", "_summary_stats.txt", results_dataframe_file)

    result_tif_filename = ""
    if write_result_tifs:
        result_tif_filename = re.sub(r"_results\.h5\Z", "_ICESat2_error_map.tif", results_dataframe_file)

    plot_filename = ""
    if plot_results:
        plot_filename = re.sub(r"_results\.h5\Z", "_plot.png", results_dataframe_file)

    coastline_mask_filename = os.path.join(output_dir if export_coastline_mask else interim_data_dir,
        re.sub(r"_results\.h5\Z", "_coastline_mask.tif", os.path.basename(results_dataframe_file)))

    files_to_export = []

    if overwrite:
        if os.path.exists(results_dataframe_file):
            os.remove(results_dataframe_file)

        if os.path.exists(summary_stats_filename):
            os.remove(summary_stats_filename)

        if os.path.exists(result_tif_filename):
            os.remove(result_tif_filename)

        if os.path.exists(plot_filename):
            os.remove(plot_filename)

        if os.path.exists(coastline_mask_filename):
            os.remove(coastline_mask_filename)

    elif os.path.exists(results_dataframe_file):
        results_dataframe = None
        if ivert_job_name is not None:
            ivert_exporter.upload_file_to_export_bucket(ivert_job_name, results_dataframe_file)
        files_to_export.append(results_dataframe_file)
        shared_ret_values["results_dataframe_file"] = results_dataframe_file

        if write_summary_stats:
            if not os.path.exists(summary_stats_filename):
                if results_dataframe is None:
                    if verbose:
                        print("Reading", results_dataframe_file, '...', end="")
                    results_dataframe = read_dataframe_file(results_dataframe_file)
                    if verbose:
                        print("done.")

                write_summary_stats_file(results_dataframe, summary_stats_filename, verbose=verbose)

            if ivert_job_name is not None:
                ivert_exporter.upload_file_to_export_bucket(ivert_job_name, summary_stats_filename)
            files_to_export.append(summary_stats_filename)
            shared_ret_values["summary_stats_filename"] = summary_stats_filename

        if write_result_tifs:
            if not os.path.exists(result_tif_filename):
                if dem_ds is None:
                    dem_ds = gdal.Open(dem_name, gdal.GA_ReadOnly)

                if results_dataframe is None:
                    if verbose:
                        print("Reading", results_dataframe_file, '...', end="")
                    results_dataframe = read_dataframe_file(results_dataframe_file)
                    if verbose:
                        print("done.")

                generate_result_geotiff(results_dataframe, dem_ds, result_tif_filename, verbose=verbose)

            if ivert_job_name is not None:
                ivert_exporter.upload_file_to_export_bucket(ivert_job_name, result_tif_filename)
            files_to_export.append(result_tif_filename)
            shared_ret_values["result_tif_filename"] = result_tif_filename

        if plot_results:
            if not os.path.exists(plot_filename):
                if location_name is None:
                    location_name = os.path.split(dem_name)[1]

                if results_dataframe is None:
                    if verbose:
                        print("Reading", results_dataframe_file, '...', end="")
                    results_dataframe = read_dataframe_file(results_dataframe_file)
                    if verbose:
                        print("done.")

                plot_validation_results.plot_histogram_and_error_stats_4_panels(results_dataframe,
                                                                                plot_filename,
                                                                                place_name=location_name,
                                                                                verbose=verbose)

            if ivert_job_name is not None:
                ivert_exporter.upload_file_to_export_bucket(ivert_job_name, plot_filename)
            files_to_export.append(plot_filename)
            shared_ret_values["plot_filename"] = plot_filename

        if export_coastline_mask:
            if not os.path.exists(coastline_mask_filename):
                if results_dataframe is None:
                    if verbose:
                        print("Reading", results_dataframe_file, '...', end="")
                    results_dataframe = read_dataframe_file(results_dataframe_file)
                    if verbose:
                        print("done.")

                coastline_mask.create_coastline_mask(dem_name,
                                                     mask_out_lakes = mask_out_lakes,
                                                     mask_out_buildings = mask_out_buildings,
                                                     mask_out_urban = mask_out_urban,
                                                     use_osm_planet = use_osm_planet,
                                                     include_gmrt = include_gmrt_mask,
                                                     output_file = coastline_mask_filename,
                                                     verbose=verbose)

            if ivert_job_name is not None:
                ivert_exporter.upload_file_to_export_bucket(ivert_job_name, coastline_mask_filename)
            files_to_export.append(coastline_mask_filename)
            shared_ret_values["coastline_mask_filename"] = coastline_mask_filename

        # If we didn't have to open the dataframe or export anything, it was all already done.
        elif results_dataframe is None and verbose:
            print("Work already done here. Moving on.")

        return files_to_export

    elif mark_empty_results and os.path.exists(empty_results_filename):
        if verbose:
            print("No valid data produced during previous ICESat-2 analysis of", dem_name + ". Returning.")
        return files_to_export

    # Collect the metadata from the DEM.
    dem_ds, dem_array, dem_bbox, dem_epsg, dem_step_xy, \
        coastline_mask_filename, coastline_mask_array = \
        coastline_mask.get_coastline_mask_and_other_dem_data(dem_name,
                                                             mask_out_lakes=mask_out_lakes,
                                                             mask_out_buildings=mask_out_buildings,
                                                             mask_out_urban=mask_out_urban,
                                                             use_osm_planet=use_osm_planet,
                                                             include_gmrt=include_gmrt_mask,
                                                             target_fname_or_dir=coastline_mask_filename,
                                                             band_num=band_num,
                                                             verbose=verbose)

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
    if isinstance(dem_vertical_datum, str):
        dem_vertical_datum = dem_vertical_datum.strip().lower()
    # assert dem_vertical_datum in convert_vdatum.SUPPORTED_VDATUMS
    if isinstance(output_vertical_datum, str):
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
                                                   verbose=verbose)
            if (retval != 0) or (not os.path.exists(converted_dem_name)):
                raise FileNotFoundError(f"{dem_name} not converted correctly to {converted_dem_name}. Aborting.")

        # Get the dem array from the new dataset.
        dem_ds = gdal.Open(converted_dem_name, gdal.GA_ReadOnly)
        dem_array = dem_ds.GetRasterBand(1).ReadAsArray()
    else:
        converted_dem_name = None

    # elif use_icesat2_photon_database:
    if icesat2_photon_database_obj is None:
        icesat2_photon_database_obj = icesat2_photon_database.ICESat2_Database()

    photon_df = icesat2_photon_database_obj.get_photon_database(dem_bbox,
                                                                build_tiles_if_nonexistent=False,
                                                                good_photons_only=True,
                                                                dem_fname=converted_dem_name,
                                                                dem_epsg=dem_epsg,
                                                                verbose=verbose)

    if photon_df is None:
        if mark_empty_results:
            with open(empty_results_filename, 'w') as f:
                f.write(os.path.basename(dem_name) + " had no ICESat-2 results.")
            if verbose:
                print("Created", empty_results_filename, "to indicate no valid ICESat-2 data was returned here.")

            if ivert_job_name is not None:
                ivert_exporter.upload_file_to_export_bucket(ivert_job_name, empty_results_filename)
            shared_ret_values["empty_results_filename"] = empty_results_filename
            files_to_export.append(empty_results_filename)
            return files_to_export
        else:
            return files_to_export

    if verbose:
        print("{0:,}".format(len(photon_df)), "ICESat-2 photons present in photon dataframe.")

    if len(photon_df) == 0:
        if mark_empty_results:
            # Just create an empty file to mark this dataset as done.
            with open(empty_results_filename, 'w') as f:
                f.write(os.path.basename(dem_name) + " had no ICESat-2 results.")
            if verbose:
                print("Created", empty_results_filename, "to indicate no data was returned here.")

            if ivert_job_name is not None:
                ivert_exporter.upload_file_to_export_bucket(ivert_job_name, empty_results_filename)
            shared_ret_values["empty_results_filename"] = empty_results_filename
            return [empty_results_filename]
        else:
            return []

    ph_xcoords = photon_df["dem_x"]
    ph_ycoords = photon_df["dem_y"]

    # Omit any photons from "bad granules" found from find_bad_icesat2_granules.py
    # NOTE: After we've filtered out bad granules from the ICESat-2 database, we can
    # un-set the "omit_bad_granules" flag because the database will have already globally been
    # filtered out of bad-granule entries.
    if omit_bad_granules:
        bad_granules_gid_list = \
            find_bad_icesat2_granules.get_list_of_granules_to_reject(refind_bad_granules=False,
                                                                     return_as_gid_numbers=True,
                                                                     verbose=verbose)

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

    # Assign a dem (i,j) index location for each photon. We use this for computing.
    xstart, xstep, _, ystart, _, ystep = dem_ds.GetGeoTransform()
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
    photon_df = photon_df.set_index(["i", "j"], drop=False)

    # Make sure that we only look at cells that have at least 1 ground photon in them.
    ph_mask_ground_only = (photon_df["class_code"] == 1)
    dem_mask_w_ground_photons = numpy.zeros(dem_array.shape, dtype=bool)
    dem_mask_w_ground_photons[photon_df.i[ph_mask_ground_only],
                              photon_df.j[ph_mask_ground_only]] = 1

    dem_overlap_mask = dem_goodpixel_mask & dem_mask_w_ground_photons

    dem_overlap_i, dem_overlap_j = numpy.where(dem_overlap_mask)
    dem_overlap_elevs = dem_array[dem_overlap_mask]

    if measure_coverage:
        dem_overlap_xmin = xstart + (xstep * dem_overlap_j)
        dem_overlap_xmax = dem_overlap_xmin + xstep
        dem_overlap_ymax = ystart + (ystep * dem_overlap_i)
        dem_overlap_ymin = dem_overlap_ymax + ystep
    N = len(dem_overlap_i)

    if verbose:
        num_goodpixels = numpy.count_nonzero(dem_goodpixel_mask)
        print("{:,}".format(num_goodpixels), "land cells exist in the DEM.")
        if num_goodpixels == 0:
            print("No land cells found in DEM with overlapping ICESat-2 data. Stopping and moving on.")

            if mark_empty_results:
                # Just create an empty file to mark this dataset as done.
                with open(empty_results_filename, 'w') as f:
                    f.write(os.path.basename(dem_name) + " had no ICESat-2 results.")
                if verbose:
                    print("Created", empty_results_filename, "to indicate no data was returned here.")

                if ivert_job_name is not None:
                    ivert_exporter.upload_file_to_export_bucket(ivert_job_name, empty_results_filename)
                files_to_export.append(empty_results_filename)
                shared_ret_values["empty_results_filename"] = empty_results_filename

            return files_to_export

        else:
            print("{:,} ICESat-2 photons overlap".format(len(photon_df)),
                  "{:,}".format(N),
                  "DEM cells ({:0.2f}% of total DEM data).".format(
                      numpy.count_nonzero(dem_overlap_mask) * 100 / num_goodpixels))

    # If we have no data overlapping the valid land DEM cells, just return None
    if numpy.count_nonzero(dem_overlap_mask) == 0:
        if verbose:
            print("No overlapping ICESat-2 data with valid land cells. Stopping and moving on.")

        if mark_empty_results:
            # Just create an empty file to makre this dataset as done.
            with open(empty_results_filename, 'w') as f:
                f.write(os.path.basename(dem_name) + " had no ICESat-2 results.")
            if verbose:
                print("Created", empty_results_filename, "to indicate no data was returned here.")

            if ivert_job_name is not None:
                ivert_exporter.upload_file_to_export_bucket(ivert_job_name, empty_results_filename)
            shared_ret_values["empty_results_filename"] = empty_results_filename
            files_to_export.append(empty_results_filename)
            return files_to_export

        else:
            return files_to_export

    # If requested, perform a validation on a photon-by-photon basis, in addition to the grid-cell
    # analysis peformed later on down. First, create a dataframe with just the DEM elevations.
    if include_photon_level_validation:
        if verbose:
            print("Performing photon-level validation...")

        if verbose:
            print("\tSubsetting ground-only photons... ", end="")
        # Get the subset of the dataframe with ground-only photons.
        photon_df_ground_only = photon_df[ph_mask_ground_only]
        if verbose:
            print("Done.")

        if verbose:
            print("\tGenerating DEM elevation dataframe... ", end="")

        # # Generate a dataframe of the dem elevations, indexed by their i,j coordinates.
        dem_elev_df = pandas.DataFrame({"dem_elevation": dem_overlap_elevs},
                                       index=pandas.MultiIndex.from_arrays((dem_overlap_i, dem_overlap_j),
                                                                           names=("i", "j"))
                                       )
        if verbose:
            print("Done with {0} records.".format(len(dem_elev_df)))

        # print(dem_elev_df)

        # Join the dataframes by their i,j values, which will add the "dem_i", "dem_j",
        # and "dem_elevations" columns to the photon dataframe.
        # This could take a while to run, depending on the sizes of the dataframes.
        # Both dataframes have (i,j) as their index, so this should be good, I shouldn't need to specify the "on=" parameter.
        if verbose:
            print("\tJoining photon_df and DEM elevation tables... ", end="")
        photon_df_with_dem_elevs = photon_df_ground_only.join(dem_elev_df, how='left') # on=('i','j')
        # Then, drop all photons that didn't line up with a valid land cell according to the coastline mask (dem_elevation values would be NaN)
        photon_df_with_dem_elevs = photon_df_with_dem_elevs[pandas.notna(photon_df_with_dem_elevs["dem_elevation"])]
        if verbose:
            print("Done with {0} records.".format(len(photon_df_with_dem_elevs)))

        # Get the correct height field from the database.
        if output_vertical_datum in ("ellipsoid", "wgs84"):
            height_field = photon_df_with_dem_elevs.h_ellipsoid
        elif output_vertical_datum in ("geoid", "egm2008"):
            height_field = photon_df_with_dem_elevs.h_geoid
        else:
            raise ValueError("Should not have gotten here. Unhandled vdatum: {}".format(output_vertical_datum))

        # Subtract the elevations and give us a photon_level error bar.
        # This is a single-column subtraction, should be pretty quick.
        if verbose:
            print("\tCalculating elevation differences... ", end="")
        photon_df_with_dem_elevs["dem_minus_is2_m"] = photon_df_with_dem_elevs["dem_elevation"] - height_field
        if verbose:
            print("Done.")

        # Write out the photon level elevation difference dataset.
        base, ext = os.path.splitext(results_dataframe_file)
        photon_results_dataframe_file = base.replace("_results", "_photons") + ext
        if verbose:
            print("\tWriting", os.path.split(photon_results_dataframe_file)[1] + "... ", end="")
        photon_df_with_dem_elevs.to_hdf(photon_results_dataframe_file, "icesat2", complib="zlib", complevel=3)
        if verbose:
            # Add an extra newline at the end to visually separate it from the next set of steps.
            print("Done.\n")

        if ivert_job_name is not None:
            ivert_exporter.upload_file_to_export_bucket(ivert_job_name, photon_results_dataframe_file)
        files_to_export.append(photon_results_dataframe_file)
        shared_ret_values["photon_results_dataframe_file"] = photon_results_dataframe_file

    if verbose:
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

    # Create a multiprocessing shared-memory objects for photon heights, i, j. and codes.
    if output_vertical_datum in ("ellipsoid", "wgs84"):
        height_field = photon_df.h_ellipsoid
    elif output_vertical_datum in ("geoid", "egm2008"):
        height_field = photon_df.h_geoid
    else:
        raise ValueError("Should not have gotten here. Unhandled vdatum: {}".format(output_vertical_datum))

    # height_array = manager.Array(dt_dict[height_field.dtype], height_field)
    # i_array = manager.Array(dt_dict[photon_df.i.dtype], photon_df.i)
    # j_array = manager.Array(dt_dict[photon_df.j.dtype], photon_df.j)
    # code_array = manager.Array(dt_dict[photon_df.class_code.dtype], photon_df.class_code)

    # Create "shared memory" arrays for the sub-processes to read.
    proc_id = os.getpid()
    height_array_name = f"heights_{proc_id}"
    i_array_name = f"i_{proc_id}"
    j_array_name = f"j_{proc_id}"
    code_array_name = f"codes_{proc_id}"
    assert height_field.shape == photon_df.i.shape == photon_df.j.shape == photon_df.class_code.shape

    # Create the shared memory arrays and copy the data into them.
    height_smo = shared_memory.SharedMemory(size=height_field.nbytes,
                                            name=height_array_name,
                                            create=True)
    height_smo.buf[:] = height_field.to_numpy().tobytes()
    height_dtype = height_field.dtype

    i_smo = shared_memory.SharedMemory(size=photon_df.i.nbytes,
                                       name=i_array_name,
                                       create=True)
    i_smo.buf[:] = photon_df.i.to_numpy().tobytes()
    i_dtype = photon_df.i.dtype

    j_smo = shared_memory.SharedMemory(size=photon_df.j.nbytes,
                                       name=j_array_name,
                                       create=True)
    j_smo.buf[:] = photon_df.j.to_numpy().tobytes()
    j_dtype = photon_df.j.dtype

    code_smo = shared_memory.SharedMemory(size=photon_df.class_code.nbytes,
                                          name=code_array_name,
                                          create=True)
    code_smo.buf[:] = photon_df.class_code.to_numpy().tobytes()
    code_dtype = photon_df.class_code.dtype

    if measure_coverage:
        # If we're trying to measure coverage, then we really do need the floating point x and y locations as well.
        # x_array = manager.Array(dt_dict[photon_df.dem_x.dtype], photon_df.dem_x)
        # y_array = manager.Array(dt_dict[photon_df.dem_y.dtype], photon_df.dem_y)
        assert height_field.shape == photon_df.dem_x.shape == photon_df.dem_y.shape
        x_array_name = f"x_{proc_id}"
        x_smo = shared_memory.SharedMemory(size=photon_df.dem_x.nbytes,
                                           name=x_array_name,
                                           create=True)
        x_smo.buf[:] = photon_df.dem_x.to_numpy().tobytes()
        x_dtype = photon_df.dem_x.dtype

        y_array_name = f"y_{proc_id}"
        y_smo = shared_memory.SharedMemory(size=photon_df.dem_y.nbytes,
                                           name=y_array_name,
                                           create=True)
        y_smo[:] = photon_df.dem_y.to_numpy().tobytes()
        y_dtype = photon_df.dem_y.dtype

    else:
        x_array_name = None
        y_array_name = None
        x_smo = None
        y_smo = None
        x_dtype = None
        y_dtype = None

    # Keep a list of the active shared memory objects, so we can close them later.
    if measure_coverage:
        memory_objs = [height_smo, i_smo, j_smo, code_smo, x_smo, y_smo]
    else:
        memory_objs = [height_smo, i_smo, j_smo, code_smo]

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
                kick_off_new_child_process(height_array_name,
                                           height_dtype,
                                           i_array_name,
                                           i_dtype,
                                           j_array_name,
                                           j_dtype,
                                           code_array_name,
                                           code_dtype,
                                           height_field.shape,
                                           photon_limit=max_photons_per_cell,
                                           measure_coverage=measure_coverage,
                                           x_array_name=x_array_name,
                                           x_dtype=x_dtype,
                                           y_array_name=y_array_name,
                                           y_dtype=y_dtype)

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
                    if verbose:
                        # raise UserWarning("Sub-process terminated unexpectedly. Some data may be missing. Restarting a new process.")
                        print("\nSub-process terminated unexpectedly. Some data may be missing. Restarting a new process.")
                    # Close out the dead process and its pipes
                    proc.join()
                    pipe.close()
                    pipe_child.close()
                    # Kick off a shiny new process
                    proc, pipe, pipe_child = kick_off_new_child_process(height_array_name,
                                                                        height_dtype,
                                                                        i_array_name,
                                                                        i_dtype,
                                                                        j_array_name,
                                                                        j_dtype,
                                                                        code_array_name,
                                                                        code_dtype,
                                                                        height_field.shape,
                                                                        photon_limit=max_photons_per_cell,
                                                                        measure_coverage=measure_coverage,
                                                                        x_array_name=x_array_name,
                                                                        x_dtype=x_dtype,
                                                                        y_array_name=y_array_name,
                                                                        y_dtype=y_dtype)
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
                    if verbose:
                        progress_bar.ProgressBar(counter_finished, N, suffix=("{0:>" +str(len(str(N))) + "d}/{1:d}").format(counter_finished, N))

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

    except Exception as e:
        if verbose:
            print("\nException encountered in ICESat-2 processing loop. Exiting.")

        clean_procs_and_pipes(running_procs, open_pipes_parent, open_pipes_child, memory_objs)
        print(e)
        return files_to_export

    t_end = time.perf_counter()
    if verbose:
        total_time_s = t_end - t_start
        # If there's 100 or more seconds, state the time with minutes.
        if total_time_s >= 100:
            total_time_m = int(total_time_s / 60)
            partial_time_s = total_time_s % 60
            print("{0:d} minute".format(total_time_m) + ("s" if total_time_m > 1 else "") + " {0:0.1f} seconds total, ({1:0.4f} s/iteration)".format(partial_time_s, ( (total_time_s/N) if N>0 else 0)))
        else:
            print("{0:0.1f} seconds total, ({1:0.4f} s/iteration)".format(total_time_s,
                                                                          ((total_time_s / N) if N > 0 else 0)))

    # Clean up any remaining processes and pipes
    clean_procs_and_pipes(running_procs, open_pipes_parent, open_pipes_child, memory_objs)

    # Concatenate all the results dataframes
    # If there were no overlappying photons, then just return none.
    if len(results_dataframes_list) == 0:
        return files_to_export

    results_dataframe = pandas.concat(results_dataframes_list)
    # Subset for only valid results out. Eliminate useless nodata values.
    results_dataframe = results_dataframe[results_dataframe["mean"] != EMPTY_VAL].copy()
    if verbose:
        print("{0:,} valid interdecile photon records in {1:,} DEM cells.".format(results_dataframe["numphotons_intd"].sum(), len(results_dataframe)))

    # The outliers_sd_threshold is the # of standard deviations outside the mean to call outliers in the data, and omit
    # results of those outliers. "None" does not omit anything. Default is 2.5 SDs, which is how outliers are defined
    # in box-and-whisker plots.
    if outliers_sd_threshold is not None:
        assert type(outliers_sd_threshold) in (int, float)
        diff_mean = results_dataframe["diff_mean"]
        meanval, stdval = diff_mean.mean(), diff_mean.std()
        low_cutoff = meanval - (stdval * outliers_sd_threshold)
        hi_cutoff = meanval + (stdval * outliers_sd_threshold)
        valid_mask = (diff_mean >= low_cutoff) & (diff_mean <= hi_cutoff)
        results_dataframe = results_dataframe[valid_mask].copy()
        if verbose:
            print("{0:,} DEM cells after removing outliers.".format(len(results_dataframe)))

    if export_coastline_mask:
        if ivert_job_name is not None:
            ivert_exporter.upload_file_to_export_bucket(ivert_job_name, coastline_mask_filename)
        files_to_export.append(coastline_mask_filename)
        shared_ret_values["coastline_mask_filename"] = coastline_mask_filename

    if len(results_dataframe) == 0:
        if verbose:
            print("No valid results in results dataframe. No outputs computed.")
        if mark_empty_results:
            # Just create an empty file to makre this dataset as done.
            with open(empty_results_filename, 'w') as f:
                f.write("No ICESat-2 data data overlapping this DEM to validate.")

            if verbose:
                print("Created", empty_results_filename, "to indicate no data was returned here.")

            if ivert_job_name is not None:
                ivert_exporter.upload_file_to_export_bucket(ivert_job_name, empty_results_filename)
            files_to_export.append(empty_results_filename)
            shared_ret_values["empty_results_filename"] = empty_results_filename

        return files_to_export

    else:
        # Write out the results dataframe. Method depends upon the file type. Can be .csv, .txt, .h5 (assumed default of not one of the text files.)
        base, ext = os.path.splitext(results_dataframe_file)
        ext = ext.lower().strip()

        if ext in (".txt", ".csv"):
            results_dataframe.to_csv(results_dataframe_file)
        else:
            results_dataframe.to_hdf(results_dataframe_file, key="icesat2", complib="zlib", mode='w')

        if verbose:
            print(results_dataframe_file, "written.")

        if ivert_job_name is not None:
            ivert_exporter.upload_file_to_export_bucket(ivert_job_name, results_dataframe_file)
        files_to_export.append(results_dataframe_file)
        shared_ret_values["results_dataframe_file"] = results_dataframe_file

    if write_summary_stats:
        write_summary_stats_file(results_dataframe,
                                 summary_stats_filename,
                                 verbose=verbose)

        if ivert_job_name is not None:
            ivert_exporter.upload_file_to_export_bucket(ivert_job_name, summary_stats_filename)
        files_to_export.append(summary_stats_filename)
        shared_ret_values["summary_stats_filename"] = summary_stats_filename

    if write_result_tifs:
        if dem_ds is None:
            dem_ds = gdal.Open(dem_name, gdal.GA_ReadOnly)
        generate_result_geotiff(results_dataframe,
                                dem_ds,
                                result_tif_filename,
                                verbose=verbose)

        if ivert_job_name is not None:
            ivert_exporter.upload_file_to_export_bucket(ivert_job_name, result_tif_filename)
        files_to_export.append(result_tif_filename)
        shared_ret_values["result_tif_filename"] = result_tif_filename

    if plot_results:
        if location_name is None:
            location_name = os.path.split(dem_name)[1]

        plot_validation_results.plot_histogram_and_error_stats_4_panels(results_dataframe,
                                                                        plot_filename,
                                                                        place_name=location_name,
                                                                        verbose=verbose)

        if ivert_job_name is not None:
            ivert_exporter.upload_file_to_export_bucket(ivert_job_name, plot_filename)

        files_to_export.append(plot_filename)
        shared_ret_values["plot_filename"] = plot_filename

    if delete_datafiles:
        del dem_ds
        if verbose:
            print("Cleaning up...", end="")

        if os.path.exists(coastline_mask_filename) and not export_coastline_mask:
            os.remove(coastline_mask_filename)
        if (converted_dem_name is not None) and os.path.exists(converted_dem_name):
            os.remove(converted_dem_name)

        if verbose:
            print("Done.")

    return files_to_export


def write_summary_stats_file(results_df: pandas.DataFrame,
                             statsfile_name: str,
                             verbose: bool = True) -> None:
    """Write the summary statistics file.

    Args:
        results_df: pandas dataframe - contains the summary statistics
        statsfile_name: string - the name of the file to write
        verbose: bool - if True, print diagnostic messages

    Returns:
        None
    """
    if results_df is None:
        if verbose:
            print("write_summary_stats_file(): No results dataframe to write. Returning")

    if len(results_df) == 0:
        if verbose:
            print("write_summary_stats_file(): No stats to compute in results dataframe. Returning")
        return

    lines = list()
    lines.append("Number of DEM cells validated (cells): {0}".format(len(results_df)))
    lines.append("Total number of ground photons used to validate this DEM (photons): {0}".format(results_df["numphotons_intd"].sum()))
    lines.append("Mean number of photons used to validate each cell (photons): {0}".format(results_df["numphotons_intd"].mean()))

    mean_diff = results_df["diff_mean"]

    lines.append("Mean bias error (ICESat-2 - DEM) (m): {0}".format(mean_diff.mean()))
    lines.append("RMSE (m): {0}".format(numpy.sqrt(numpy.mean(numpy.power(mean_diff, 2)))))
    lines.append("== Decile ranges of errors (ICESat-2 - DEM) (m) (Look for long-tails, indicating possible artifacts.) ===")

    percentile_levels = [0, 1, 10, 20, 30, 40, 50, 60, 70, 80, 90, 99, 100]
    percentile_values = numpy.percentile(mean_diff, percentile_levels)
    for l, v in zip(percentile_levels, percentile_values):
        lines.append("    {0:>3d} percentile error level (m): {1}".format(l, v))

    lines.append("Mean canopy cover (% cover): {0:0.02f}".format(results_df["canopy_fraction"].mean()*100))
    lines.append("% of cells with >0 measured canopy (%): {0}".format((numpy.count_nonzero(results_df.canopy_fraction > 0.0) / len(results_df))*100))
    lines.append("Mean canopy cover in 'wooded' cells containing >0 canopy (% cover): {0}".format(results_df[results_df["canopy_fraction"] > 0]["canopy_fraction"].mean()*100))
    lines.append("Mean roughness (stddev. of photon elevations within each cell (m)): {0}".format(results_df["stddev"].mean()))

    out_text = "\n".join(lines)
    with open(statsfile_name, 'w') as outf:
        outf.write(out_text)

    if verbose:
        if os.path.exists(statsfile_name):
            print(statsfile_name, "written.")
        else:
            print(statsfile_name, "NOT written.")

    return


def generate_result_geotiff(results_dataframe, dem_ds, result_tif_filename, verbose=True):
    """Given the results in the dataframe, output geotiffs to visualize these.

    Name the geotiffs after the dataframe: [original_filename]_<tag>.tif

    Geotiff tags will include:
        - mean_diff
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
                           options=["COMPRESS=DEFLATE", "PREDICTOR=2", "TILED=YES"])
    out_ds.SetProjection(projection) # Might need to add .ExportToWkt()
    out_ds.SetGeoTransform(gt)
    band = out_ds.GetRasterBand(1)
    band.WriteArray(result_array)
    band.SetNoDataValue(emptyval)
    band.GetStatistics(0, 1)
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
    parser.add_argument('output_dir', type=str, nargs="?", default="",
                        help='Directory to write output results. Default: Will put in the same directory as input filename')
    parser.add_argument('--input_vdatum', '-ivd', type=str, default="egm2008",
                        help="Input DEM vertical datum. (Default: 'egm2008')"
                             " Currently supported datum arguments, not case-sensitive: ({})".format(
                                ",".join([str(vd) for vd in convert_vdatum.SUPPORTED_VDATUMS])))
    parser.add_argument('--output_vdatum', '-ovd', type=str, default="egm2008",
                        help="Output vertical datum. ICESat-2 only supports 'wgs84' and 'egm2008'. (Default: 'egm2008')")
    parser.add_argument('--datadir', type=str, default="",
                        help="A scratch directory to write interim data files. Useful if user would like to save temp files elsewhere. Defaults to the output_dir directory.")
    parser.add_argument('--band_num', type=int, default=1,
                        help="The band number (1-indexed) of the input_dem. (Default: 1)")
    parser.add_argument("--export_coastline_mask", "--coast", "-c", action='store_true', default=False,
                        help="Export a geotiff of the coastline mask.")
    parser.add_argument('--place_name', '-name', type=str, default=None,
                        help='A text name of the location, to put in the title of the plot (if --plot_results is selected)')
    parser.add_argument("--numprocs", '-np', type=int, default=parallel_funcs.physical_cpu_count(),
                        help='The number of sub-processes to run for this validation. Default to the maximum physical CPU count on this machine.')
    parser.add_argument('--delete_datafiles', action='store_true', default=False,
                        help='Delete the interim data files generated. Reduces storage requirements. (Default: keep them all.)')
    parser.add_argument('--use_urban_mask', action='store_true', default=False,
                        help="Use the WSL 'Urban Area' mask rather than OSM building footprints to mask out IceSat-2 data. Useful over lower-resolution (10m or coarser) dems, which tend to be bigger than building footprints.")
    parser.add_argument("--measure_coverage", "-mc", action="store_true", default=False,
                        help="Measure the coverage %age of icesat-2 data in each of the output DEM cells.")
    parser.add_argument('--write_result_tifs', action='store_true', default=False,
                        help=""""Write output geotiff with the errors in cells that have ICESat-2 photons, NDVs elsewhere.""")
    parser.add_argument("--outlier_sd_threshold", default="2.5",
                        help="Number of standard-deviations away from the mean to omit outliers. Default 2.5 (standard deviations). Choose 'None' if no outlier filtering is requested.")
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

    # The output directory defaults to the input directory.
    if not args.output_dir:
        args.output_dir = os.path.dirname(args.input_dem)

    # The data directory defaults to the output directory.
    if not args.datadir:
        args.datadir = args.output_dir

    # Set up multiprocessing. 'spawn' is the slowest but the most reliable. Otherwise, file handlers are fucking us up.
    mp.set_start_method('spawn')

    # Run the validation
    validate_dem(args.input_dem,
                 output_dir=args.output_dir,
                 dem_vertical_datum=args.input_vdatum,
                 output_vertical_datum=args.output_vdatum,
                 interim_data_dir=(None if not args.datadir else args.datadir),
                 overwrite=args.overwrite,
                 delete_datafiles=args.delete_datafiles,
                 write_result_tifs=args.write_result_tifs,
                 plot_results=args.plot_results,
                 location_name=args.place_name,
                 outliers_sd_threshold=ast.literal_eval(args.outlier_sd_threshold),
                 mask_out_buildings=not args.use_urban_mask,
                 mask_out_urban=args.use_urban_mask,
                 measure_coverage=args.measure_coverage,
                 numprocs=args.numprocs,
                 band_num=args.band_num,
                 verbose=not args.quiet)
