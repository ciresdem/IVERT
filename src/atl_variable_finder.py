# -*- coding: utf-8 -*-
import numpy

import argparse

def accrue_variables(var_id_list, dataset_name="ATL03", return_type=list, str_sep="\n"):
    """Given a list of variable IDs, return a complete list of granule variables to acquire.

    Return in whatever datatype we can convert to (from a list). If string, omit the bounding []
    and separate by whatever character is given by str_sep."""

    dataset_name = dataset_name.upper().strip()
    var_dict = {"ATL03": ATL03_var_dict,
                "ATL06": ATL06_var_dict,
                "ATL08": ATL08_var_dict}[dataset_name]

    var_list = []
    for var_id in var_id_list:
        var_list.extend(var_dict[var_id.strip().lower()])

    # Make sure we just have unique entries
    var_list = list(numpy.unique(var_list))

    return return_type(var_list)

def list_argument_ids(dataset_name="ATL03"):
    dataset_name = dataset_name.upper().strip()
    var_dict = {"ATL03": ATL03_var_dict,
                "ATL06": ATL06_var_dict,
                "ATL08": ATL08_var_dict}[dataset_name]

    return sorted(var_dict.keys())

def make_variable_id_dict(var_list, sort=True):
    """From a complete list of ICESat-2 granule variables, create a dictionary
    (similar to what icepyx does, but complete) of variable_id : [list_of_individual_variables] key:value pairs.

    Used to make the dictionaries present in this module."""
    var_dict = {}
    for var in var_list:
        var = var.lower()
        item_list = var.split("/")
        # Some of the variables end in the beam name (i.e. 'gt1l'). If so, use the tag immediately in front of it.
        if item_list[-1] in ("gt1l","gt1r","gt2l","gt2r","gt3l","gt3r"):
            var_key = item_list[-2]
        else:
            var_key = item_list[-1]

        if var_key in var_dict:
            var_dict[var_key].append(var)
        else:
            var_dict[var_key] = [var]

    if sort:
        sorted_keys = sorted(var_dict.keys())
        var_dict = dict([(key, var_dict[key]) for key in sorted_keys])

    return var_dict

###############################################################################
################################### ATL03 #####################################
###############################################################################
ATL03_var_dict = \
{
  "addpad_flag": [
    "/ancillary_data/gt1l/signal_find_input/addpad_flag",
    "/ancillary_data/gt1r/signal_find_input/addpad_flag",
    "/ancillary_data/gt2l/signal_find_input/addpad_flag",
    "/ancillary_data/gt2r/signal_find_input/addpad_flag",
    "/ancillary_data/gt3l/signal_find_input/addpad_flag",
    "/ancillary_data/gt3r/signal_find_input/addpad_flag"
  ],
  "alpha_inc": [
    "/ancillary_data/gt1l/signal_find_input/alpha_inc",
    "/ancillary_data/gt1r/signal_find_input/alpha_inc",
    "/ancillary_data/gt2l/signal_find_input/alpha_inc",
    "/ancillary_data/gt2r/signal_find_input/alpha_inc",
    "/ancillary_data/gt3l/signal_find_input/alpha_inc",
    "/ancillary_data/gt3r/signal_find_input/alpha_inc"
  ],
  "alpha_max": [
    "/ancillary_data/gt1l/signal_find_input/alpha_max",
    "/ancillary_data/gt1r/signal_find_input/alpha_max",
    "/ancillary_data/gt2l/signal_find_input/alpha_max",
    "/ancillary_data/gt2r/signal_find_input/alpha_max",
    "/ancillary_data/gt3l/signal_find_input/alpha_max",
    "/ancillary_data/gt3r/signal_find_input/alpha_max"
  ],
  "altimetry": [
    "/ancillary_data/altimetry"
  ],
  "altitude_sc": [
    "/gt1l/geolocation/altitude_sc",
    "/gt1r/geolocation/altitude_sc",
    "/gt2l/geolocation/altitude_sc",
    "/gt2r/geolocation/altitude_sc",
    "/gt3l/geolocation/altitude_sc",
    "/gt3r/geolocation/altitude_sc"
  ],
  "ancillary_data": [
    "/ancillary_data",
    "/ancillary_data/gt1l",
    "/ancillary_data/gt1r",
    "/ancillary_data/gt2l",
    "/ancillary_data/gt2r",
    "/ancillary_data/gt3l",
    "/ancillary_data/gt3r"
  ],
  "atl03_pad": [
    "/ancillary_data/altimetry/atl03_pad"
  ],
  "atlas_engineering": [
    "/ancillary_data/atlas_engineering"
  ],
  "atlas_impulse_response": [
    "/atlas_impulse_response"
  ],
  "atlas_sdp_gps_epoch": [
    "/ancillary_data/atlas_sdp_gps_epoch"
  ],
  "band_tol": [
    "/ancillary_data/altimetry/band_tol"
  ],
  "bckgrd_atlas": [
    "/gt1l/bckgrd_atlas",
    "/gt1r/bckgrd_atlas",
    "/gt2l/bckgrd_atlas",
    "/gt2r/bckgrd_atlas",
    "/gt3l/bckgrd_atlas",
    "/gt3r/bckgrd_atlas"
  ],
  "bckgrd_counts": [
    "/gt1l/bckgrd_atlas/bckgrd_counts",
    "/gt1r/bckgrd_atlas/bckgrd_counts",
    "/gt2l/bckgrd_atlas/bckgrd_counts",
    "/gt2r/bckgrd_atlas/bckgrd_counts",
    "/gt3l/bckgrd_atlas/bckgrd_counts",
    "/gt3r/bckgrd_atlas/bckgrd_counts"
  ],
  "bckgrd_counts_reduced": [
    "/gt1l/bckgrd_atlas/bckgrd_counts_reduced",
    "/gt1r/bckgrd_atlas/bckgrd_counts_reduced",
    "/gt2l/bckgrd_atlas/bckgrd_counts_reduced",
    "/gt2r/bckgrd_atlas/bckgrd_counts_reduced",
    "/gt3l/bckgrd_atlas/bckgrd_counts_reduced",
    "/gt3r/bckgrd_atlas/bckgrd_counts_reduced"
  ],
  "bckgrd_hist_top": [
    "/gt1l/bckgrd_atlas/bckgrd_hist_top",
    "/gt1r/bckgrd_atlas/bckgrd_hist_top",
    "/gt2l/bckgrd_atlas/bckgrd_hist_top",
    "/gt2r/bckgrd_atlas/bckgrd_hist_top",
    "/gt3l/bckgrd_atlas/bckgrd_hist_top",
    "/gt3r/bckgrd_atlas/bckgrd_hist_top"
  ],
  "bckgrd_int_height": [
    "/gt1l/bckgrd_atlas/bckgrd_int_height",
    "/gt1r/bckgrd_atlas/bckgrd_int_height",
    "/gt2l/bckgrd_atlas/bckgrd_int_height",
    "/gt2r/bckgrd_atlas/bckgrd_int_height",
    "/gt3l/bckgrd_atlas/bckgrd_int_height",
    "/gt3r/bckgrd_atlas/bckgrd_int_height"
  ],
  "bckgrd_int_height_reduced": [
    "/gt1l/bckgrd_atlas/bckgrd_int_height_reduced",
    "/gt1r/bckgrd_atlas/bckgrd_int_height_reduced",
    "/gt2l/bckgrd_atlas/bckgrd_int_height_reduced",
    "/gt2r/bckgrd_atlas/bckgrd_int_height_reduced",
    "/gt3l/bckgrd_atlas/bckgrd_int_height_reduced",
    "/gt3r/bckgrd_atlas/bckgrd_int_height_reduced"
  ],
  "bckgrd_mean": [
    "/gt1l/signal_find_output/inlandwater/bckgrd_mean",
    "/gt1l/signal_find_output/land/bckgrd_mean",
    "/gt1l/signal_find_output/ocean/bckgrd_mean",
    "/gt1l/signal_find_output/sea_ice/bckgrd_mean",
    "/gt1r/signal_find_output/inlandwater/bckgrd_mean",
    "/gt1r/signal_find_output/land/bckgrd_mean",
    "/gt1r/signal_find_output/ocean/bckgrd_mean",
    "/gt1r/signal_find_output/sea_ice/bckgrd_mean",
    "/gt2l/signal_find_output/inlandwater/bckgrd_mean",
    "/gt2l/signal_find_output/land/bckgrd_mean",
    "/gt2l/signal_find_output/ocean/bckgrd_mean",
    "/gt2l/signal_find_output/sea_ice/bckgrd_mean",
    "/gt2r/signal_find_output/inlandwater/bckgrd_mean",
    "/gt2r/signal_find_output/land/bckgrd_mean",
    "/gt2r/signal_find_output/ocean/bckgrd_mean",
    "/gt2r/signal_find_output/sea_ice/bckgrd_mean",
    "/gt3l/signal_find_output/inlandwater/bckgrd_mean",
    "/gt3l/signal_find_output/land/bckgrd_mean",
    "/gt3l/signal_find_output/ocean/bckgrd_mean",
    "/gt3l/signal_find_output/sea_ice/bckgrd_mean",
    "/gt3r/signal_find_output/inlandwater/bckgrd_mean",
    "/gt3r/signal_find_output/land/bckgrd_mean",
    "/gt3r/signal_find_output/ocean/bckgrd_mean",
    "/gt3r/signal_find_output/sea_ice/bckgrd_mean"
  ],
  "bckgrd_rate": [
    "/gt1l/bckgrd_atlas/bckgrd_rate",
    "/gt1r/bckgrd_atlas/bckgrd_rate",
    "/gt2l/bckgrd_atlas/bckgrd_rate",
    "/gt2r/bckgrd_atlas/bckgrd_rate",
    "/gt3l/bckgrd_atlas/bckgrd_rate",
    "/gt3r/bckgrd_atlas/bckgrd_rate"
  ],
  "bckgrd_sigma": [
    "/gt1l/signal_find_output/inlandwater/bckgrd_sigma",
    "/gt1l/signal_find_output/land/bckgrd_sigma",
    "/gt1l/signal_find_output/ocean/bckgrd_sigma",
    "/gt1l/signal_find_output/sea_ice/bckgrd_sigma",
    "/gt1r/signal_find_output/inlandwater/bckgrd_sigma",
    "/gt1r/signal_find_output/land/bckgrd_sigma",
    "/gt1r/signal_find_output/ocean/bckgrd_sigma",
    "/gt1r/signal_find_output/sea_ice/bckgrd_sigma",
    "/gt2l/signal_find_output/inlandwater/bckgrd_sigma",
    "/gt2l/signal_find_output/land/bckgrd_sigma",
    "/gt2l/signal_find_output/ocean/bckgrd_sigma",
    "/gt2l/signal_find_output/sea_ice/bckgrd_sigma",
    "/gt2r/signal_find_output/inlandwater/bckgrd_sigma",
    "/gt2r/signal_find_output/land/bckgrd_sigma",
    "/gt2r/signal_find_output/ocean/bckgrd_sigma",
    "/gt2r/signal_find_output/sea_ice/bckgrd_sigma",
    "/gt3l/signal_find_output/inlandwater/bckgrd_sigma",
    "/gt3l/signal_find_output/land/bckgrd_sigma",
    "/gt3l/signal_find_output/ocean/bckgrd_sigma",
    "/gt3l/signal_find_output/sea_ice/bckgrd_sigma",
    "/gt3r/signal_find_output/inlandwater/bckgrd_sigma",
    "/gt3r/signal_find_output/land/bckgrd_sigma",
    "/gt3r/signal_find_output/ocean/bckgrd_sigma",
    "/gt3r/signal_find_output/sea_ice/bckgrd_sigma"
  ],
  "bin_width": [
    "/ancillary_data/calibrations/low_link_impulse_response/bin_width"
  ],
  "bounce_time_offset": [
    "/gt1l/geolocation/bounce_time_offset",
    "/gt1r/geolocation/bounce_time_offset",
    "/gt2l/geolocation/bounce_time_offset",
    "/gt2r/geolocation/bounce_time_offset",
    "/gt3l/geolocation/bounce_time_offset",
    "/gt3r/geolocation/bounce_time_offset"
  ],
  "cal19_product": [
    "/ancillary_data/calibrations/first_photon_bias/cal19_product"
  ],
  "cal20_product": [
    "/ancillary_data/calibrations/low_link_impulse_response/cal20_product"
  ],
  "cal34_product": [
    "/ancillary_data/calibrations/dead_time_radiometric_signal_loss/cal34_product"
  ],
  "cal42_product": [
    "/ancillary_data/calibrations/dead_time/cal42_product"
  ],
  "calibrations": [
    "/ancillary_data/calibrations"
  ],
  "control": [
    "/ancillary_data/control"
  ],
  "crossing_time": [
    "/orbit_info/crossing_time"
  ],
  "cycle_number": [
    "/orbit_info/cycle_number"
  ],
  "dac": [
    "/gt1l/geophys_corr/dac",
    "/gt1r/geophys_corr/dac",
    "/gt2l/geophys_corr/dac",
    "/gt2r/geophys_corr/dac",
    "/gt3l/geophys_corr/dac",
    "/gt3r/geophys_corr/dac"
  ],
  "data_end_utc": [
    "/ancillary_data/data_end_utc"
  ],
  "data_start_utc": [
    "/ancillary_data/data_start_utc"
  ],
  "dead_time": [
    "/ancillary_data/calibrations/dead_time",
    "/ancillary_data/calibrations/dead_time/gt1l",
    "/ancillary_data/calibrations/dead_time/gt1l",
    "/ancillary_data/calibrations/dead_time/gt1r",
    "/ancillary_data/calibrations/dead_time/gt1r",
    "/ancillary_data/calibrations/dead_time/gt2l",
    "/ancillary_data/calibrations/dead_time/gt2l",
    "/ancillary_data/calibrations/dead_time/gt2r",
    "/ancillary_data/calibrations/dead_time/gt2r",
    "/ancillary_data/calibrations/dead_time/gt3l",
    "/ancillary_data/calibrations/dead_time/gt3l",
    "/ancillary_data/calibrations/dead_time/gt3r",
    "/ancillary_data/calibrations/dead_time/gt3r"
  ],
  "dead_time_radiometric_signal_loss": [
    "/ancillary_data/calibrations/dead_time_radiometric_signal_loss",
    "/ancillary_data/calibrations/dead_time_radiometric_signal_loss/gt1l",
    "/ancillary_data/calibrations/dead_time_radiometric_signal_loss/gt1l",
    "/ancillary_data/calibrations/dead_time_radiometric_signal_loss/gt1l",
    "/ancillary_data/calibrations/dead_time_radiometric_signal_loss/gt1l",
    "/ancillary_data/calibrations/dead_time_radiometric_signal_loss/gt1r",
    "/ancillary_data/calibrations/dead_time_radiometric_signal_loss/gt1r",
    "/ancillary_data/calibrations/dead_time_radiometric_signal_loss/gt1r",
    "/ancillary_data/calibrations/dead_time_radiometric_signal_loss/gt1r",
    "/ancillary_data/calibrations/dead_time_radiometric_signal_loss/gt2l",
    "/ancillary_data/calibrations/dead_time_radiometric_signal_loss/gt2l",
    "/ancillary_data/calibrations/dead_time_radiometric_signal_loss/gt2l",
    "/ancillary_data/calibrations/dead_time_radiometric_signal_loss/gt2l",
    "/ancillary_data/calibrations/dead_time_radiometric_signal_loss/gt2r",
    "/ancillary_data/calibrations/dead_time_radiometric_signal_loss/gt2r",
    "/ancillary_data/calibrations/dead_time_radiometric_signal_loss/gt2r",
    "/ancillary_data/calibrations/dead_time_radiometric_signal_loss/gt2r",
    "/ancillary_data/calibrations/dead_time_radiometric_signal_loss/gt3l",
    "/ancillary_data/calibrations/dead_time_radiometric_signal_loss/gt3l",
    "/ancillary_data/calibrations/dead_time_radiometric_signal_loss/gt3l",
    "/ancillary_data/calibrations/dead_time_radiometric_signal_loss/gt3l",
    "/ancillary_data/calibrations/dead_time_radiometric_signal_loss/gt3r",
    "/ancillary_data/calibrations/dead_time_radiometric_signal_loss/gt3r",
    "/ancillary_data/calibrations/dead_time_radiometric_signal_loss/gt3r",
    "/ancillary_data/calibrations/dead_time_radiometric_signal_loss/gt3r"
  ],
  "delta_t_gap_min": [
    "/ancillary_data/gt1l/signal_find_input/delta_t_gap_min",
    "/ancillary_data/gt1r/signal_find_input/delta_t_gap_min",
    "/ancillary_data/gt2l/signal_find_input/delta_t_gap_min",
    "/ancillary_data/gt2r/signal_find_input/delta_t_gap_min",
    "/ancillary_data/gt3l/signal_find_input/delta_t_gap_min",
    "/ancillary_data/gt3r/signal_find_input/delta_t_gap_min"
  ],
  "delta_t_lin_fit": [
    "/ancillary_data/gt1l/signal_find_input/delta_t_lin_fit",
    "/ancillary_data/gt1r/signal_find_input/delta_t_lin_fit",
    "/ancillary_data/gt2l/signal_find_input/delta_t_lin_fit",
    "/ancillary_data/gt2r/signal_find_input/delta_t_lin_fit",
    "/ancillary_data/gt3l/signal_find_input/delta_t_lin_fit",
    "/ancillary_data/gt3r/signal_find_input/delta_t_lin_fit"
  ],
  "delta_t_max": [
    "/ancillary_data/gt1l/signal_find_input/delta_t_max",
    "/ancillary_data/gt1r/signal_find_input/delta_t_max",
    "/ancillary_data/gt2l/signal_find_input/delta_t_max",
    "/ancillary_data/gt2r/signal_find_input/delta_t_max",
    "/ancillary_data/gt3l/signal_find_input/delta_t_max",
    "/ancillary_data/gt3r/signal_find_input/delta_t_max"
  ],
  "delta_t_min": [
    "/ancillary_data/gt1l/signal_find_input/delta_t_min",
    "/ancillary_data/gt1r/signal_find_input/delta_t_min",
    "/ancillary_data/gt2l/signal_find_input/delta_t_min",
    "/ancillary_data/gt2r/signal_find_input/delta_t_min",
    "/ancillary_data/gt3l/signal_find_input/delta_t_min",
    "/ancillary_data/gt3r/signal_find_input/delta_t_min"
  ],
  "delta_time": [
    "/gt1l/bckgrd_atlas/delta_time",
    "/gt1l/geolocation/delta_time",
    "/gt1l/geophys_corr/delta_time",
    "/gt1l/heights/delta_time",
    "/gt1l/signal_find_output/inlandwater/delta_time",
    "/gt1l/signal_find_output/land/delta_time",
    "/gt1l/signal_find_output/ocean/delta_time",
    "/gt1l/signal_find_output/sea_ice/delta_time",
    "/gt1r/bckgrd_atlas/delta_time",
    "/gt1r/geolocation/delta_time",
    "/gt1r/geophys_corr/delta_time",
    "/gt1r/heights/delta_time",
    "/gt1r/signal_find_output/inlandwater/delta_time",
    "/gt1r/signal_find_output/land/delta_time",
    "/gt1r/signal_find_output/ocean/delta_time",
    "/gt1r/signal_find_output/sea_ice/delta_time",
    "/gt2l/bckgrd_atlas/delta_time",
    "/gt2l/geolocation/delta_time",
    "/gt2l/geophys_corr/delta_time",
    "/gt2l/heights/delta_time",
    "/gt2l/signal_find_output/inlandwater/delta_time",
    "/gt2l/signal_find_output/land/delta_time",
    "/gt2l/signal_find_output/ocean/delta_time",
    "/gt2l/signal_find_output/sea_ice/delta_time",
    "/gt2r/bckgrd_atlas/delta_time",
    "/gt2r/geolocation/delta_time",
    "/gt2r/geophys_corr/delta_time",
    "/gt2r/heights/delta_time",
    "/gt2r/signal_find_output/inlandwater/delta_time",
    "/gt2r/signal_find_output/land/delta_time",
    "/gt2r/signal_find_output/ocean/delta_time",
    "/gt2r/signal_find_output/sea_ice/delta_time",
    "/gt3l/bckgrd_atlas/delta_time",
    "/gt3l/geolocation/delta_time",
    "/gt3l/geophys_corr/delta_time",
    "/gt3l/heights/delta_time",
    "/gt3l/signal_find_output/inlandwater/delta_time",
    "/gt3l/signal_find_output/land/delta_time",
    "/gt3l/signal_find_output/ocean/delta_time",
    "/gt3l/signal_find_output/sea_ice/delta_time",
    "/gt3r/bckgrd_atlas/delta_time",
    "/gt3r/geolocation/delta_time",
    "/gt3r/geophys_corr/delta_time",
    "/gt3r/heights/delta_time",
    "/gt3r/signal_find_output/inlandwater/delta_time",
    "/gt3r/signal_find_output/land/delta_time",
    "/gt3r/signal_find_output/ocean/delta_time",
    "/gt3r/signal_find_output/sea_ice/delta_time",
    "/quality_assessment/delta_time"
  ],
  "delta_z_bg": [
    "/ancillary_data/gt1l/signal_find_input/delta_z_bg",
    "/ancillary_data/gt1r/signal_find_input/delta_z_bg",
    "/ancillary_data/gt2l/signal_find_input/delta_z_bg",
    "/ancillary_data/gt2r/signal_find_input/delta_z_bg",
    "/ancillary_data/gt3l/signal_find_input/delta_z_bg",
    "/ancillary_data/gt3r/signal_find_input/delta_z_bg"
  ],
  "delta_zmax2": [
    "/ancillary_data/gt1l/signal_find_input/delta_zmax2",
    "/ancillary_data/gt1r/signal_find_input/delta_zmax2",
    "/ancillary_data/gt2l/signal_find_input/delta_zmax2",
    "/ancillary_data/gt2r/signal_find_input/delta_zmax2",
    "/ancillary_data/gt3l/signal_find_input/delta_zmax2",
    "/ancillary_data/gt3r/signal_find_input/delta_zmax2"
  ],
  "delta_zmin": [
    "/ancillary_data/gt1l/signal_find_input/delta_zmin",
    "/ancillary_data/gt1r/signal_find_input/delta_zmin",
    "/ancillary_data/gt2l/signal_find_input/delta_zmin",
    "/ancillary_data/gt2r/signal_find_input/delta_zmin",
    "/ancillary_data/gt3l/signal_find_input/delta_zmin",
    "/ancillary_data/gt3r/signal_find_input/delta_zmin"
  ],
  "dem_flag": [
    "/gt1l/geophys_corr/dem_flag",
    "/gt1r/geophys_corr/dem_flag",
    "/gt2l/geophys_corr/dem_flag",
    "/gt2r/geophys_corr/dem_flag",
    "/gt3l/geophys_corr/dem_flag",
    "/gt3r/geophys_corr/dem_flag"
  ],
  "dem_h": [
    "/gt1l/geophys_corr/dem_h",
    "/gt1r/geophys_corr/dem_h",
    "/gt2l/geophys_corr/dem_h",
    "/gt2r/geophys_corr/dem_h",
    "/gt3l/geophys_corr/dem_h",
    "/gt3r/geophys_corr/dem_h"
  ],
  "det_ab_flag": [
    "/ancillary_data/atlas_engineering/det_ab_flag"
  ],
  "dist_ph_across": [
    "/gt1l/heights/dist_ph_across",
    "/gt1r/heights/dist_ph_across",
    "/gt2l/heights/dist_ph_across",
    "/gt2r/heights/dist_ph_across",
    "/gt3l/heights/dist_ph_across",
    "/gt3r/heights/dist_ph_across"
  ],
  "dist_ph_along": [
    "/gt1l/heights/dist_ph_along",
    "/gt1r/heights/dist_ph_along",
    "/gt2l/heights/dist_ph_along",
    "/gt2r/heights/dist_ph_along",
    "/gt3l/heights/dist_ph_along",
    "/gt3r/heights/dist_ph_along"
  ],
  "ds_channel": [
    "/ancillary_data/calibrations/ds_channel"
  ],
  "ds_gt": [
    "/ancillary_data/atlas_engineering/ds_gt",
    "/ancillary_data/tep/ds_gt"
  ],
  "ds_stat": [
    "/ancillary_data/atlas_engineering/ds_stat"
  ],
  "ds_surf_type": [
    "//ds_surf_type"
  ],
  "ds_xyz": [
    "//ds_xyz"
  ],
  "e_a": [
    "/ancillary_data/gt1l/signal_find_input/e_a",
    "/ancillary_data/gt1r/signal_find_input/e_a",
    "/ancillary_data/gt2l/signal_find_input/e_a",
    "/ancillary_data/gt2r/signal_find_input/e_a",
    "/ancillary_data/gt3l/signal_find_input/e_a",
    "/ancillary_data/gt3r/signal_find_input/e_a"
  ],
  "e_linfit_edit": [
    "/ancillary_data/gt1l/signal_find_input/e_linfit_edit",
    "/ancillary_data/gt1r/signal_find_input/e_linfit_edit",
    "/ancillary_data/gt2l/signal_find_input/e_linfit_edit",
    "/ancillary_data/gt2r/signal_find_input/e_linfit_edit",
    "/ancillary_data/gt3l/signal_find_input/e_linfit_edit",
    "/ancillary_data/gt3r/signal_find_input/e_linfit_edit"
  ],
  "e_linfit_slant": [
    "/ancillary_data/gt1l/signal_find_input/e_linfit_slant",
    "/ancillary_data/gt1r/signal_find_input/e_linfit_slant",
    "/ancillary_data/gt2l/signal_find_input/e_linfit_slant",
    "/ancillary_data/gt2r/signal_find_input/e_linfit_slant",
    "/ancillary_data/gt3l/signal_find_input/e_linfit_slant",
    "/ancillary_data/gt3r/signal_find_input/e_linfit_slant"
  ],
  "e_m": [
    "/ancillary_data/gt1l/signal_find_input/e_m",
    "/ancillary_data/gt1r/signal_find_input/e_m",
    "/ancillary_data/gt2l/signal_find_input/e_m",
    "/ancillary_data/gt2r/signal_find_input/e_m",
    "/ancillary_data/gt3l/signal_find_input/e_m",
    "/ancillary_data/gt3r/signal_find_input/e_m"
  ],
  "e_m_mult": [
    "/ancillary_data/gt1l/signal_find_input/e_m_mult",
    "/ancillary_data/gt1r/signal_find_input/e_m_mult",
    "/ancillary_data/gt2l/signal_find_input/e_m_mult",
    "/ancillary_data/gt2r/signal_find_input/e_m_mult",
    "/ancillary_data/gt3l/signal_find_input/e_m_mult",
    "/ancillary_data/gt3r/signal_find_input/e_m_mult"
  ],
  "end_cycle": [
    "/ancillary_data/end_cycle"
  ],
  "end_delta_time": [
    "/ancillary_data/end_delta_time"
  ],
  "end_geoseg": [
    "/ancillary_data/end_geoseg"
  ],
  "end_gpssow": [
    "/ancillary_data/end_gpssow"
  ],
  "end_gpsweek": [
    "/ancillary_data/end_gpsweek"
  ],
  "end_orbit": [
    "/ancillary_data/end_orbit"
  ],
  "end_region": [
    "/ancillary_data/end_region"
  ],
  "end_rgt": [
    "/ancillary_data/end_rgt"
  ],
  "first_photon_bias": [
    "/ancillary_data/calibrations/first_photon_bias",
    "/ancillary_data/calibrations/first_photon_bias/gt1l",
    "/ancillary_data/calibrations/first_photon_bias/gt1l",
    "/ancillary_data/calibrations/first_photon_bias/gt1l",
    "/ancillary_data/calibrations/first_photon_bias/gt1l",
    "/ancillary_data/calibrations/first_photon_bias/gt1r",
    "/ancillary_data/calibrations/first_photon_bias/gt1r",
    "/ancillary_data/calibrations/first_photon_bias/gt1r",
    "/ancillary_data/calibrations/first_photon_bias/gt1r",
    "/ancillary_data/calibrations/first_photon_bias/gt2l",
    "/ancillary_data/calibrations/first_photon_bias/gt2l",
    "/ancillary_data/calibrations/first_photon_bias/gt2l",
    "/ancillary_data/calibrations/first_photon_bias/gt2l",
    "/ancillary_data/calibrations/first_photon_bias/gt2r",
    "/ancillary_data/calibrations/first_photon_bias/gt2r",
    "/ancillary_data/calibrations/first_photon_bias/gt2r",
    "/ancillary_data/calibrations/first_photon_bias/gt2r",
    "/ancillary_data/calibrations/first_photon_bias/gt3l",
    "/ancillary_data/calibrations/first_photon_bias/gt3l",
    "/ancillary_data/calibrations/first_photon_bias/gt3l",
    "/ancillary_data/calibrations/first_photon_bias/gt3l",
    "/ancillary_data/calibrations/first_photon_bias/gt3r",
    "/ancillary_data/calibrations/first_photon_bias/gt3r",
    "/ancillary_data/calibrations/first_photon_bias/gt3r",
    "/ancillary_data/calibrations/first_photon_bias/gt3r"
  ],
  "full_sat_fract": [
    "/gt1l/geolocation/full_sat_fract",
    "/gt1r/geolocation/full_sat_fract",
    "/gt2l/geolocation/full_sat_fract",
    "/gt2r/geolocation/full_sat_fract",
    "/gt3l/geolocation/full_sat_fract",
    "/gt3r/geolocation/full_sat_fract"
  ],
  "geoid": [
    "/gt1l/geophys_corr/geoid",
    "/gt1r/geophys_corr/geoid",
    "/gt2l/geophys_corr/geoid",
    "/gt2r/geophys_corr/geoid",
    "/gt3l/geophys_corr/geoid",
    "/gt3r/geophys_corr/geoid"
  ],
  "geoid_free2mean": [
    "/gt1l/geophys_corr/geoid_free2mean",
    "/gt1r/geophys_corr/geoid_free2mean",
    "/gt2l/geophys_corr/geoid_free2mean",
    "/gt2r/geophys_corr/geoid_free2mean",
    "/gt3l/geophys_corr/geoid_free2mean",
    "/gt3r/geophys_corr/geoid_free2mean"
  ],
  "geolocation": [
    "/gt1l/geolocation",
    "/gt1r/geolocation",
    "/gt2l/geolocation",
    "/gt2r/geolocation",
    "/gt3l/geolocation",
    "/gt3r/geolocation"
  ],
  "geophys_corr": [
    "/gt1l/geophys_corr",
    "/gt1r/geophys_corr",
    "/gt2l/geophys_corr",
    "/gt2r/geophys_corr",
    "/gt3l/geophys_corr",
    "/gt3r/geophys_corr"
  ],
  "granule_end_utc": [
    "/ancillary_data/granule_end_utc"
  ],
  "granule_start_utc": [
    "/ancillary_data/granule_start_utc"
  ],
  "h_ph": [
    "/gt1l/heights/h_ph",
    "/gt1r/heights/h_ph",
    "/gt2l/heights/h_ph",
    "/gt2r/heights/h_ph",
    "/gt3l/heights/h_ph",
    "/gt3r/heights/h_ph"
  ],
  "heights": [
    "/gt1l/heights",
    "/gt1r/heights",
    "/gt2l/heights",
    "/gt2r/heights",
    "/gt3l/heights",
    "/gt3r/heights"
  ],
  "hist_x": [
    "/ancillary_data/calibrations/low_link_impulse_response/hist_x"
  ],
  "htspanmin": [
    "/ancillary_data/gt1l/signal_find_input/htspanmin",
    "/ancillary_data/gt1r/signal_find_input/htspanmin",
    "/ancillary_data/gt2l/signal_find_input/htspanmin",
    "/ancillary_data/gt2r/signal_find_input/htspanmin",
    "/ancillary_data/gt3l/signal_find_input/htspanmin",
    "/ancillary_data/gt3r/signal_find_input/htspanmin"
  ],
  "hvpc_ab_flag": [
    "/ancillary_data/atlas_engineering/hvpc_ab_flag"
  ],
  "inlandwater": [
    "/gt1l/signal_find_output/inlandwater",
    "/gt1r/signal_find_output/inlandwater",
    "/gt2l/signal_find_output/inlandwater",
    "/gt2r/signal_find_output/inlandwater",
    "/gt3l/signal_find_output/inlandwater",
    "/gt3r/signal_find_output/inlandwater"
  ],
  "lan": [
    "/orbit_info/lan"
  ],
  "land": [
    "/gt1l/signal_find_output/land",
    "/gt1r/signal_find_output/land",
    "/gt2l/signal_find_output/land",
    "/gt2r/signal_find_output/land",
    "/gt3l/signal_find_output/land",
    "/gt3r/signal_find_output/land"
  ],
  "laser": [
    "/ancillary_data/calibrations/low_link_impulse_response/laser"
  ],
  "laser_12_flag": [
    "/ancillary_data/atlas_engineering/laser_12_flag"
  ],
  "lat_ph": [
    "/gt1l/heights/lat_ph",
    "/gt1r/heights/lat_ph",
    "/gt2l/heights/lat_ph",
    "/gt2r/heights/lat_ph",
    "/gt3l/heights/lat_ph",
    "/gt3r/heights/lat_ph"
  ],
  "lon_ph": [
    "/gt1l/heights/lon_ph",
    "/gt1r/heights/lon_ph",
    "/gt2l/heights/lon_ph",
    "/gt2r/heights/lon_ph",
    "/gt3l/heights/lon_ph",
    "/gt3r/heights/lon_ph"
  ],
  "low_link_impulse_response": [
    "/ancillary_data/calibrations/low_link_impulse_response",
    "/ancillary_data/calibrations/low_link_impulse_response/gt1l",
    "/ancillary_data/calibrations/low_link_impulse_response/gt1l",
    "/ancillary_data/calibrations/low_link_impulse_response/gt1r",
    "/ancillary_data/calibrations/low_link_impulse_response/gt1r",
    "/ancillary_data/calibrations/low_link_impulse_response/gt2l",
    "/ancillary_data/calibrations/low_link_impulse_response/gt2l",
    "/ancillary_data/calibrations/low_link_impulse_response/gt2r",
    "/ancillary_data/calibrations/low_link_impulse_response/gt2r",
    "/ancillary_data/calibrations/low_link_impulse_response/gt3l",
    "/ancillary_data/calibrations/low_link_impulse_response/gt3l",
    "/ancillary_data/calibrations/low_link_impulse_response/gt3r",
    "/ancillary_data/calibrations/low_link_impulse_response/gt3r"
  ],
  "lrs_ab_flag": [
    "/ancillary_data/atlas_engineering/lrs_ab_flag"
  ],
  "lslant_flag": [
    "/ancillary_data/gt1l/signal_find_input/lslant_flag",
    "/ancillary_data/gt1r/signal_find_input/lslant_flag",
    "/ancillary_data/gt2l/signal_find_input/lslant_flag",
    "/ancillary_data/gt2r/signal_find_input/lslant_flag",
    "/ancillary_data/gt3l/signal_find_input/lslant_flag",
    "/ancillary_data/gt3r/signal_find_input/lslant_flag"
  ],
  "min_fit_time_fact": [
    "/ancillary_data/gt1l/signal_find_input/min_fit_time_fact",
    "/ancillary_data/gt1r/signal_find_input/min_fit_time_fact",
    "/ancillary_data/gt2l/signal_find_input/min_fit_time_fact",
    "/ancillary_data/gt2r/signal_find_input/min_fit_time_fact",
    "/ancillary_data/gt3l/signal_find_input/min_fit_time_fact",
    "/ancillary_data/gt3r/signal_find_input/min_fit_time_fact"
  ],
  "min_full_sat": [
    "/ancillary_data/altimetry/min_full_sat"
  ],
  "min_near_sat": [
    "/ancillary_data/altimetry/min_near_sat"
  ],
  "min_sat_h": [
    "/ancillary_data/altimetry/min_sat_h"
  ],
  "min_tep_ph": [
    "/ancillary_data/tep/min_tep_ph"
  ],
  "min_tep_secs": [
    "/ancillary_data/tep/min_tep_secs"
  ],
  "mode": [
    "/ancillary_data/calibrations/low_link_impulse_response/mode"
  ],
  "n_delta_z1": [
    "/ancillary_data/gt1l/signal_find_input/n_delta_z1",
    "/ancillary_data/gt1r/signal_find_input/n_delta_z1",
    "/ancillary_data/gt2l/signal_find_input/n_delta_z1",
    "/ancillary_data/gt2r/signal_find_input/n_delta_z1",
    "/ancillary_data/gt3l/signal_find_input/n_delta_z1",
    "/ancillary_data/gt3r/signal_find_input/n_delta_z1"
  ],
  "n_delta_z2": [
    "/ancillary_data/gt1l/signal_find_input/n_delta_z2",
    "/ancillary_data/gt1r/signal_find_input/n_delta_z2",
    "/ancillary_data/gt2l/signal_find_input/n_delta_z2",
    "/ancillary_data/gt2r/signal_find_input/n_delta_z2",
    "/ancillary_data/gt3l/signal_find_input/n_delta_z2",
    "/ancillary_data/gt3r/signal_find_input/n_delta_z2"
  ],
  "n_tep_bins": [
    "/ancillary_data/tep/n_tep_bins"
  ],
  "nbin_min": [
    "/ancillary_data/gt1l/signal_find_input/nbin_min",
    "/ancillary_data/gt1r/signal_find_input/nbin_min",
    "/ancillary_data/gt2l/signal_find_input/nbin_min",
    "/ancillary_data/gt2r/signal_find_input/nbin_min",
    "/ancillary_data/gt3l/signal_find_input/nbin_min",
    "/ancillary_data/gt3r/signal_find_input/nbin_min"
  ],
  "near_sat_fract": [
    "/gt1l/geolocation/near_sat_fract",
    "/gt1r/geolocation/near_sat_fract",
    "/gt2l/geolocation/near_sat_fract",
    "/gt2r/geolocation/near_sat_fract",
    "/gt3l/geolocation/near_sat_fract",
    "/gt3r/geolocation/near_sat_fract"
  ],
  "neutat_delay_derivative": [
    "/gt1l/geolocation/neutat_delay_derivative",
    "/gt1r/geolocation/neutat_delay_derivative",
    "/gt2l/geolocation/neutat_delay_derivative",
    "/gt2r/geolocation/neutat_delay_derivative",
    "/gt3l/geolocation/neutat_delay_derivative",
    "/gt3r/geolocation/neutat_delay_derivative"
  ],
  "neutat_delay_total": [
    "/gt1l/geolocation/neutat_delay_total",
    "/gt1r/geolocation/neutat_delay_total",
    "/gt2l/geolocation/neutat_delay_total",
    "/gt2r/geolocation/neutat_delay_total",
    "/gt3l/geolocation/neutat_delay_total",
    "/gt3r/geolocation/neutat_delay_total"
  ],
  "neutat_ht": [
    "/gt1l/geolocation/neutat_ht",
    "/gt1r/geolocation/neutat_ht",
    "/gt2l/geolocation/neutat_ht",
    "/gt2r/geolocation/neutat_ht",
    "/gt3l/geolocation/neutat_ht",
    "/gt3r/geolocation/neutat_ht"
  ],
  "nphot_min": [
    "/ancillary_data/gt1l/signal_find_input/nphot_min",
    "/ancillary_data/gt1r/signal_find_input/nphot_min",
    "/ancillary_data/gt2l/signal_find_input/nphot_min",
    "/ancillary_data/gt2r/signal_find_input/nphot_min",
    "/ancillary_data/gt3l/signal_find_input/nphot_min",
    "/ancillary_data/gt3r/signal_find_input/nphot_min"
  ],
  "nslw": [
    "/ancillary_data/gt1l/signal_find_input/nslw",
    "/ancillary_data/gt1r/signal_find_input/nslw",
    "/ancillary_data/gt2l/signal_find_input/nslw",
    "/ancillary_data/gt2r/signal_find_input/nslw",
    "/ancillary_data/gt3l/signal_find_input/nslw",
    "/ancillary_data/gt3r/signal_find_input/nslw"
  ],
  "nslw_v": [
    "/ancillary_data/gt1l/signal_find_input/nslw_v",
    "/ancillary_data/gt1r/signal_find_input/nslw_v",
    "/ancillary_data/gt2l/signal_find_input/nslw_v",
    "/ancillary_data/gt2r/signal_find_input/nslw_v",
    "/ancillary_data/gt3l/signal_find_input/nslw_v",
    "/ancillary_data/gt3r/signal_find_input/nslw_v"
  ],
  "num_bins": [
    "/ancillary_data/calibrations/low_link_impulse_response/num_bins"
  ],
  "ocean": [
    "/gt1l/signal_find_output/ocean",
    "/gt1r/signal_find_output/ocean",
    "/gt2l/signal_find_output/ocean",
    "/gt2r/signal_find_output/ocean",
    "/gt3l/signal_find_output/ocean",
    "/gt3r/signal_find_output/ocean"
  ],
  "orbit_info": [
    "/orbit_info"
  ],
  "orbit_number": [
    "/orbit_info/orbit_number"
  ],
  "out_edit_flag": [
    "/ancillary_data/gt1l/signal_find_input/out_edit_flag",
    "/ancillary_data/gt1r/signal_find_input/out_edit_flag",
    "/ancillary_data/gt2l/signal_find_input/out_edit_flag",
    "/ancillary_data/gt2r/signal_find_input/out_edit_flag",
    "/ancillary_data/gt3l/signal_find_input/out_edit_flag",
    "/ancillary_data/gt3r/signal_find_input/out_edit_flag"
  ],
  "pc_bckgrd_flag": [
    "/ancillary_data/gt1l/signal_find_input/pc_bckgrd_flag",
    "/ancillary_data/gt1r/signal_find_input/pc_bckgrd_flag",
    "/ancillary_data/gt2l/signal_find_input/pc_bckgrd_flag",
    "/ancillary_data/gt2r/signal_find_input/pc_bckgrd_flag",
    "/ancillary_data/gt3l/signal_find_input/pc_bckgrd_flag",
    "/ancillary_data/gt3r/signal_find_input/pc_bckgrd_flag"
  ],
  "pce1_spot1": [
    "/atlas_impulse_response/pce1_spot1"
  ],
  "pce2_spot3": [
    "/atlas_impulse_response/pce2_spot3"
  ],
  "pce_mframe_cnt": [
    "/gt1l/bckgrd_atlas/pce_mframe_cnt",
    "/gt1l/heights/pce_mframe_cnt",
    "/gt1r/bckgrd_atlas/pce_mframe_cnt",
    "/gt1r/heights/pce_mframe_cnt",
    "/gt2l/bckgrd_atlas/pce_mframe_cnt",
    "/gt2l/heights/pce_mframe_cnt",
    "/gt2r/bckgrd_atlas/pce_mframe_cnt",
    "/gt2r/heights/pce_mframe_cnt",
    "/gt3l/bckgrd_atlas/pce_mframe_cnt",
    "/gt3l/heights/pce_mframe_cnt",
    "/gt3r/bckgrd_atlas/pce_mframe_cnt",
    "/gt3r/heights/pce_mframe_cnt"
  ],
  "pdu_ab_flag": [
    "/ancillary_data/atlas_engineering/pdu_ab_flag"
  ],
  "ph_id_channel": [
    "/gt1l/heights/ph_id_channel",
    "/gt1r/heights/ph_id_channel",
    "/gt2l/heights/ph_id_channel",
    "/gt2r/heights/ph_id_channel",
    "/gt3l/heights/ph_id_channel",
    "/gt3r/heights/ph_id_channel"
  ],
  "ph_id_count": [
    "/gt1l/heights/ph_id_count",
    "/gt1r/heights/ph_id_count",
    "/gt2l/heights/ph_id_count",
    "/gt2r/heights/ph_id_count",
    "/gt3l/heights/ph_id_count",
    "/gt3r/heights/ph_id_count"
  ],
  "ph_id_pulse": [
    "/gt1l/heights/ph_id_pulse",
    "/gt1r/heights/ph_id_pulse",
    "/gt2l/heights/ph_id_pulse",
    "/gt2r/heights/ph_id_pulse",
    "/gt3l/heights/ph_id_pulse",
    "/gt3r/heights/ph_id_pulse"
  ],
  "ph_index_beg": [
    "/gt1l/geolocation/ph_index_beg",
    "/gt1r/geolocation/ph_index_beg",
    "/gt2l/geolocation/ph_index_beg",
    "/gt2r/geolocation/ph_index_beg",
    "/gt3l/geolocation/ph_index_beg",
    "/gt3r/geolocation/ph_index_beg"
  ],
  "ph_sat_flag": [
    "/ancillary_data/altimetry/ph_sat_flag"
  ],
  "ph_sat_lb": [
    "/ancillary_data/altimetry/ph_sat_lb"
  ],
  "ph_sat_ub": [
    "/ancillary_data/altimetry/ph_sat_ub"
  ],
  "ph_uncorrelated_error": [
    "/ancillary_data/atlas_engineering/ph_uncorrelated_error"
  ],
  "pitch": [
    "/gt1l/geolocation/pitch",
    "/gt1r/geolocation/pitch",
    "/gt2l/geolocation/pitch",
    "/gt2r/geolocation/pitch",
    "/gt3l/geolocation/pitch",
    "/gt3r/geolocation/pitch"
  ],
  "podppd_flag": [
    "/gt1l/geolocation/podppd_flag",
    "/gt1r/geolocation/podppd_flag",
    "/gt2l/geolocation/podppd_flag",
    "/gt2r/geolocation/podppd_flag",
    "/gt3l/geolocation/podppd_flag",
    "/gt3r/geolocation/podppd_flag"
  ],
  "podppd_pad": [
    "/ancillary_data/altimetry/podppd_pad"
  ],
  "qa_granule_fail_reason": [
    "/quality_assessment/qa_granule_fail_reason"
  ],
  "qa_granule_pass_fail": [
    "/quality_assessment/qa_granule_pass_fail"
  ],
  "qa_perc_signal_conf_ph_high": [
    "/quality_assessment/gt1l/qa_perc_signal_conf_ph_high",
    "/quality_assessment/gt1r/qa_perc_signal_conf_ph_high",
    "/quality_assessment/gt2l/qa_perc_signal_conf_ph_high",
    "/quality_assessment/gt2r/qa_perc_signal_conf_ph_high",
    "/quality_assessment/gt3l/qa_perc_signal_conf_ph_high",
    "/quality_assessment/gt3r/qa_perc_signal_conf_ph_high"
  ],
  "qa_perc_signal_conf_ph_low": [
    "/quality_assessment/gt1l/qa_perc_signal_conf_ph_low",
    "/quality_assessment/gt1r/qa_perc_signal_conf_ph_low",
    "/quality_assessment/gt2l/qa_perc_signal_conf_ph_low",
    "/quality_assessment/gt2r/qa_perc_signal_conf_ph_low",
    "/quality_assessment/gt3l/qa_perc_signal_conf_ph_low",
    "/quality_assessment/gt3r/qa_perc_signal_conf_ph_low"
  ],
  "qa_perc_signal_conf_ph_med": [
    "/quality_assessment/gt1l/qa_perc_signal_conf_ph_med",
    "/quality_assessment/gt1r/qa_perc_signal_conf_ph_med",
    "/quality_assessment/gt2l/qa_perc_signal_conf_ph_med",
    "/quality_assessment/gt2r/qa_perc_signal_conf_ph_med",
    "/quality_assessment/gt3l/qa_perc_signal_conf_ph_med",
    "/quality_assessment/gt3r/qa_perc_signal_conf_ph_med"
  ],
  "qa_perc_surf_type": [
    "/quality_assessment/gt1l/qa_perc_surf_type",
    "/quality_assessment/gt1r/qa_perc_surf_type",
    "/quality_assessment/gt2l/qa_perc_surf_type",
    "/quality_assessment/gt2r/qa_perc_surf_type",
    "/quality_assessment/gt3l/qa_perc_surf_type",
    "/quality_assessment/gt3r/qa_perc_surf_type"
  ],
  "qa_total_signal_conf_ph_high": [
    "/quality_assessment/gt1l/qa_total_signal_conf_ph_high",
    "/quality_assessment/gt1r/qa_total_signal_conf_ph_high",
    "/quality_assessment/gt2l/qa_total_signal_conf_ph_high",
    "/quality_assessment/gt2r/qa_total_signal_conf_ph_high",
    "/quality_assessment/gt3l/qa_total_signal_conf_ph_high",
    "/quality_assessment/gt3r/qa_total_signal_conf_ph_high"
  ],
  "qa_total_signal_conf_ph_low": [
    "/quality_assessment/gt1l/qa_total_signal_conf_ph_low",
    "/quality_assessment/gt1r/qa_total_signal_conf_ph_low",
    "/quality_assessment/gt2l/qa_total_signal_conf_ph_low",
    "/quality_assessment/gt2r/qa_total_signal_conf_ph_low",
    "/quality_assessment/gt3l/qa_total_signal_conf_ph_low",
    "/quality_assessment/gt3r/qa_total_signal_conf_ph_low"
  ],
  "qa_total_signal_conf_ph_med": [
    "/quality_assessment/gt1l/qa_total_signal_conf_ph_med",
    "/quality_assessment/gt1r/qa_total_signal_conf_ph_med",
    "/quality_assessment/gt2l/qa_total_signal_conf_ph_med",
    "/quality_assessment/gt2r/qa_total_signal_conf_ph_med",
    "/quality_assessment/gt3l/qa_total_signal_conf_ph_med",
    "/quality_assessment/gt3r/qa_total_signal_conf_ph_med"
  ],
  "quality_assessment": [
    "/quality_assessment",
    "/quality_assessment/gt1l",
    "/quality_assessment/gt1r",
    "/quality_assessment/gt2l",
    "/quality_assessment/gt2r",
    "/quality_assessment/gt3l",
    "/quality_assessment/gt3r"
  ],
  "quality_ph": [
    "/gt1l/heights/quality_ph",
    "/gt1r/heights/quality_ph",
    "/gt2l/heights/quality_ph",
    "/gt2r/heights/quality_ph",
    "/gt3l/heights/quality_ph",
    "/gt3r/heights/quality_ph"
  ],
  "r": [
    "/ancillary_data/gt1l/signal_find_input/r",
    "/ancillary_data/gt1r/signal_find_input/r",
    "/ancillary_data/gt2l/signal_find_input/r",
    "/ancillary_data/gt2r/signal_find_input/r",
    "/ancillary_data/gt3l/signal_find_input/r",
    "/ancillary_data/gt3r/signal_find_input/r"
  ],
  "r2": [
    "/ancillary_data/gt1l/signal_find_input/r2",
    "/ancillary_data/gt1r/signal_find_input/r2",
    "/ancillary_data/gt2l/signal_find_input/r2",
    "/ancillary_data/gt2r/signal_find_input/r2",
    "/ancillary_data/gt3l/signal_find_input/r2",
    "/ancillary_data/gt3r/signal_find_input/r2"
  ],
  "range_bias_corr": [
    "/gt1l/geolocation/range_bias_corr",
    "/gt1r/geolocation/range_bias_corr",
    "/gt2l/geolocation/range_bias_corr",
    "/gt2r/geolocation/range_bias_corr",
    "/gt3l/geolocation/range_bias_corr",
    "/gt3r/geolocation/range_bias_corr"
  ],
  "receiver": [
    "/ancillary_data/atlas_engineering/receiver"
  ],
  "ref_azimuth": [
    "/gt1l/geolocation/ref_azimuth",
    "/gt1r/geolocation/ref_azimuth",
    "/gt2l/geolocation/ref_azimuth",
    "/gt2r/geolocation/ref_azimuth",
    "/gt3l/geolocation/ref_azimuth",
    "/gt3r/geolocation/ref_azimuth"
  ],
  "ref_elev": [
    "/gt1l/geolocation/ref_elev",
    "/gt1r/geolocation/ref_elev",
    "/gt2l/geolocation/ref_elev",
    "/gt2r/geolocation/ref_elev",
    "/gt3l/geolocation/ref_elev",
    "/gt3r/geolocation/ref_elev"
  ],
  "reference_photon_index": [
    "/gt1l/geolocation/reference_photon_index",
    "/gt1r/geolocation/reference_photon_index",
    "/gt2l/geolocation/reference_photon_index",
    "/gt2r/geolocation/reference_photon_index",
    "/gt3l/geolocation/reference_photon_index",
    "/gt3r/geolocation/reference_photon_index"
  ],
  "reference_photon_lat": [
    "/gt1l/geolocation/reference_photon_lat",
    "/gt1r/geolocation/reference_photon_lat",
    "/gt2l/geolocation/reference_photon_lat",
    "/gt2r/geolocation/reference_photon_lat",
    "/gt3l/geolocation/reference_photon_lat",
    "/gt3r/geolocation/reference_photon_lat"
  ],
  "reference_photon_lon": [
    "/gt1l/geolocation/reference_photon_lon",
    "/gt1r/geolocation/reference_photon_lon",
    "/gt2l/geolocation/reference_photon_lon",
    "/gt2r/geolocation/reference_photon_lon",
    "/gt3l/geolocation/reference_photon_lon",
    "/gt3r/geolocation/reference_photon_lon"
  ],
  "reference_tep_flag": [
    "/atlas_impulse_response/pce1_spot1/tep_histogram/reference_tep_flag",
    "/atlas_impulse_response/pce2_spot3/tep_histogram/reference_tep_flag"
  ],
  "release": [
    "/ancillary_data/release"
  ],
  "return_source": [
    "/ancillary_data/calibrations/low_link_impulse_response/return_source"
  ],
  "rgt": [
    "/orbit_info/rgt"
  ],
  "roll": [
    "/gt1l/geolocation/roll",
    "/gt1r/geolocation/roll",
    "/gt2l/geolocation/roll",
    "/gt2r/geolocation/roll",
    "/gt3l/geolocation/roll",
    "/gt3r/geolocation/roll"
  ],
  "rx_bckgrd_sensitivity": [
    "/ancillary_data/atlas_engineering/receiver/rx_bckgrd_sensitivity"
  ],
  "rx_return_sensitivity": [
    "/ancillary_data/atlas_engineering/receiver/rx_return_sensitivity"
  ],
  "sc_orient": [
    "/orbit_info/sc_orient"
  ],
  "sc_orient_time": [
    "/orbit_info/sc_orient_time"
  ],
  "sea_ice": [
    "/gt1l/signal_find_output/sea_ice",
    "/gt1r/signal_find_output/sea_ice",
    "/gt2l/signal_find_output/sea_ice",
    "/gt2r/signal_find_output/sea_ice",
    "/gt3l/signal_find_output/sea_ice",
    "/gt3r/signal_find_output/sea_ice"
  ],
  "segment_dist_x": [
    "/gt1l/geolocation/segment_dist_x",
    "/gt1r/geolocation/segment_dist_x",
    "/gt2l/geolocation/segment_dist_x",
    "/gt2r/geolocation/segment_dist_x",
    "/gt3l/geolocation/segment_dist_x",
    "/gt3r/geolocation/segment_dist_x"
  ],
  "segment_id": [
    "/gt1l/geolocation/segment_id",
    "/gt1r/geolocation/segment_id",
    "/gt2l/geolocation/segment_id",
    "/gt2r/geolocation/segment_id",
    "/gt3l/geolocation/segment_id",
    "/gt3r/geolocation/segment_id"
  ],
  "segment_length": [
    "/gt1l/geolocation/segment_length",
    "/gt1r/geolocation/segment_length",
    "/gt2l/geolocation/segment_length",
    "/gt2r/geolocation/segment_length",
    "/gt3l/geolocation/segment_length",
    "/gt3r/geolocation/segment_length"
  ],
  "segment_ph_cnt": [
    "/gt1l/geolocation/segment_ph_cnt",
    "/gt1r/geolocation/segment_ph_cnt",
    "/gt2l/geolocation/segment_ph_cnt",
    "/gt2r/geolocation/segment_ph_cnt",
    "/gt3l/geolocation/segment_ph_cnt",
    "/gt3r/geolocation/segment_ph_cnt"
  ],
  "side": [
    "/ancillary_data/calibrations/dead_time/side",
    "/ancillary_data/calibrations/low_link_impulse_response/side"
  ],
  "sig_find_t_inc": [
    "/ancillary_data/gt1l/signal_find_input/sig_find_t_inc",
    "/ancillary_data/gt1r/signal_find_input/sig_find_t_inc",
    "/ancillary_data/gt2l/signal_find_input/sig_find_t_inc",
    "/ancillary_data/gt2r/signal_find_input/sig_find_t_inc",
    "/ancillary_data/gt3l/signal_find_input/sig_find_t_inc",
    "/ancillary_data/gt3r/signal_find_input/sig_find_t_inc"
  ],
  "sigma_across": [
    "/gt1l/geolocation/sigma_across",
    "/gt1r/geolocation/sigma_across",
    "/gt2l/geolocation/sigma_across",
    "/gt2r/geolocation/sigma_across",
    "/gt3l/geolocation/sigma_across",
    "/gt3r/geolocation/sigma_across"
  ],
  "sigma_along": [
    "/gt1l/geolocation/sigma_along",
    "/gt1r/geolocation/sigma_along",
    "/gt2l/geolocation/sigma_along",
    "/gt2r/geolocation/sigma_along",
    "/gt3l/geolocation/sigma_along",
    "/gt3r/geolocation/sigma_along"
  ],
  "sigma_h": [
    "/gt1l/geolocation/sigma_h",
    "/gt1r/geolocation/sigma_h",
    "/gt2l/geolocation/sigma_h",
    "/gt2r/geolocation/sigma_h",
    "/gt3l/geolocation/sigma_h",
    "/gt3r/geolocation/sigma_h"
  ],
  "sigma_lat": [
    "/gt1l/geolocation/sigma_lat",
    "/gt1r/geolocation/sigma_lat",
    "/gt2l/geolocation/sigma_lat",
    "/gt2r/geolocation/sigma_lat",
    "/gt3l/geolocation/sigma_lat",
    "/gt3r/geolocation/sigma_lat"
  ],
  "sigma_lon": [
    "/gt1l/geolocation/sigma_lon",
    "/gt1r/geolocation/sigma_lon",
    "/gt2l/geolocation/sigma_lon",
    "/gt2r/geolocation/sigma_lon",
    "/gt3l/geolocation/sigma_lon",
    "/gt3r/geolocation/sigma_lon"
  ],
  "signal_conf_ph": [
    "/gt1l/heights/signal_conf_ph",
    "/gt1r/heights/signal_conf_ph",
    "/gt2l/heights/signal_conf_ph",
    "/gt2r/heights/signal_conf_ph",
    "/gt3l/heights/signal_conf_ph",
    "/gt3r/heights/signal_conf_ph"
  ],
  "signal_find_input": [
    "/ancillary_data/gt1l/signal_find_input",
    "/ancillary_data/gt1r/signal_find_input",
    "/ancillary_data/gt2l/signal_find_input",
    "/ancillary_data/gt2r/signal_find_input",
    "/ancillary_data/gt3l/signal_find_input",
    "/ancillary_data/gt3r/signal_find_input"
  ],
  "signal_find_output": [
    "/gt1l/signal_find_output",
    "/gt1r/signal_find_output",
    "/gt2l/signal_find_output",
    "/gt2r/signal_find_output",
    "/gt3l/signal_find_output",
    "/gt3r/signal_find_output"
  ],
  "snrlow": [
    "/ancillary_data/gt1l/signal_find_input/snrlow",
    "/ancillary_data/gt1r/signal_find_input/snrlow",
    "/ancillary_data/gt2l/signal_find_input/snrlow",
    "/ancillary_data/gt2r/signal_find_input/snrlow",
    "/ancillary_data/gt3l/signal_find_input/snrlow",
    "/ancillary_data/gt3r/signal_find_input/snrlow"
  ],
  "snrmed": [
    "/ancillary_data/gt1l/signal_find_input/snrmed",
    "/ancillary_data/gt1r/signal_find_input/snrmed",
    "/ancillary_data/gt2l/signal_find_input/snrmed",
    "/ancillary_data/gt2r/signal_find_input/snrmed",
    "/ancillary_data/gt3l/signal_find_input/snrmed",
    "/ancillary_data/gt3r/signal_find_input/snrmed"
  ],
  "solar_azimuth": [
    "/gt1l/geolocation/solar_azimuth",
    "/gt1r/geolocation/solar_azimuth",
    "/gt2l/geolocation/solar_azimuth",
    "/gt2r/geolocation/solar_azimuth",
    "/gt3l/geolocation/solar_azimuth",
    "/gt3r/geolocation/solar_azimuth"
  ],
  "solar_elevation": [
    "/gt1l/geolocation/solar_elevation",
    "/gt1r/geolocation/solar_elevation",
    "/gt2l/geolocation/solar_elevation",
    "/gt2r/geolocation/solar_elevation",
    "/gt3l/geolocation/solar_elevation",
    "/gt3r/geolocation/solar_elevation"
  ],
  "spd_ab_flag": [
    "/ancillary_data/atlas_engineering/spd_ab_flag"
  ],
  "start_cycle": [
    "/ancillary_data/start_cycle"
  ],
  "start_delta_time": [
    "/ancillary_data/start_delta_time"
  ],
  "start_geoseg": [
    "/ancillary_data/start_geoseg"
  ],
  "start_gpssow": [
    "/ancillary_data/start_gpssow"
  ],
  "start_gpsweek": [
    "/ancillary_data/start_gpsweek"
  ],
  "start_orbit": [
    "/ancillary_data/start_orbit"
  ],
  "start_region": [
    "/ancillary_data/start_region"
  ],
  "start_rgt": [
    "/ancillary_data/start_rgt"
  ],
  "surf_type": [
    "/gt1l/geolocation/surf_type",
    "/gt1r/geolocation/surf_type",
    "/gt2l/geolocation/surf_type",
    "/gt2r/geolocation/surf_type",
    "/gt3l/geolocation/surf_type",
    "/gt3r/geolocation/surf_type"
  ],
  "t_gap_big": [
    "/ancillary_data/gt1l/signal_find_input/t_gap_big",
    "/ancillary_data/gt1r/signal_find_input/t_gap_big",
    "/ancillary_data/gt2l/signal_find_input/t_gap_big",
    "/ancillary_data/gt2r/signal_find_input/t_gap_big",
    "/ancillary_data/gt3l/signal_find_input/t_gap_big",
    "/ancillary_data/gt3r/signal_find_input/t_gap_big"
  ],
  "t_pc_delta": [
    "/gt1l/signal_find_output/inlandwater/t_pc_delta",
    "/gt1l/signal_find_output/land/t_pc_delta",
    "/gt1l/signal_find_output/ocean/t_pc_delta",
    "/gt1l/signal_find_output/sea_ice/t_pc_delta",
    "/gt1r/signal_find_output/inlandwater/t_pc_delta",
    "/gt1r/signal_find_output/land/t_pc_delta",
    "/gt1r/signal_find_output/ocean/t_pc_delta",
    "/gt1r/signal_find_output/sea_ice/t_pc_delta",
    "/gt2l/signal_find_output/inlandwater/t_pc_delta",
    "/gt2l/signal_find_output/land/t_pc_delta",
    "/gt2l/signal_find_output/ocean/t_pc_delta",
    "/gt2l/signal_find_output/sea_ice/t_pc_delta",
    "/gt2r/signal_find_output/inlandwater/t_pc_delta",
    "/gt2r/signal_find_output/land/t_pc_delta",
    "/gt2r/signal_find_output/ocean/t_pc_delta",
    "/gt2r/signal_find_output/sea_ice/t_pc_delta",
    "/gt3l/signal_find_output/inlandwater/t_pc_delta",
    "/gt3l/signal_find_output/land/t_pc_delta",
    "/gt3l/signal_find_output/ocean/t_pc_delta",
    "/gt3l/signal_find_output/sea_ice/t_pc_delta",
    "/gt3r/signal_find_output/inlandwater/t_pc_delta",
    "/gt3r/signal_find_output/land/t_pc_delta",
    "/gt3r/signal_find_output/ocean/t_pc_delta",
    "/gt3r/signal_find_output/sea_ice/t_pc_delta"
  ],
  "tams_ab_flag": [
    "/ancillary_data/atlas_engineering/tams_ab_flag"
  ],
  "temperature": [
    "/ancillary_data/calibrations/dead_time/temperature",
    "/ancillary_data/calibrations/low_link_impulse_response/temperature"
  ],
  "tep": [
    "/ancillary_data/tep"
  ],
  "tep_bckgrd": [
    "/atlas_impulse_response/pce1_spot1/tep_histogram/tep_bckgrd",
    "/atlas_impulse_response/pce2_spot3/tep_histogram/tep_bckgrd"
  ],
  "tep_bin_size": [
    "/ancillary_data/tep/tep_bin_size"
  ],
  "tep_duration": [
    "/atlas_impulse_response/pce1_spot1/tep_histogram/tep_duration",
    "/atlas_impulse_response/pce2_spot3/tep_histogram/tep_duration"
  ],
  "tep_gap_size": [
    "/ancillary_data/tep/tep_gap_size"
  ],
  "tep_hist": [
    "/atlas_impulse_response/pce1_spot1/tep_histogram/tep_hist",
    "/atlas_impulse_response/pce2_spot3/tep_histogram/tep_hist"
  ],
  "tep_hist_sum": [
    "/atlas_impulse_response/pce1_spot1/tep_histogram/tep_hist_sum",
    "/atlas_impulse_response/pce2_spot3/tep_histogram/tep_hist_sum"
  ],
  "tep_hist_time": [
    "/atlas_impulse_response/pce1_spot1/tep_histogram/tep_hist_time",
    "/atlas_impulse_response/pce2_spot3/tep_histogram/tep_hist_time"
  ],
  "tep_histogram": [
    "/atlas_impulse_response/pce1_spot1/tep_histogram",
    "/atlas_impulse_response/pce2_spot3/tep_histogram"
  ],
  "tep_normalize": [
    "/ancillary_data/tep/tep_normalize"
  ],
  "tep_peak_bins": [
    "/ancillary_data/tep/tep_peak_bins"
  ],
  "tep_prim_window": [
    "/ancillary_data/tep/tep_prim_window"
  ],
  "tep_range_prim": [
    "/ancillary_data/tep/tep_range_prim"
  ],
  "tep_rm_noise": [
    "/ancillary_data/tep/tep_rm_noise"
  ],
  "tep_sec_window": [
    "/ancillary_data/tep/tep_sec_window"
  ],
  "tep_start_x": [
    "/ancillary_data/tep/tep_start_x"
  ],
  "tep_tod": [
    "/atlas_impulse_response/pce1_spot1/tep_histogram/tep_tod",
    "/atlas_impulse_response/pce2_spot3/tep_histogram/tep_tod"
  ],
  "tep_valid_spot": [
    "/ancillary_data/tep/tep_valid_spot"
  ],
  "tide_earth": [
    "/gt1l/geophys_corr/tide_earth",
    "/gt1r/geophys_corr/tide_earth",
    "/gt2l/geophys_corr/tide_earth",
    "/gt2r/geophys_corr/tide_earth",
    "/gt3l/geophys_corr/tide_earth",
    "/gt3r/geophys_corr/tide_earth"
  ],
  "tide_earth_free2mean": [
    "/gt1l/geophys_corr/tide_earth_free2mean",
    "/gt1r/geophys_corr/tide_earth_free2mean",
    "/gt2l/geophys_corr/tide_earth_free2mean",
    "/gt2r/geophys_corr/tide_earth_free2mean",
    "/gt3l/geophys_corr/tide_earth_free2mean",
    "/gt3r/geophys_corr/tide_earth_free2mean"
  ],
  "tide_equilibrium": [
    "/gt1l/geophys_corr/tide_equilibrium",
    "/gt1r/geophys_corr/tide_equilibrium",
    "/gt2l/geophys_corr/tide_equilibrium",
    "/gt2r/geophys_corr/tide_equilibrium",
    "/gt3l/geophys_corr/tide_equilibrium",
    "/gt3r/geophys_corr/tide_equilibrium"
  ],
  "tide_load": [
    "/gt1l/geophys_corr/tide_load",
    "/gt1r/geophys_corr/tide_load",
    "/gt2l/geophys_corr/tide_load",
    "/gt2r/geophys_corr/tide_load",
    "/gt3l/geophys_corr/tide_load",
    "/gt3r/geophys_corr/tide_load"
  ],
  "tide_oc_pole": [
    "/gt1l/geophys_corr/tide_oc_pole",
    "/gt1r/geophys_corr/tide_oc_pole",
    "/gt2l/geophys_corr/tide_oc_pole",
    "/gt2r/geophys_corr/tide_oc_pole",
    "/gt3l/geophys_corr/tide_oc_pole",
    "/gt3r/geophys_corr/tide_oc_pole"
  ],
  "tide_ocean": [
    "/gt1l/geophys_corr/tide_ocean",
    "/gt1r/geophys_corr/tide_ocean",
    "/gt2l/geophys_corr/tide_ocean",
    "/gt2r/geophys_corr/tide_ocean",
    "/gt3l/geophys_corr/tide_ocean",
    "/gt3r/geophys_corr/tide_ocean"
  ],
  "tide_pole": [
    "/gt1l/geophys_corr/tide_pole",
    "/gt1r/geophys_corr/tide_pole",
    "/gt2l/geophys_corr/tide_pole",
    "/gt2r/geophys_corr/tide_pole",
    "/gt3l/geophys_corr/tide_pole",
    "/gt3r/geophys_corr/tide_pole"
  ],
  "tlm_height_band1": [
    "/gt1l/bckgrd_atlas/tlm_height_band1",
    "/gt1r/bckgrd_atlas/tlm_height_band1",
    "/gt2l/bckgrd_atlas/tlm_height_band1",
    "/gt2r/bckgrd_atlas/tlm_height_band1",
    "/gt3l/bckgrd_atlas/tlm_height_band1",
    "/gt3r/bckgrd_atlas/tlm_height_band1"
  ],
  "tlm_height_band2": [
    "/gt1l/bckgrd_atlas/tlm_height_band2",
    "/gt1r/bckgrd_atlas/tlm_height_band2",
    "/gt2l/bckgrd_atlas/tlm_height_band2",
    "/gt2r/bckgrd_atlas/tlm_height_band2",
    "/gt3l/bckgrd_atlas/tlm_height_band2",
    "/gt3r/bckgrd_atlas/tlm_height_band2"
  ],
  "tlm_top_band1": [
    "/gt1l/bckgrd_atlas/tlm_top_band1",
    "/gt1r/bckgrd_atlas/tlm_top_band1",
    "/gt2l/bckgrd_atlas/tlm_top_band1",
    "/gt2r/bckgrd_atlas/tlm_top_band1",
    "/gt3l/bckgrd_atlas/tlm_top_band1",
    "/gt3r/bckgrd_atlas/tlm_top_band1"
  ],
  "tlm_top_band2": [
    "/gt1l/bckgrd_atlas/tlm_top_band2",
    "/gt1r/bckgrd_atlas/tlm_top_band2",
    "/gt2l/bckgrd_atlas/tlm_top_band2",
    "/gt2r/bckgrd_atlas/tlm_top_band2",
    "/gt3l/bckgrd_atlas/tlm_top_band2",
    "/gt3r/bckgrd_atlas/tlm_top_band2"
  ],
  "transmit": [
    "/ancillary_data/atlas_engineering/transmit"
  ],
  "tx_pulse_distribution": [
    "/ancillary_data/atlas_engineering/transmit/tx_pulse_distribution"
  ],
  "tx_pulse_energy": [
    "/ancillary_data/atlas_engineering/transmit/tx_pulse_energy",
    "/gt1l/geolocation/tx_pulse_energy",
    "/gt1r/geolocation/tx_pulse_energy",
    "/gt2l/geolocation/tx_pulse_energy",
    "/gt2r/geolocation/tx_pulse_energy",
    "/gt3l/geolocation/tx_pulse_energy",
    "/gt3r/geolocation/tx_pulse_energy"
  ],
  "tx_pulse_skew_est": [
    "/ancillary_data/atlas_engineering/transmit/tx_pulse_skew_est",
    "/gt1l/geolocation/tx_pulse_skew_est",
    "/gt1r/geolocation/tx_pulse_skew_est",
    "/gt2l/geolocation/tx_pulse_skew_est",
    "/gt2r/geolocation/tx_pulse_skew_est",
    "/gt3l/geolocation/tx_pulse_skew_est",
    "/gt3r/geolocation/tx_pulse_skew_est"
  ],
  "tx_pulse_thresh_lower": [
    "/ancillary_data/atlas_engineering/transmit/tx_pulse_thresh_lower"
  ],
  "tx_pulse_thresh_upper": [
    "/ancillary_data/atlas_engineering/transmit/tx_pulse_thresh_upper"
  ],
  "tx_pulse_width_lower": [
    "/ancillary_data/atlas_engineering/transmit/tx_pulse_width_lower",
    "/gt1l/geolocation/tx_pulse_width_lower",
    "/gt1r/geolocation/tx_pulse_width_lower",
    "/gt2l/geolocation/tx_pulse_width_lower",
    "/gt2r/geolocation/tx_pulse_width_lower",
    "/gt3l/geolocation/tx_pulse_width_lower",
    "/gt3r/geolocation/tx_pulse_width_lower"
  ],
  "tx_pulse_width_upper": [
    "/ancillary_data/atlas_engineering/transmit/tx_pulse_width_upper",
    "/gt1l/geolocation/tx_pulse_width_upper",
    "/gt1r/geolocation/tx_pulse_width_upper",
    "/gt2l/geolocation/tx_pulse_width_upper",
    "/gt2r/geolocation/tx_pulse_width_upper",
    "/gt3l/geolocation/tx_pulse_width_upper",
    "/gt3r/geolocation/tx_pulse_width_upper"
  ],
  "velocity_sc": [
    "/gt1l/geolocation/velocity_sc",
    "/gt1r/geolocation/velocity_sc",
    "/gt2l/geolocation/velocity_sc",
    "/gt2r/geolocation/velocity_sc",
    "/gt3l/geolocation/velocity_sc",
    "/gt3r/geolocation/velocity_sc"
  ],
  "version": [
    "/ancillary_data/version"
  ],
  "yaw": [
    "/gt1l/geolocation/yaw",
    "/gt1r/geolocation/yaw",
    "/gt2l/geolocation/yaw",
    "/gt2r/geolocation/yaw",
    "/gt3l/geolocation/yaw",
    "/gt3r/geolocation/yaw"
  ],
  "z_pc_delta": [
    "/gt1l/signal_find_output/inlandwater/z_pc_delta",
    "/gt1l/signal_find_output/land/z_pc_delta",
    "/gt1l/signal_find_output/ocean/z_pc_delta",
    "/gt1l/signal_find_output/sea_ice/z_pc_delta",
    "/gt1r/signal_find_output/inlandwater/z_pc_delta",
    "/gt1r/signal_find_output/land/z_pc_delta",
    "/gt1r/signal_find_output/ocean/z_pc_delta",
    "/gt1r/signal_find_output/sea_ice/z_pc_delta",
    "/gt2l/signal_find_output/inlandwater/z_pc_delta",
    "/gt2l/signal_find_output/land/z_pc_delta",
    "/gt2l/signal_find_output/ocean/z_pc_delta",
    "/gt2l/signal_find_output/sea_ice/z_pc_delta",
    "/gt2r/signal_find_output/inlandwater/z_pc_delta",
    "/gt2r/signal_find_output/land/z_pc_delta",
    "/gt2r/signal_find_output/ocean/z_pc_delta",
    "/gt2r/signal_find_output/sea_ice/z_pc_delta",
    "/gt3l/signal_find_output/inlandwater/z_pc_delta",
    "/gt3l/signal_find_output/land/z_pc_delta",
    "/gt3l/signal_find_output/ocean/z_pc_delta",
    "/gt3l/signal_find_output/sea_ice/z_pc_delta",
    "/gt3r/signal_find_output/inlandwater/z_pc_delta",
    "/gt3r/signal_find_output/land/z_pc_delta",
    "/gt3r/signal_find_output/ocean/z_pc_delta",
    "/gt3r/signal_find_output/sea_ice/z_pc_delta"
  ]
}

###############################################################################
################################### ATL06 #####################################
###############################################################################


ATL06_var_dict = \
{
  "": [
    "/gt1l",
    "/gt1r",
    "/gt2l",
    "/gt2r",
    "/gt3l",
    "/gt3r"
  ],
  "ancillary_data": [
    "/ancillary_data"
  ],
  "atl06_quality_summary": [
    "/gt1l/land_ice_segments/atl06_quality_summary",
    "/gt1r/land_ice_segments/atl06_quality_summary",
    "/gt2l/land_ice_segments/atl06_quality_summary",
    "/gt2r/land_ice_segments/atl06_quality_summary",
    "/gt3l/land_ice_segments/atl06_quality_summary",
    "/gt3r/land_ice_segments/atl06_quality_summary"
  ],
  "atlas_sdp_gps_epoch": [
    "/ancillary_data/atlas_sdp_gps_epoch"
  ],
  "bckgrd": [
    "/gt1l/land_ice_segments/geophysical/bckgrd",
    "/gt1r/land_ice_segments/geophysical/bckgrd",
    "/gt2l/land_ice_segments/geophysical/bckgrd",
    "/gt2r/land_ice_segments/geophysical/bckgrd",
    "/gt3l/land_ice_segments/geophysical/bckgrd",
    "/gt3r/land_ice_segments/geophysical/bckgrd"
  ],
  "bckgrd_per_m": [
    "/gt1l/residual_histogram/bckgrd_per_m",
    "/gt1r/residual_histogram/bckgrd_per_m",
    "/gt2l/residual_histogram/bckgrd_per_m",
    "/gt2r/residual_histogram/bckgrd_per_m",
    "/gt3l/residual_histogram/bckgrd_per_m",
    "/gt3r/residual_histogram/bckgrd_per_m"
  ],
  "bias_correction": [
    "/gt1l/land_ice_segments/bias_correction",
    "/gt1r/land_ice_segments/bias_correction",
    "/gt2l/land_ice_segments/bias_correction",
    "/gt2r/land_ice_segments/bias_correction",
    "/gt3l/land_ice_segments/bias_correction",
    "/gt3r/land_ice_segments/bias_correction"
  ],
  "bin_top_h": [
    "/gt1l/residual_histogram/bin_top_h",
    "/gt1r/residual_histogram/bin_top_h",
    "/gt2l/residual_histogram/bin_top_h",
    "/gt2r/residual_histogram/bin_top_h",
    "/gt3l/residual_histogram/bin_top_h",
    "/gt3r/residual_histogram/bin_top_h"
  ],
  "bsnow_conf": [
    "/gt1l/land_ice_segments/geophysical/bsnow_conf",
    "/gt1r/land_ice_segments/geophysical/bsnow_conf",
    "/gt2l/land_ice_segments/geophysical/bsnow_conf",
    "/gt2r/land_ice_segments/geophysical/bsnow_conf",
    "/gt3l/land_ice_segments/geophysical/bsnow_conf",
    "/gt3r/land_ice_segments/geophysical/bsnow_conf"
  ],
  "bsnow_h": [
    "/gt1l/land_ice_segments/geophysical/bsnow_h",
    "/gt1r/land_ice_segments/geophysical/bsnow_h",
    "/gt2l/land_ice_segments/geophysical/bsnow_h",
    "/gt2r/land_ice_segments/geophysical/bsnow_h",
    "/gt3l/land_ice_segments/geophysical/bsnow_h",
    "/gt3r/land_ice_segments/geophysical/bsnow_h"
  ],
  "bsnow_od": [
    "/gt1l/land_ice_segments/geophysical/bsnow_od",
    "/gt1r/land_ice_segments/geophysical/bsnow_od",
    "/gt2l/land_ice_segments/geophysical/bsnow_od",
    "/gt2r/land_ice_segments/geophysical/bsnow_od",
    "/gt3l/land_ice_segments/geophysical/bsnow_od",
    "/gt3r/land_ice_segments/geophysical/bsnow_od"
  ],
  "cloud_flg_asr": [
    "/gt1l/land_ice_segments/geophysical/cloud_flg_asr",
    "/gt1r/land_ice_segments/geophysical/cloud_flg_asr",
    "/gt2l/land_ice_segments/geophysical/cloud_flg_asr",
    "/gt2r/land_ice_segments/geophysical/cloud_flg_asr",
    "/gt3l/land_ice_segments/geophysical/cloud_flg_asr",
    "/gt3r/land_ice_segments/geophysical/cloud_flg_asr"
  ],
  "cloud_flg_atm": [
    "/gt1l/land_ice_segments/geophysical/cloud_flg_atm",
    "/gt1r/land_ice_segments/geophysical/cloud_flg_atm",
    "/gt2l/land_ice_segments/geophysical/cloud_flg_atm",
    "/gt2r/land_ice_segments/geophysical/cloud_flg_atm",
    "/gt3l/land_ice_segments/geophysical/cloud_flg_atm",
    "/gt3r/land_ice_segments/geophysical/cloud_flg_atm"
  ],
  "control": [
    "/ancillary_data/control"
  ],
  "count": [
    "/gt1l/residual_histogram/count",
    "/gt1r/residual_histogram/count",
    "/gt2l/residual_histogram/count",
    "/gt2r/residual_histogram/count",
    "/gt3l/residual_histogram/count",
    "/gt3r/residual_histogram/count"
  ],
  "crossing_time": [
    "/orbit_info/crossing_time"
  ],
  "cycle_number": [
    "/orbit_info/cycle_number"
  ],
  "dac": [
    "/gt1l/land_ice_segments/geophysical/dac",
    "/gt1r/land_ice_segments/geophysical/dac",
    "/gt2l/land_ice_segments/geophysical/dac",
    "/gt2r/land_ice_segments/geophysical/dac",
    "/gt3l/land_ice_segments/geophysical/dac",
    "/gt3r/land_ice_segments/geophysical/dac"
  ],
  "data_end_utc": [
    "/ancillary_data/data_end_utc"
  ],
  "data_start_utc": [
    "/ancillary_data/data_start_utc"
  ],
  "delta_time": [
    "/gt1l/land_ice_segments/delta_time",
    "/gt1l/residual_histogram/delta_time",
    "/gt1l/segment_quality/delta_time",
    "/gt1r/land_ice_segments/delta_time",
    "/gt1r/residual_histogram/delta_time",
    "/gt1r/segment_quality/delta_time",
    "/gt2l/land_ice_segments/delta_time",
    "/gt2l/residual_histogram/delta_time",
    "/gt2l/segment_quality/delta_time",
    "/gt2r/land_ice_segments/delta_time",
    "/gt2r/residual_histogram/delta_time",
    "/gt2r/segment_quality/delta_time",
    "/gt3l/land_ice_segments/delta_time",
    "/gt3l/residual_histogram/delta_time",
    "/gt3l/segment_quality/delta_time",
    "/gt3r/land_ice_segments/delta_time",
    "/gt3r/residual_histogram/delta_time",
    "/gt3r/segment_quality/delta_time",
    "/quality_assessment/gt1l/delta_time",
    "/quality_assessment/gt1r/delta_time",
    "/quality_assessment/gt2l/delta_time",
    "/quality_assessment/gt2r/delta_time",
    "/quality_assessment/gt3l/delta_time",
    "/quality_assessment/gt3r/delta_time"
  ],
  "dem": [
    "/gt1l/land_ice_segments/dem",
    "/gt1r/land_ice_segments/dem",
    "/gt2l/land_ice_segments/dem",
    "/gt2r/land_ice_segments/dem",
    "/gt3l/land_ice_segments/dem",
    "/gt3r/land_ice_segments/dem"
  ],
  "dem_flag": [
    "/gt1l/land_ice_segments/dem/dem_flag",
    "/gt1r/land_ice_segments/dem/dem_flag",
    "/gt2l/land_ice_segments/dem/dem_flag",
    "/gt2r/land_ice_segments/dem/dem_flag",
    "/gt3l/land_ice_segments/dem/dem_flag",
    "/gt3r/land_ice_segments/dem/dem_flag"
  ],
  "dem_h": [
    "/gt1l/land_ice_segments/dem/dem_h",
    "/gt1r/land_ice_segments/dem/dem_h",
    "/gt2l/land_ice_segments/dem/dem_h",
    "/gt2r/land_ice_segments/dem/dem_h",
    "/gt3l/land_ice_segments/dem/dem_h",
    "/gt3r/land_ice_segments/dem/dem_h"
  ],
  "dh_fit_dx": [
    "/gt1l/land_ice_segments/fit_statistics/dh_fit_dx",
    "/gt1r/land_ice_segments/fit_statistics/dh_fit_dx",
    "/gt2l/land_ice_segments/fit_statistics/dh_fit_dx",
    "/gt2r/land_ice_segments/fit_statistics/dh_fit_dx",
    "/gt3l/land_ice_segments/fit_statistics/dh_fit_dx",
    "/gt3r/land_ice_segments/fit_statistics/dh_fit_dx"
  ],
  "dh_fit_dx_sigma": [
    "/gt1l/land_ice_segments/fit_statistics/dh_fit_dx_sigma",
    "/gt1r/land_ice_segments/fit_statistics/dh_fit_dx_sigma",
    "/gt2l/land_ice_segments/fit_statistics/dh_fit_dx_sigma",
    "/gt2r/land_ice_segments/fit_statistics/dh_fit_dx_sigma",
    "/gt3l/land_ice_segments/fit_statistics/dh_fit_dx_sigma",
    "/gt3r/land_ice_segments/fit_statistics/dh_fit_dx_sigma"
  ],
  "dh_fit_dy": [
    "/gt1l/land_ice_segments/fit_statistics/dh_fit_dy",
    "/gt1r/land_ice_segments/fit_statistics/dh_fit_dy",
    "/gt2l/land_ice_segments/fit_statistics/dh_fit_dy",
    "/gt2r/land_ice_segments/fit_statistics/dh_fit_dy",
    "/gt3l/land_ice_segments/fit_statistics/dh_fit_dy",
    "/gt3r/land_ice_segments/fit_statistics/dh_fit_dy"
  ],
  "ds_segment_id": [
    "/gt1l/residual_histogram/ds_segment_id",
    "/gt1r/residual_histogram/ds_segment_id",
    "/gt2l/residual_histogram/ds_segment_id",
    "/gt2r/residual_histogram/ds_segment_id",
    "/gt3l/residual_histogram/ds_segment_id",
    "/gt3r/residual_histogram/ds_segment_id"
  ],
  "dt_hist": [
    "/ancillary_data/land_ice/dt_hist"
  ],
  "e_bckgrd": [
    "/gt1l/land_ice_segments/geophysical/e_bckgrd",
    "/gt1r/land_ice_segments/geophysical/e_bckgrd",
    "/gt2l/land_ice_segments/geophysical/e_bckgrd",
    "/gt2r/land_ice_segments/geophysical/e_bckgrd",
    "/gt3l/land_ice_segments/geophysical/e_bckgrd",
    "/gt3r/land_ice_segments/geophysical/e_bckgrd"
  ],
  "end_cycle": [
    "/ancillary_data/end_cycle"
  ],
  "end_delta_time": [
    "/ancillary_data/end_delta_time"
  ],
  "end_geoseg": [
    "/ancillary_data/end_geoseg"
  ],
  "end_gpssow": [
    "/ancillary_data/end_gpssow"
  ],
  "end_gpsweek": [
    "/ancillary_data/end_gpsweek"
  ],
  "end_orbit": [
    "/ancillary_data/end_orbit"
  ],
  "end_region": [
    "/ancillary_data/end_region"
  ],
  "end_rgt": [
    "/ancillary_data/end_rgt"
  ],
  "fit_maxiter": [
    "/ancillary_data/land_ice/fit_maxiter"
  ],
  "fit_statistics": [
    "/gt1l/land_ice_segments/fit_statistics",
    "/gt1r/land_ice_segments/fit_statistics",
    "/gt2l/land_ice_segments/fit_statistics",
    "/gt2r/land_ice_segments/fit_statistics",
    "/gt3l/land_ice_segments/fit_statistics",
    "/gt3r/land_ice_segments/fit_statistics"
  ],
  "fpb_maxiter": [
    "/ancillary_data/land_ice/fpb_maxiter"
  ],
  "fpb_mean_corr": [
    "/gt1l/land_ice_segments/bias_correction/fpb_mean_corr",
    "/gt1r/land_ice_segments/bias_correction/fpb_mean_corr",
    "/gt2l/land_ice_segments/bias_correction/fpb_mean_corr",
    "/gt2r/land_ice_segments/bias_correction/fpb_mean_corr",
    "/gt3l/land_ice_segments/bias_correction/fpb_mean_corr",
    "/gt3r/land_ice_segments/bias_correction/fpb_mean_corr"
  ],
  "fpb_mean_corr_sigma": [
    "/gt1l/land_ice_segments/bias_correction/fpb_mean_corr_sigma",
    "/gt1r/land_ice_segments/bias_correction/fpb_mean_corr_sigma",
    "/gt2l/land_ice_segments/bias_correction/fpb_mean_corr_sigma",
    "/gt2r/land_ice_segments/bias_correction/fpb_mean_corr_sigma",
    "/gt3l/land_ice_segments/bias_correction/fpb_mean_corr_sigma",
    "/gt3r/land_ice_segments/bias_correction/fpb_mean_corr_sigma"
  ],
  "fpb_med_corr": [
    "/gt1l/land_ice_segments/bias_correction/fpb_med_corr",
    "/gt1r/land_ice_segments/bias_correction/fpb_med_corr",
    "/gt2l/land_ice_segments/bias_correction/fpb_med_corr",
    "/gt2r/land_ice_segments/bias_correction/fpb_med_corr",
    "/gt3l/land_ice_segments/bias_correction/fpb_med_corr",
    "/gt3r/land_ice_segments/bias_correction/fpb_med_corr"
  ],
  "fpb_med_corr_sigma": [
    "/gt1l/land_ice_segments/bias_correction/fpb_med_corr_sigma",
    "/gt1r/land_ice_segments/bias_correction/fpb_med_corr_sigma",
    "/gt2l/land_ice_segments/bias_correction/fpb_med_corr_sigma",
    "/gt2r/land_ice_segments/bias_correction/fpb_med_corr_sigma",
    "/gt3l/land_ice_segments/bias_correction/fpb_med_corr_sigma",
    "/gt3r/land_ice_segments/bias_correction/fpb_med_corr_sigma"
  ],
  "fpb_n_corr": [
    "/gt1l/land_ice_segments/bias_correction/fpb_n_corr",
    "/gt1r/land_ice_segments/bias_correction/fpb_n_corr",
    "/gt2l/land_ice_segments/bias_correction/fpb_n_corr",
    "/gt2r/land_ice_segments/bias_correction/fpb_n_corr",
    "/gt3l/land_ice_segments/bias_correction/fpb_n_corr",
    "/gt3r/land_ice_segments/bias_correction/fpb_n_corr"
  ],
  "geoid_free2mean": [
    "/gt1l/land_ice_segments/dem/geoid_free2mean",
    "/gt1r/land_ice_segments/dem/geoid_free2mean",
    "/gt2l/land_ice_segments/dem/geoid_free2mean",
    "/gt2r/land_ice_segments/dem/geoid_free2mean",
    "/gt3l/land_ice_segments/dem/geoid_free2mean",
    "/gt3r/land_ice_segments/dem/geoid_free2mean"
  ],
  "geoid_h": [
    "/gt1l/land_ice_segments/dem/geoid_h",
    "/gt1r/land_ice_segments/dem/geoid_h",
    "/gt2l/land_ice_segments/dem/geoid_h",
    "/gt2r/land_ice_segments/dem/geoid_h",
    "/gt3l/land_ice_segments/dem/geoid_h",
    "/gt3r/land_ice_segments/dem/geoid_h"
  ],
  "geophysical": [
    "/gt1l/land_ice_segments/geophysical",
    "/gt1r/land_ice_segments/geophysical",
    "/gt2l/land_ice_segments/geophysical",
    "/gt2r/land_ice_segments/geophysical",
    "/gt3l/land_ice_segments/geophysical",
    "/gt3r/land_ice_segments/geophysical"
  ],
  "granule_end_utc": [
    "/ancillary_data/granule_end_utc"
  ],
  "granule_start_utc": [
    "/ancillary_data/granule_start_utc"
  ],
  "ground_track": [
    "/gt1l/land_ice_segments/ground_track",
    "/gt1r/land_ice_segments/ground_track",
    "/gt2l/land_ice_segments/ground_track",
    "/gt2r/land_ice_segments/ground_track",
    "/gt3l/land_ice_segments/ground_track",
    "/gt3r/land_ice_segments/ground_track"
  ],
  "h_expected_rms": [
    "/gt1l/land_ice_segments/fit_statistics/h_expected_rms",
    "/gt1r/land_ice_segments/fit_statistics/h_expected_rms",
    "/gt2l/land_ice_segments/fit_statistics/h_expected_rms",
    "/gt2r/land_ice_segments/fit_statistics/h_expected_rms",
    "/gt3l/land_ice_segments/fit_statistics/h_expected_rms",
    "/gt3r/land_ice_segments/fit_statistics/h_expected_rms"
  ],
  "h_li": [
    "/gt1l/land_ice_segments/h_li",
    "/gt1r/land_ice_segments/h_li",
    "/gt2l/land_ice_segments/h_li",
    "/gt2r/land_ice_segments/h_li",
    "/gt3l/land_ice_segments/h_li",
    "/gt3r/land_ice_segments/h_li"
  ],
  "h_li_sigma": [
    "/gt1l/land_ice_segments/h_li_sigma",
    "/gt1r/land_ice_segments/h_li_sigma",
    "/gt2l/land_ice_segments/h_li_sigma",
    "/gt2r/land_ice_segments/h_li_sigma",
    "/gt3l/land_ice_segments/h_li_sigma",
    "/gt3r/land_ice_segments/h_li_sigma"
  ],
  "h_mean": [
    "/gt1l/land_ice_segments/fit_statistics/h_mean",
    "/gt1r/land_ice_segments/fit_statistics/h_mean",
    "/gt2l/land_ice_segments/fit_statistics/h_mean",
    "/gt2r/land_ice_segments/fit_statistics/h_mean",
    "/gt3l/land_ice_segments/fit_statistics/h_mean",
    "/gt3r/land_ice_segments/fit_statistics/h_mean"
  ],
  "h_rms_misfit": [
    "/gt1l/land_ice_segments/fit_statistics/h_rms_misfit",
    "/gt1r/land_ice_segments/fit_statistics/h_rms_misfit",
    "/gt2l/land_ice_segments/fit_statistics/h_rms_misfit",
    "/gt2r/land_ice_segments/fit_statistics/h_rms_misfit",
    "/gt3l/land_ice_segments/fit_statistics/h_rms_misfit",
    "/gt3r/land_ice_segments/fit_statistics/h_rms_misfit"
  ],
  "h_robust_sprd": [
    "/gt1l/land_ice_segments/fit_statistics/h_robust_sprd",
    "/gt1r/land_ice_segments/fit_statistics/h_robust_sprd",
    "/gt2l/land_ice_segments/fit_statistics/h_robust_sprd",
    "/gt2r/land_ice_segments/fit_statistics/h_robust_sprd",
    "/gt3l/land_ice_segments/fit_statistics/h_robust_sprd",
    "/gt3r/land_ice_segments/fit_statistics/h_robust_sprd"
  ],
  "lan": [
    "/orbit_info/lan"
  ],
  "land_ice": [
    "/ancillary_data/land_ice"
  ],
  "land_ice_segments": [
    "/gt1l/land_ice_segments",
    "/gt1r/land_ice_segments",
    "/gt2l/land_ice_segments",
    "/gt2r/land_ice_segments",
    "/gt3l/land_ice_segments",
    "/gt3r/land_ice_segments"
  ],
  "lat_mean": [
    "/gt1l/residual_histogram/lat_mean",
    "/gt1r/residual_histogram/lat_mean",
    "/gt2l/residual_histogram/lat_mean",
    "/gt2r/residual_histogram/lat_mean",
    "/gt3l/residual_histogram/lat_mean",
    "/gt3r/residual_histogram/lat_mean",
    "/quality_assessment/gt1l/lat_mean",
    "/quality_assessment/gt1r/lat_mean",
    "/quality_assessment/gt2l/lat_mean",
    "/quality_assessment/gt2r/lat_mean",
    "/quality_assessment/gt3l/lat_mean",
    "/quality_assessment/gt3r/lat_mean"
  ],
  "latitude": [
    "/gt1l/land_ice_segments/latitude",
    "/gt1r/land_ice_segments/latitude",
    "/gt2l/land_ice_segments/latitude",
    "/gt2r/land_ice_segments/latitude",
    "/gt3l/land_ice_segments/latitude",
    "/gt3r/land_ice_segments/latitude"
  ],
  "layer_flag": [
    "/gt1l/land_ice_segments/geophysical/layer_flag",
    "/gt1r/land_ice_segments/geophysical/layer_flag",
    "/gt2l/land_ice_segments/geophysical/layer_flag",
    "/gt2r/land_ice_segments/geophysical/layer_flag",
    "/gt3l/land_ice_segments/geophysical/layer_flag",
    "/gt3r/land_ice_segments/geophysical/layer_flag"
  ],
  "lon_mean": [
    "/gt1l/residual_histogram/lon_mean",
    "/gt1r/residual_histogram/lon_mean",
    "/gt2l/residual_histogram/lon_mean",
    "/gt2r/residual_histogram/lon_mean",
    "/gt3l/residual_histogram/lon_mean",
    "/gt3r/residual_histogram/lon_mean",
    "/quality_assessment/gt1l/lon_mean",
    "/quality_assessment/gt1r/lon_mean",
    "/quality_assessment/gt2l/lon_mean",
    "/quality_assessment/gt2r/lon_mean",
    "/quality_assessment/gt3l/lon_mean",
    "/quality_assessment/gt3r/lon_mean"
  ],
  "longitude": [
    "/gt1l/land_ice_segments/longitude",
    "/gt1r/land_ice_segments/longitude",
    "/gt2l/land_ice_segments/longitude",
    "/gt2r/land_ice_segments/longitude",
    "/gt3l/land_ice_segments/longitude",
    "/gt3r/land_ice_segments/longitude"
  ],
  "max_res_ids": [
    "/ancillary_data/land_ice/max_res_ids"
  ],
  "med_r_fit": [
    "/gt1l/land_ice_segments/bias_correction/med_r_fit",
    "/gt1r/land_ice_segments/bias_correction/med_r_fit",
    "/gt2l/land_ice_segments/bias_correction/med_r_fit",
    "/gt2r/land_ice_segments/bias_correction/med_r_fit",
    "/gt3l/land_ice_segments/bias_correction/med_r_fit",
    "/gt3r/land_ice_segments/bias_correction/med_r_fit"
  ],
  "min_dist": [
    "/ancillary_data/land_ice/min_dist"
  ],
  "min_gain_th": [
    "/ancillary_data/land_ice/min_gain_th"
  ],
  "min_n_pe": [
    "/ancillary_data/land_ice/min_n_pe"
  ],
  "min_n_sel": [
    "/ancillary_data/land_ice/min_n_sel"
  ],
  "min_signal_conf": [
    "/ancillary_data/land_ice/min_signal_conf"
  ],
  "msw_flag": [
    "/gt1l/land_ice_segments/geophysical/msw_flag",
    "/gt1r/land_ice_segments/geophysical/msw_flag",
    "/gt2l/land_ice_segments/geophysical/msw_flag",
    "/gt2r/land_ice_segments/geophysical/msw_flag",
    "/gt3l/land_ice_segments/geophysical/msw_flag",
    "/gt3r/land_ice_segments/geophysical/msw_flag"
  ],
  "n_fit_photons": [
    "/gt1l/land_ice_segments/fit_statistics/n_fit_photons",
    "/gt1r/land_ice_segments/fit_statistics/n_fit_photons",
    "/gt2l/land_ice_segments/fit_statistics/n_fit_photons",
    "/gt2r/land_ice_segments/fit_statistics/n_fit_photons",
    "/gt3l/land_ice_segments/fit_statistics/n_fit_photons",
    "/gt3r/land_ice_segments/fit_statistics/n_fit_photons"
  ],
  "n_hist": [
    "/ancillary_data/land_ice/n_hist"
  ],
  "n_seg_pulses": [
    "/gt1l/land_ice_segments/fit_statistics/n_seg_pulses",
    "/gt1r/land_ice_segments/fit_statistics/n_seg_pulses",
    "/gt2l/land_ice_segments/fit_statistics/n_seg_pulses",
    "/gt2r/land_ice_segments/fit_statistics/n_seg_pulses",
    "/gt3l/land_ice_segments/fit_statistics/n_seg_pulses",
    "/gt3r/land_ice_segments/fit_statistics/n_seg_pulses"
  ],
  "n_sigmas": [
    "/ancillary_data/land_ice/n_sigmas"
  ],
  "neutat_delay_total": [
    "/gt1l/land_ice_segments/geophysical/neutat_delay_total",
    "/gt1r/land_ice_segments/geophysical/neutat_delay_total",
    "/gt2l/land_ice_segments/geophysical/neutat_delay_total",
    "/gt2r/land_ice_segments/geophysical/neutat_delay_total",
    "/gt3l/land_ice_segments/geophysical/neutat_delay_total",
    "/gt3r/land_ice_segments/geophysical/neutat_delay_total"
  ],
  "nhist_bins": [
    "/ancillary_data/land_ice/nhist_bins"
  ],
  "orbit_info": [
    "/orbit_info"
  ],
  "orbit_number": [
    "/orbit_info/orbit_number"
  ],
  "proc_interval": [
    "/ancillary_data/land_ice/proc_interval"
  ],
  "pulse_count": [
    "/gt1l/residual_histogram/pulse_count",
    "/gt1r/residual_histogram/pulse_count",
    "/gt2l/residual_histogram/pulse_count",
    "/gt2r/residual_histogram/pulse_count",
    "/gt3l/residual_histogram/pulse_count",
    "/gt3r/residual_histogram/pulse_count"
  ],
  "qa_at_interval": [
    "/ancillary_data/qa_at_interval"
  ],
  "qa_granule_fail_reason": [
    "/quality_assessment/qa_granule_fail_reason"
  ],
  "qa_granule_pass_fail": [
    "/quality_assessment/qa_granule_pass_fail"
  ],
  "qs_lim_bsc": [
    "/ancillary_data/land_ice/qs_lim_bsc"
  ],
  "qs_lim_hrs": [
    "/ancillary_data/land_ice/qs_lim_hrs"
  ],
  "qs_lim_hsigma": [
    "/ancillary_data/land_ice/qs_lim_hsigma"
  ],
  "qs_lim_msw": [
    "/ancillary_data/land_ice/qs_lim_msw"
  ],
  "qs_lim_snr": [
    "/ancillary_data/land_ice/qs_lim_snr"
  ],
  "qs_lim_sss": [
    "/ancillary_data/land_ice/qs_lim_sss"
  ],
  "quality_assessment": [
    "/quality_assessment",
    "/quality_assessment/gt1l",
    "/quality_assessment/gt1r",
    "/quality_assessment/gt2l",
    "/quality_assessment/gt2r",
    "/quality_assessment/gt3l",
    "/quality_assessment/gt3r"
  ],
  "r_eff": [
    "/gt1l/land_ice_segments/geophysical/r_eff",
    "/gt1r/land_ice_segments/geophysical/r_eff",
    "/gt2l/land_ice_segments/geophysical/r_eff",
    "/gt2r/land_ice_segments/geophysical/r_eff",
    "/gt3l/land_ice_segments/geophysical/r_eff",
    "/gt3r/land_ice_segments/geophysical/r_eff"
  ],
  "rbin_width": [
    "/ancillary_data/land_ice/rbin_width"
  ],
  "record_number": [
    "/gt1l/segment_quality/record_number",
    "/gt1r/segment_quality/record_number",
    "/gt2l/segment_quality/record_number",
    "/gt2r/segment_quality/record_number",
    "/gt3l/segment_quality/record_number",
    "/gt3r/segment_quality/record_number"
  ],
  "ref_azimuth": [
    "/gt1l/land_ice_segments/ground_track/ref_azimuth",
    "/gt1r/land_ice_segments/ground_track/ref_azimuth",
    "/gt2l/land_ice_segments/ground_track/ref_azimuth",
    "/gt2r/land_ice_segments/ground_track/ref_azimuth",
    "/gt3l/land_ice_segments/ground_track/ref_azimuth",
    "/gt3r/land_ice_segments/ground_track/ref_azimuth"
  ],
  "ref_coelv": [
    "/gt1l/land_ice_segments/ground_track/ref_coelv",
    "/gt1r/land_ice_segments/ground_track/ref_coelv",
    "/gt2l/land_ice_segments/ground_track/ref_coelv",
    "/gt2r/land_ice_segments/ground_track/ref_coelv",
    "/gt3l/land_ice_segments/ground_track/ref_coelv",
    "/gt3r/land_ice_segments/ground_track/ref_coelv"
  ],
  "reference_pt_lat": [
    "/gt1l/segment_quality/reference_pt_lat",
    "/gt1r/segment_quality/reference_pt_lat",
    "/gt2l/segment_quality/reference_pt_lat",
    "/gt2r/segment_quality/reference_pt_lat",
    "/gt3l/segment_quality/reference_pt_lat",
    "/gt3r/segment_quality/reference_pt_lat"
  ],
  "reference_pt_lon": [
    "/gt1l/segment_quality/reference_pt_lon",
    "/gt1r/segment_quality/reference_pt_lon",
    "/gt2l/segment_quality/reference_pt_lon",
    "/gt2r/segment_quality/reference_pt_lon",
    "/gt3l/segment_quality/reference_pt_lon",
    "/gt3r/segment_quality/reference_pt_lon"
  ],
  "release": [
    "/ancillary_data/release"
  ],
  "residual_histogram": [
    "/gt1l/residual_histogram",
    "/gt1r/residual_histogram",
    "/gt2l/residual_histogram",
    "/gt2r/residual_histogram",
    "/gt3l/residual_histogram",
    "/gt3r/residual_histogram"
  ],
  "rgt": [
    "/orbit_info/rgt"
  ],
  "sc_orient": [
    "/orbit_info/sc_orient"
  ],
  "sc_orient_time": [
    "/orbit_info/sc_orient_time"
  ],
  "seg_azimuth": [
    "/gt1l/land_ice_segments/ground_track/seg_azimuth",
    "/gt1r/land_ice_segments/ground_track/seg_azimuth",
    "/gt2l/land_ice_segments/ground_track/seg_azimuth",
    "/gt2r/land_ice_segments/ground_track/seg_azimuth",
    "/gt3l/land_ice_segments/ground_track/seg_azimuth",
    "/gt3r/land_ice_segments/ground_track/seg_azimuth"
  ],
  "segment_id": [
    "/gt1l/land_ice_segments/segment_id",
    "/gt1l/segment_quality/segment_id",
    "/gt1r/land_ice_segments/segment_id",
    "/gt1r/segment_quality/segment_id",
    "/gt2l/land_ice_segments/segment_id",
    "/gt2l/segment_quality/segment_id",
    "/gt2r/land_ice_segments/segment_id",
    "/gt2r/segment_quality/segment_id",
    "/gt3l/land_ice_segments/segment_id",
    "/gt3l/segment_quality/segment_id",
    "/gt3r/land_ice_segments/segment_id",
    "/gt3r/segment_quality/segment_id"
  ],
  "segment_id_list": [
    "/gt1l/residual_histogram/segment_id_list",
    "/gt1r/residual_histogram/segment_id_list",
    "/gt2l/residual_histogram/segment_id_list",
    "/gt2r/residual_histogram/segment_id_list",
    "/gt3l/residual_histogram/segment_id_list",
    "/gt3r/residual_histogram/segment_id_list"
  ],
  "segment_quality": [
    "/gt1l/segment_quality",
    "/gt1r/segment_quality",
    "/gt2l/segment_quality",
    "/gt2r/segment_quality",
    "/gt3l/segment_quality",
    "/gt3r/segment_quality"
  ],
  "sigma_beam": [
    "/ancillary_data/land_ice/sigma_beam"
  ],
  "sigma_geo_at": [
    "/gt1l/land_ice_segments/ground_track/sigma_geo_at",
    "/gt1r/land_ice_segments/ground_track/sigma_geo_at",
    "/gt2l/land_ice_segments/ground_track/sigma_geo_at",
    "/gt2r/land_ice_segments/ground_track/sigma_geo_at",
    "/gt3l/land_ice_segments/ground_track/sigma_geo_at",
    "/gt3r/land_ice_segments/ground_track/sigma_geo_at"
  ],
  "sigma_geo_h": [
    "/gt1l/land_ice_segments/sigma_geo_h",
    "/gt1r/land_ice_segments/sigma_geo_h",
    "/gt2l/land_ice_segments/sigma_geo_h",
    "/gt2r/land_ice_segments/sigma_geo_h",
    "/gt3l/land_ice_segments/sigma_geo_h",
    "/gt3r/land_ice_segments/sigma_geo_h"
  ],
  "sigma_geo_r": [
    "/gt1l/land_ice_segments/ground_track/sigma_geo_r",
    "/gt1r/land_ice_segments/ground_track/sigma_geo_r",
    "/gt2l/land_ice_segments/ground_track/sigma_geo_r",
    "/gt2r/land_ice_segments/ground_track/sigma_geo_r",
    "/gt3l/land_ice_segments/ground_track/sigma_geo_r",
    "/gt3r/land_ice_segments/ground_track/sigma_geo_r"
  ],
  "sigma_geo_xt": [
    "/gt1l/land_ice_segments/ground_track/sigma_geo_xt",
    "/gt1r/land_ice_segments/ground_track/sigma_geo_xt",
    "/gt2l/land_ice_segments/ground_track/sigma_geo_xt",
    "/gt2r/land_ice_segments/ground_track/sigma_geo_xt",
    "/gt3l/land_ice_segments/ground_track/sigma_geo_xt",
    "/gt3r/land_ice_segments/ground_track/sigma_geo_xt"
  ],
  "sigma_h_mean": [
    "/gt1l/land_ice_segments/fit_statistics/sigma_h_mean",
    "/gt1r/land_ice_segments/fit_statistics/sigma_h_mean",
    "/gt2l/land_ice_segments/fit_statistics/sigma_h_mean",
    "/gt2r/land_ice_segments/fit_statistics/sigma_h_mean",
    "/gt3l/land_ice_segments/fit_statistics/sigma_h_mean",
    "/gt3r/land_ice_segments/fit_statistics/sigma_h_mean"
  ],
  "sigma_tx": [
    "/ancillary_data/land_ice/sigma_tx"
  ],
  "signal_selection_source": [
    "/gt1l/land_ice_segments/fit_statistics/signal_selection_source",
    "/gt1l/segment_quality/signal_selection_source",
    "/gt1r/land_ice_segments/fit_statistics/signal_selection_source",
    "/gt1r/segment_quality/signal_selection_source",
    "/gt2l/land_ice_segments/fit_statistics/signal_selection_source",
    "/gt2l/segment_quality/signal_selection_source",
    "/gt2r/land_ice_segments/fit_statistics/signal_selection_source",
    "/gt2r/segment_quality/signal_selection_source",
    "/gt3l/land_ice_segments/fit_statistics/signal_selection_source",
    "/gt3l/segment_quality/signal_selection_source",
    "/gt3r/land_ice_segments/fit_statistics/signal_selection_source",
    "/gt3r/segment_quality/signal_selection_source"
  ],
  "signal_selection_source_fraction_0": [
    "/quality_assessment/gt1l/signal_selection_source_fraction_0",
    "/quality_assessment/gt1r/signal_selection_source_fraction_0",
    "/quality_assessment/gt2l/signal_selection_source_fraction_0",
    "/quality_assessment/gt2r/signal_selection_source_fraction_0",
    "/quality_assessment/gt3l/signal_selection_source_fraction_0",
    "/quality_assessment/gt3r/signal_selection_source_fraction_0"
  ],
  "signal_selection_source_fraction_1": [
    "/quality_assessment/gt1l/signal_selection_source_fraction_1",
    "/quality_assessment/gt1r/signal_selection_source_fraction_1",
    "/quality_assessment/gt2l/signal_selection_source_fraction_1",
    "/quality_assessment/gt2r/signal_selection_source_fraction_1",
    "/quality_assessment/gt3l/signal_selection_source_fraction_1",
    "/quality_assessment/gt3r/signal_selection_source_fraction_1"
  ],
  "signal_selection_source_fraction_2": [
    "/quality_assessment/gt1l/signal_selection_source_fraction_2",
    "/quality_assessment/gt1r/signal_selection_source_fraction_2",
    "/quality_assessment/gt2l/signal_selection_source_fraction_2",
    "/quality_assessment/gt2r/signal_selection_source_fraction_2",
    "/quality_assessment/gt3l/signal_selection_source_fraction_2",
    "/quality_assessment/gt3r/signal_selection_source_fraction_2"
  ],
  "signal_selection_source_fraction_3": [
    "/quality_assessment/gt1l/signal_selection_source_fraction_3",
    "/quality_assessment/gt1r/signal_selection_source_fraction_3",
    "/quality_assessment/gt2l/signal_selection_source_fraction_3",
    "/quality_assessment/gt2r/signal_selection_source_fraction_3",
    "/quality_assessment/gt3l/signal_selection_source_fraction_3",
    "/quality_assessment/gt3r/signal_selection_source_fraction_3"
  ],
  "signal_selection_source_status": [
    "/gt1l/land_ice_segments/fit_statistics/signal_selection_source_status",
    "/gt1r/land_ice_segments/fit_statistics/signal_selection_source_status",
    "/gt2l/land_ice_segments/fit_statistics/signal_selection_source_status",
    "/gt2r/land_ice_segments/fit_statistics/signal_selection_source_status",
    "/gt3l/land_ice_segments/fit_statistics/signal_selection_source_status",
    "/gt3r/land_ice_segments/fit_statistics/signal_selection_source_status"
  ],
  "signal_selection_status": [
    "/gt1l/segment_quality/signal_selection_status",
    "/gt1r/segment_quality/signal_selection_status",
    "/gt2l/segment_quality/signal_selection_status",
    "/gt2r/segment_quality/signal_selection_status",
    "/gt3l/segment_quality/signal_selection_status",
    "/gt3r/segment_quality/signal_selection_status"
  ],
  "signal_selection_status_all": [
    "/gt1l/segment_quality/signal_selection_status/signal_selection_status_all",
    "/gt1r/segment_quality/signal_selection_status/signal_selection_status_all",
    "/gt2l/segment_quality/signal_selection_status/signal_selection_status_all",
    "/gt2r/segment_quality/signal_selection_status/signal_selection_status_all",
    "/gt3l/segment_quality/signal_selection_status/signal_selection_status_all",
    "/gt3r/segment_quality/signal_selection_status/signal_selection_status_all"
  ],
  "signal_selection_status_backup": [
    "/gt1l/segment_quality/signal_selection_status/signal_selection_status_backup",
    "/gt1r/segment_quality/signal_selection_status/signal_selection_status_backup",
    "/gt2l/segment_quality/signal_selection_status/signal_selection_status_backup",
    "/gt2r/segment_quality/signal_selection_status/signal_selection_status_backup",
    "/gt3l/segment_quality/signal_selection_status/signal_selection_status_backup",
    "/gt3r/segment_quality/signal_selection_status/signal_selection_status_backup"
  ],
  "signal_selection_status_confident": [
    "/gt1l/segment_quality/signal_selection_status/signal_selection_status_confident",
    "/gt1r/segment_quality/signal_selection_status/signal_selection_status_confident",
    "/gt2l/segment_quality/signal_selection_status/signal_selection_status_confident",
    "/gt2r/segment_quality/signal_selection_status/signal_selection_status_confident",
    "/gt3l/segment_quality/signal_selection_status/signal_selection_status_confident",
    "/gt3r/segment_quality/signal_selection_status/signal_selection_status_confident"
  ],
  "snr": [
    "/gt1l/land_ice_segments/fit_statistics/snr",
    "/gt1r/land_ice_segments/fit_statistics/snr",
    "/gt2l/land_ice_segments/fit_statistics/snr",
    "/gt2r/land_ice_segments/fit_statistics/snr",
    "/gt3l/land_ice_segments/fit_statistics/snr",
    "/gt3r/land_ice_segments/fit_statistics/snr"
  ],
  "snr_significance": [
    "/gt1l/land_ice_segments/fit_statistics/snr_significance",
    "/gt1r/land_ice_segments/fit_statistics/snr_significance",
    "/gt2l/land_ice_segments/fit_statistics/snr_significance",
    "/gt2r/land_ice_segments/fit_statistics/snr_significance",
    "/gt3l/land_ice_segments/fit_statistics/snr_significance",
    "/gt3r/land_ice_segments/fit_statistics/snr_significance"
  ],
  "solar_azimuth": [
    "/gt1l/land_ice_segments/geophysical/solar_azimuth",
    "/gt1r/land_ice_segments/geophysical/solar_azimuth",
    "/gt2l/land_ice_segments/geophysical/solar_azimuth",
    "/gt2r/land_ice_segments/geophysical/solar_azimuth",
    "/gt3l/land_ice_segments/geophysical/solar_azimuth",
    "/gt3r/land_ice_segments/geophysical/solar_azimuth"
  ],
  "solar_elevation": [
    "/gt1l/land_ice_segments/geophysical/solar_elevation",
    "/gt1r/land_ice_segments/geophysical/solar_elevation",
    "/gt2l/land_ice_segments/geophysical/solar_elevation",
    "/gt2r/land_ice_segments/geophysical/solar_elevation",
    "/gt3l/land_ice_segments/geophysical/solar_elevation",
    "/gt3r/land_ice_segments/geophysical/solar_elevation"
  ],
  "start_cycle": [
    "/ancillary_data/start_cycle"
  ],
  "start_delta_time": [
    "/ancillary_data/start_delta_time"
  ],
  "start_geoseg": [
    "/ancillary_data/start_geoseg"
  ],
  "start_gpssow": [
    "/ancillary_data/start_gpssow"
  ],
  "start_gpsweek": [
    "/ancillary_data/start_gpsweek"
  ],
  "start_orbit": [
    "/ancillary_data/start_orbit"
  ],
  "start_region": [
    "/ancillary_data/start_region"
  ],
  "start_rgt": [
    "/ancillary_data/start_rgt"
  ],
  "t_dead": [
    "/ancillary_data/land_ice/t_dead"
  ],
  "tide_earth": [
    "/gt1l/land_ice_segments/geophysical/tide_earth",
    "/gt1r/land_ice_segments/geophysical/tide_earth",
    "/gt2l/land_ice_segments/geophysical/tide_earth",
    "/gt2r/land_ice_segments/geophysical/tide_earth",
    "/gt3l/land_ice_segments/geophysical/tide_earth",
    "/gt3r/land_ice_segments/geophysical/tide_earth"
  ],
  "tide_earth_free2mean": [
    "/gt1l/land_ice_segments/geophysical/tide_earth_free2mean",
    "/gt1r/land_ice_segments/geophysical/tide_earth_free2mean",
    "/gt2l/land_ice_segments/geophysical/tide_earth_free2mean",
    "/gt2r/land_ice_segments/geophysical/tide_earth_free2mean",
    "/gt3l/land_ice_segments/geophysical/tide_earth_free2mean",
    "/gt3r/land_ice_segments/geophysical/tide_earth_free2mean"
  ],
  "tide_equilibrium": [
    "/gt1l/land_ice_segments/geophysical/tide_equilibrium",
    "/gt1r/land_ice_segments/geophysical/tide_equilibrium",
    "/gt2l/land_ice_segments/geophysical/tide_equilibrium",
    "/gt2r/land_ice_segments/geophysical/tide_equilibrium",
    "/gt3l/land_ice_segments/geophysical/tide_equilibrium",
    "/gt3r/land_ice_segments/geophysical/tide_equilibrium"
  ],
  "tide_load": [
    "/gt1l/land_ice_segments/geophysical/tide_load",
    "/gt1r/land_ice_segments/geophysical/tide_load",
    "/gt2l/land_ice_segments/geophysical/tide_load",
    "/gt2r/land_ice_segments/geophysical/tide_load",
    "/gt3l/land_ice_segments/geophysical/tide_load",
    "/gt3r/land_ice_segments/geophysical/tide_load"
  ],
  "tide_ocean": [
    "/gt1l/land_ice_segments/geophysical/tide_ocean",
    "/gt1r/land_ice_segments/geophysical/tide_ocean",
    "/gt2l/land_ice_segments/geophysical/tide_ocean",
    "/gt2r/land_ice_segments/geophysical/tide_ocean",
    "/gt3l/land_ice_segments/geophysical/tide_ocean",
    "/gt3r/land_ice_segments/geophysical/tide_ocean"
  ],
  "tide_pole": [
    "/gt1l/land_ice_segments/geophysical/tide_pole",
    "/gt1r/land_ice_segments/geophysical/tide_pole",
    "/gt2l/land_ice_segments/geophysical/tide_pole",
    "/gt2r/land_ice_segments/geophysical/tide_pole",
    "/gt3l/land_ice_segments/geophysical/tide_pole",
    "/gt3r/land_ice_segments/geophysical/tide_pole"
  ],
  "tx_mean_corr": [
    "/gt1l/land_ice_segments/bias_correction/tx_mean_corr",
    "/gt1r/land_ice_segments/bias_correction/tx_mean_corr",
    "/gt2l/land_ice_segments/bias_correction/tx_mean_corr",
    "/gt2r/land_ice_segments/bias_correction/tx_mean_corr",
    "/gt3l/land_ice_segments/bias_correction/tx_mean_corr",
    "/gt3r/land_ice_segments/bias_correction/tx_mean_corr"
  ],
  "tx_med_corr": [
    "/gt1l/land_ice_segments/bias_correction/tx_med_corr",
    "/gt1r/land_ice_segments/bias_correction/tx_med_corr",
    "/gt2l/land_ice_segments/bias_correction/tx_med_corr",
    "/gt2r/land_ice_segments/bias_correction/tx_med_corr",
    "/gt3l/land_ice_segments/bias_correction/tx_med_corr",
    "/gt3r/land_ice_segments/bias_correction/tx_med_corr"
  ],
  "txp_maxiter": [
    "/ancillary_data/land_ice/txp_maxiter"
  ],
  "version": [
    "/ancillary_data/version"
  ],
  "w_surface_window_final": [
    "/gt1l/land_ice_segments/fit_statistics/w_surface_window_final",
    "/gt1r/land_ice_segments/fit_statistics/w_surface_window_final",
    "/gt2l/land_ice_segments/fit_statistics/w_surface_window_final",
    "/gt2r/land_ice_segments/fit_statistics/w_surface_window_final",
    "/gt3l/land_ice_segments/fit_statistics/w_surface_window_final",
    "/gt3r/land_ice_segments/fit_statistics/w_surface_window_final"
  ],
  "x_atc": [
    "/gt1l/land_ice_segments/ground_track/x_atc",
    "/gt1r/land_ice_segments/ground_track/x_atc",
    "/gt2l/land_ice_segments/ground_track/x_atc",
    "/gt2r/land_ice_segments/ground_track/x_atc",
    "/gt3l/land_ice_segments/ground_track/x_atc",
    "/gt3r/land_ice_segments/ground_track/x_atc"
  ],
  "x_atc_mean": [
    "/gt1l/residual_histogram/x_atc_mean",
    "/gt1r/residual_histogram/x_atc_mean",
    "/gt2l/residual_histogram/x_atc_mean",
    "/gt2r/residual_histogram/x_atc_mean",
    "/gt3l/residual_histogram/x_atc_mean",
    "/gt3r/residual_histogram/x_atc_mean"
  ],
  "y_atc": [
    "/gt1l/land_ice_segments/ground_track/y_atc",
    "/gt1r/land_ice_segments/ground_track/y_atc",
    "/gt2l/land_ice_segments/ground_track/y_atc",
    "/gt2r/land_ice_segments/ground_track/y_atc",
    "/gt3l/land_ice_segments/ground_track/y_atc",
    "/gt3r/land_ice_segments/ground_track/y_atc"
  ]
}

###############################################################################
################################### ATL08 #####################################
###############################################################################

ATL08_var_dict = \
{
  "ancillary_data": [
    "/ancillary_data"
  ],
  "asr": [
    "/gt1l/land_segments/asr",
    "/gt1r/land_segments/asr",
    "/gt2l/land_segments/asr",
    "/gt2r/land_segments/asr",
    "/gt3l/land_segments/asr",
    "/gt3r/land_segments/asr"
  ],
  "atl08_region": [
    "/ancillary_data/land/atl08_region"
  ],
  "atlas_pa": [
    "/gt1l/land_segments/atlas_pa",
    "/gt1r/land_segments/atlas_pa",
    "/gt2l/land_segments/atlas_pa",
    "/gt2r/land_segments/atlas_pa",
    "/gt3l/land_segments/atlas_pa",
    "/gt3r/land_segments/atlas_pa"
  ],
  "atlas_sdp_gps_epoch": [
    "/ancillary_data/atlas_sdp_gps_epoch"
  ],
  "beam_azimuth": [
    "/gt1l/land_segments/beam_azimuth",
    "/gt1r/land_segments/beam_azimuth",
    "/gt2l/land_segments/beam_azimuth",
    "/gt2r/land_segments/beam_azimuth",
    "/gt3l/land_segments/beam_azimuth",
    "/gt3r/land_segments/beam_azimuth"
  ],
  "beam_coelev": [
    "/gt1l/land_segments/beam_coelev",
    "/gt1r/land_segments/beam_coelev",
    "/gt2l/land_segments/beam_coelev",
    "/gt2r/land_segments/beam_coelev",
    "/gt3l/land_segments/beam_coelev",
    "/gt3r/land_segments/beam_coelev"
  ],
  "bin_size_h": [
    "/ancillary_data/land/bin_size_h"
  ],
  "bin_size_n": [
    "/ancillary_data/land/bin_size_n"
  ],
  "bright_thresh": [
    "/ancillary_data/land/bright_thresh"
  ],
  "brightness_flag": [
    "/gt1l/land_segments/brightness_flag",
    "/gt1r/land_segments/brightness_flag",
    "/gt2l/land_segments/brightness_flag",
    "/gt2r/land_segments/brightness_flag",
    "/gt3l/land_segments/brightness_flag",
    "/gt3r/land_segments/brightness_flag"
  ],
  "ca_class": [
    "/ancillary_data/land/ca_class"
  ],
  "can_noise_thresh": [
    "/ancillary_data/land/can_noise_thresh"
  ],
  "can_stat_thresh": [
    "/ancillary_data/land/can_stat_thresh"
  ],
  "canopy": [
    "/gt1l/land_segments/canopy",
    "/gt1r/land_segments/canopy",
    "/gt2l/land_segments/canopy",
    "/gt2r/land_segments/canopy",
    "/gt3l/land_segments/canopy",
    "/gt3r/land_segments/canopy"
  ],
  "canopy_flag": [
    "/gt1l/land_segments/canopy/canopy_flag",
    "/gt1r/land_segments/canopy/canopy_flag",
    "/gt2l/land_segments/canopy/canopy_flag",
    "/gt2r/land_segments/canopy/canopy_flag",
    "/gt3l/land_segments/canopy/canopy_flag",
    "/gt3r/land_segments/canopy/canopy_flag"
  ],
  "canopy_flag_switch": [
    "/ancillary_data/land/canopy_flag_switch"
  ],
  "canopy_h_metrics": [
    "/gt1l/land_segments/canopy/canopy_h_metrics",
    "/gt1r/land_segments/canopy/canopy_h_metrics",
    "/gt2l/land_segments/canopy/canopy_h_metrics",
    "/gt2r/land_segments/canopy/canopy_h_metrics",
    "/gt3l/land_segments/canopy/canopy_h_metrics",
    "/gt3r/land_segments/canopy/canopy_h_metrics"
  ],
  "canopy_h_metrics_abs": [
    "/gt1l/land_segments/canopy/canopy_h_metrics_abs",
    "/gt1r/land_segments/canopy/canopy_h_metrics_abs",
    "/gt2l/land_segments/canopy/canopy_h_metrics_abs",
    "/gt2r/land_segments/canopy/canopy_h_metrics_abs",
    "/gt3l/land_segments/canopy/canopy_h_metrics_abs",
    "/gt3r/land_segments/canopy/canopy_h_metrics_abs"
  ],
  "canopy_openness": [
    "/gt1l/land_segments/canopy/canopy_openness",
    "/gt1r/land_segments/canopy/canopy_openness",
    "/gt2l/land_segments/canopy/canopy_openness",
    "/gt2r/land_segments/canopy/canopy_openness",
    "/gt3l/land_segments/canopy/canopy_openness",
    "/gt3r/land_segments/canopy/canopy_openness"
  ],
  "canopy_rh_conf": [
    "/gt1l/land_segments/canopy/canopy_rh_conf",
    "/gt1r/land_segments/canopy/canopy_rh_conf",
    "/gt2l/land_segments/canopy/canopy_rh_conf",
    "/gt2r/land_segments/canopy/canopy_rh_conf",
    "/gt3l/land_segments/canopy/canopy_rh_conf",
    "/gt3r/land_segments/canopy/canopy_rh_conf"
  ],
  "canopy_seg": [
    "/ancillary_data/land/canopy_seg"
  ],
  "centroid_height": [
    "/gt1l/land_segments/canopy/centroid_height",
    "/gt1r/land_segments/canopy/centroid_height",
    "/gt2l/land_segments/canopy/centroid_height",
    "/gt2r/land_segments/canopy/centroid_height",
    "/gt3l/land_segments/canopy/centroid_height",
    "/gt3r/land_segments/canopy/centroid_height"
  ],
  "class_thresh": [
    "/ancillary_data/land/class_thresh"
  ],
  "classed_pc_flag": [
    "/gt1l/signal_photons/classed_pc_flag",
    "/gt1r/signal_photons/classed_pc_flag",
    "/gt2l/signal_photons/classed_pc_flag",
    "/gt2r/signal_photons/classed_pc_flag",
    "/gt3l/signal_photons/classed_pc_flag",
    "/gt3r/signal_photons/classed_pc_flag"
  ],
  "classed_pc_indx": [
    "/gt1l/signal_photons/classed_pc_indx",
    "/gt1r/signal_photons/classed_pc_indx",
    "/gt2l/signal_photons/classed_pc_indx",
    "/gt2r/signal_photons/classed_pc_indx",
    "/gt3l/signal_photons/classed_pc_indx",
    "/gt3r/signal_photons/classed_pc_indx"
  ],
  "cloud_filter_switch": [
    "/ancillary_data/land/cloud_filter_switch"
  ],
  "cloud_flag_atm": [
    "/gt1l/land_segments/cloud_flag_atm",
    "/gt1r/land_segments/cloud_flag_atm",
    "/gt2l/land_segments/cloud_flag_atm",
    "/gt2r/land_segments/cloud_flag_atm",
    "/gt3l/land_segments/cloud_flag_atm",
    "/gt3r/land_segments/cloud_flag_atm"
  ],
  "cloud_fold_flag": [
    "/gt1l/land_segments/cloud_fold_flag",
    "/gt1r/land_segments/cloud_fold_flag",
    "/gt2l/land_segments/cloud_fold_flag",
    "/gt2r/land_segments/cloud_fold_flag",
    "/gt3l/land_segments/cloud_fold_flag",
    "/gt3r/land_segments/cloud_fold_flag"
  ],
  "control": [
    "/ancillary_data/control"
  ],
  "crossing_time": [
    "/orbit_info/crossing_time"
  ],
  "cycle_number": [
    "/orbit_info/cycle_number"
  ],
  "d_flag": [
    "/gt1l/signal_photons/d_flag",
    "/gt1r/signal_photons/d_flag",
    "/gt2l/signal_photons/d_flag",
    "/gt2r/signal_photons/d_flag",
    "/gt3l/signal_photons/d_flag",
    "/gt3r/signal_photons/d_flag"
  ],
  "data_end_utc": [
    "/ancillary_data/data_end_utc"
  ],
  "data_start_utc": [
    "/ancillary_data/data_start_utc"
  ],
  "del_amp": [
    "/ancillary_data/land/del_amp"
  ],
  "del_mu": [
    "/ancillary_data/land/del_mu"
  ],
  "del_sigma": [
    "/ancillary_data/land/del_sigma"
  ],
  "delta_time": [
    "/gt1l/land_segments/delta_time",
    "/gt1l/signal_photons/delta_time",
    "/gt1r/land_segments/delta_time",
    "/gt1r/signal_photons/delta_time",
    "/gt2l/land_segments/delta_time",
    "/gt2l/signal_photons/delta_time",
    "/gt2r/land_segments/delta_time",
    "/gt2r/signal_photons/delta_time",
    "/gt3l/land_segments/delta_time",
    "/gt3l/signal_photons/delta_time",
    "/gt3r/land_segments/delta_time",
    "/gt3r/signal_photons/delta_time"
  ],
  "delta_time_beg": [
    "/gt1l/land_segments/delta_time_beg",
    "/gt1r/land_segments/delta_time_beg",
    "/gt2l/land_segments/delta_time_beg",
    "/gt2r/land_segments/delta_time_beg",
    "/gt3l/land_segments/delta_time_beg",
    "/gt3r/land_segments/delta_time_beg"
  ],
  "delta_time_end": [
    "/gt1l/land_segments/delta_time_end",
    "/gt1r/land_segments/delta_time_end",
    "/gt2l/land_segments/delta_time_end",
    "/gt2r/land_segments/delta_time_end",
    "/gt3l/land_segments/delta_time_end",
    "/gt3r/land_segments/delta_time_end"
  ],
  "dem_filter_switch": [
    "/ancillary_data/land/dem_filter_switch"
  ],
  "dem_flag": [
    "/gt1l/land_segments/dem_flag",
    "/gt1r/land_segments/dem_flag",
    "/gt2l/land_segments/dem_flag",
    "/gt2r/land_segments/dem_flag",
    "/gt3l/land_segments/dem_flag",
    "/gt3r/land_segments/dem_flag"
  ],
  "dem_h": [
    "/gt1l/land_segments/dem_h",
    "/gt1r/land_segments/dem_h",
    "/gt2l/land_segments/dem_h",
    "/gt2r/land_segments/dem_h",
    "/gt3l/land_segments/dem_h",
    "/gt3r/land_segments/dem_h"
  ],
  "dem_removal_flag": [
    "/gt1l/land_segments/dem_removal_flag",
    "/gt1r/land_segments/dem_removal_flag",
    "/gt2l/land_segments/dem_removal_flag",
    "/gt2r/land_segments/dem_removal_flag",
    "/gt3l/land_segments/dem_removal_flag",
    "/gt3r/land_segments/dem_removal_flag"
  ],
  "dem_removal_percent_limit": [
    "/ancillary_data/land/dem_removal_percent_limit"
  ],
  "dragann_switch": [
    "/ancillary_data/land/dragann_switch"
  ],
  "ds_geosegments": [
    "//ds_geosegments"
  ],
  "ds_metrics": [
    "//ds_metrics"
  ],
  "ds_surf_type": [
    "//ds_surf_type"
  ],
  "dseg": [
    "/ancillary_data/land/dseg"
  ],
  "dseg_buf": [
    "/ancillary_data/land/dseg_buf"
  ],
  "end_cycle": [
    "/ancillary_data/end_cycle"
  ],
  "end_delta_time": [
    "/ancillary_data/end_delta_time"
  ],
  "end_geoseg": [
    "/ancillary_data/end_geoseg"
  ],
  "end_gpssow": [
    "/ancillary_data/end_gpssow"
  ],
  "end_gpsweek": [
    "/ancillary_data/end_gpsweek"
  ],
  "end_orbit": [
    "/ancillary_data/end_orbit"
  ],
  "end_region": [
    "/ancillary_data/end_region"
  ],
  "end_rgt": [
    "/ancillary_data/end_rgt"
  ],
  "fnlgnd_filter_switch": [
    "/ancillary_data/land/fnlgnd_filter_switch"
  ],
  "gnd_stat_thresh": [
    "/ancillary_data/land/gnd_stat_thresh"
  ],
  "granule_end_utc": [
    "/ancillary_data/granule_end_utc"
  ],
  "granule_start_utc": [
    "/ancillary_data/granule_start_utc"
  ],
  "gthresh_factor": [
    "/ancillary_data/land/gthresh_factor"
  ],
  "h_canopy": [
    "/gt1l/land_segments/canopy/h_canopy",
    "/gt1r/land_segments/canopy/h_canopy",
    "/gt2l/land_segments/canopy/h_canopy",
    "/gt2r/land_segments/canopy/h_canopy",
    "/gt3l/land_segments/canopy/h_canopy",
    "/gt3r/land_segments/canopy/h_canopy"
  ],
  "h_canopy_abs": [
    "/gt1l/land_segments/canopy/h_canopy_abs",
    "/gt1r/land_segments/canopy/h_canopy_abs",
    "/gt2l/land_segments/canopy/h_canopy_abs",
    "/gt2r/land_segments/canopy/h_canopy_abs",
    "/gt3l/land_segments/canopy/h_canopy_abs",
    "/gt3r/land_segments/canopy/h_canopy_abs"
  ],
  "h_canopy_perc": [
    "/ancillary_data/land/h_canopy_perc"
  ],
  "h_canopy_quad": [
    "/gt1l/land_segments/canopy/h_canopy_quad",
    "/gt1r/land_segments/canopy/h_canopy_quad",
    "/gt2l/land_segments/canopy/h_canopy_quad",
    "/gt2r/land_segments/canopy/h_canopy_quad",
    "/gt3l/land_segments/canopy/h_canopy_quad",
    "/gt3r/land_segments/canopy/h_canopy_quad"
  ],
  "h_canopy_uncertainty": [
    "/gt1l/land_segments/canopy/h_canopy_uncertainty",
    "/gt1r/land_segments/canopy/h_canopy_uncertainty",
    "/gt2l/land_segments/canopy/h_canopy_uncertainty",
    "/gt2r/land_segments/canopy/h_canopy_uncertainty",
    "/gt3l/land_segments/canopy/h_canopy_uncertainty",
    "/gt3r/land_segments/canopy/h_canopy_uncertainty"
  ],
  "h_dif_canopy": [
    "/gt1l/land_segments/canopy/h_dif_canopy",
    "/gt1r/land_segments/canopy/h_dif_canopy",
    "/gt2l/land_segments/canopy/h_dif_canopy",
    "/gt2r/land_segments/canopy/h_dif_canopy",
    "/gt3l/land_segments/canopy/h_dif_canopy",
    "/gt3r/land_segments/canopy/h_dif_canopy"
  ],
  "h_dif_ref": [
    "/gt1l/land_segments/h_dif_ref",
    "/gt1r/land_segments/h_dif_ref",
    "/gt2l/land_segments/h_dif_ref",
    "/gt2r/land_segments/h_dif_ref",
    "/gt3l/land_segments/h_dif_ref",
    "/gt3r/land_segments/h_dif_ref"
  ],
  "h_max_canopy": [
    "/gt1l/land_segments/canopy/h_max_canopy",
    "/gt1r/land_segments/canopy/h_max_canopy",
    "/gt2l/land_segments/canopy/h_max_canopy",
    "/gt2r/land_segments/canopy/h_max_canopy",
    "/gt3l/land_segments/canopy/h_max_canopy",
    "/gt3r/land_segments/canopy/h_max_canopy"
  ],
  "h_max_canopy_abs": [
    "/gt1l/land_segments/canopy/h_max_canopy_abs",
    "/gt1r/land_segments/canopy/h_max_canopy_abs",
    "/gt2l/land_segments/canopy/h_max_canopy_abs",
    "/gt2r/land_segments/canopy/h_max_canopy_abs",
    "/gt3l/land_segments/canopy/h_max_canopy_abs",
    "/gt3r/land_segments/canopy/h_max_canopy_abs"
  ],
  "h_mean_canopy": [
    "/gt1l/land_segments/canopy/h_mean_canopy",
    "/gt1r/land_segments/canopy/h_mean_canopy",
    "/gt2l/land_segments/canopy/h_mean_canopy",
    "/gt2r/land_segments/canopy/h_mean_canopy",
    "/gt3l/land_segments/canopy/h_mean_canopy",
    "/gt3r/land_segments/canopy/h_mean_canopy"
  ],
  "h_mean_canopy_abs": [
    "/gt1l/land_segments/canopy/h_mean_canopy_abs",
    "/gt1r/land_segments/canopy/h_mean_canopy_abs",
    "/gt2l/land_segments/canopy/h_mean_canopy_abs",
    "/gt2r/land_segments/canopy/h_mean_canopy_abs",
    "/gt3l/land_segments/canopy/h_mean_canopy_abs",
    "/gt3r/land_segments/canopy/h_mean_canopy_abs"
  ],
  "h_median_canopy": [
    "/gt1l/land_segments/canopy/h_median_canopy",
    "/gt1r/land_segments/canopy/h_median_canopy",
    "/gt2l/land_segments/canopy/h_median_canopy",
    "/gt2r/land_segments/canopy/h_median_canopy",
    "/gt3l/land_segments/canopy/h_median_canopy",
    "/gt3r/land_segments/canopy/h_median_canopy"
  ],
  "h_median_canopy_abs": [
    "/gt1l/land_segments/canopy/h_median_canopy_abs",
    "/gt1r/land_segments/canopy/h_median_canopy_abs",
    "/gt2l/land_segments/canopy/h_median_canopy_abs",
    "/gt2r/land_segments/canopy/h_median_canopy_abs",
    "/gt3l/land_segments/canopy/h_median_canopy_abs",
    "/gt3r/land_segments/canopy/h_median_canopy_abs"
  ],
  "h_min_canopy": [
    "/gt1l/land_segments/canopy/h_min_canopy",
    "/gt1r/land_segments/canopy/h_min_canopy",
    "/gt2l/land_segments/canopy/h_min_canopy",
    "/gt2r/land_segments/canopy/h_min_canopy",
    "/gt3l/land_segments/canopy/h_min_canopy",
    "/gt3r/land_segments/canopy/h_min_canopy"
  ],
  "h_min_canopy_abs": [
    "/gt1l/land_segments/canopy/h_min_canopy_abs",
    "/gt1r/land_segments/canopy/h_min_canopy_abs",
    "/gt2l/land_segments/canopy/h_min_canopy_abs",
    "/gt2r/land_segments/canopy/h_min_canopy_abs",
    "/gt3l/land_segments/canopy/h_min_canopy_abs",
    "/gt3r/land_segments/canopy/h_min_canopy_abs"
  ],
  "h_te_best_fit": [
    "/gt1l/land_segments/terrain/h_te_best_fit",
    "/gt1r/land_segments/terrain/h_te_best_fit",
    "/gt2l/land_segments/terrain/h_te_best_fit",
    "/gt2r/land_segments/terrain/h_te_best_fit",
    "/gt3l/land_segments/terrain/h_te_best_fit",
    "/gt3r/land_segments/terrain/h_te_best_fit"
  ],
  "h_te_interp": [
    "/gt1l/land_segments/terrain/h_te_interp",
    "/gt1r/land_segments/terrain/h_te_interp",
    "/gt2l/land_segments/terrain/h_te_interp",
    "/gt2r/land_segments/terrain/h_te_interp",
    "/gt3l/land_segments/terrain/h_te_interp",
    "/gt3r/land_segments/terrain/h_te_interp"
  ],
  "h_te_max": [
    "/gt1l/land_segments/terrain/h_te_max",
    "/gt1r/land_segments/terrain/h_te_max",
    "/gt2l/land_segments/terrain/h_te_max",
    "/gt2r/land_segments/terrain/h_te_max",
    "/gt3l/land_segments/terrain/h_te_max",
    "/gt3r/land_segments/terrain/h_te_max"
  ],
  "h_te_mean": [
    "/gt1l/land_segments/terrain/h_te_mean",
    "/gt1r/land_segments/terrain/h_te_mean",
    "/gt2l/land_segments/terrain/h_te_mean",
    "/gt2r/land_segments/terrain/h_te_mean",
    "/gt3l/land_segments/terrain/h_te_mean",
    "/gt3r/land_segments/terrain/h_te_mean"
  ],
  "h_te_median": [
    "/gt1l/land_segments/terrain/h_te_median",
    "/gt1r/land_segments/terrain/h_te_median",
    "/gt2l/land_segments/terrain/h_te_median",
    "/gt2r/land_segments/terrain/h_te_median",
    "/gt3l/land_segments/terrain/h_te_median",
    "/gt3r/land_segments/terrain/h_te_median"
  ],
  "h_te_min": [
    "/gt1l/land_segments/terrain/h_te_min",
    "/gt1r/land_segments/terrain/h_te_min",
    "/gt2l/land_segments/terrain/h_te_min",
    "/gt2r/land_segments/terrain/h_te_min",
    "/gt3l/land_segments/terrain/h_te_min",
    "/gt3r/land_segments/terrain/h_te_min"
  ],
  "h_te_mode": [
    "/gt1l/land_segments/terrain/h_te_mode",
    "/gt1r/land_segments/terrain/h_te_mode",
    "/gt2l/land_segments/terrain/h_te_mode",
    "/gt2r/land_segments/terrain/h_te_mode",
    "/gt3l/land_segments/terrain/h_te_mode",
    "/gt3r/land_segments/terrain/h_te_mode"
  ],
  "h_te_rh25": [
    "/gt1l/land_segments/terrain/h_te_rh25",
    "/gt1r/land_segments/terrain/h_te_rh25",
    "/gt2l/land_segments/terrain/h_te_rh25",
    "/gt2r/land_segments/terrain/h_te_rh25",
    "/gt3l/land_segments/terrain/h_te_rh25",
    "/gt3r/land_segments/terrain/h_te_rh25"
  ],
  "h_te_skew": [
    "/gt1l/land_segments/terrain/h_te_skew",
    "/gt1r/land_segments/terrain/h_te_skew",
    "/gt2l/land_segments/terrain/h_te_skew",
    "/gt2r/land_segments/terrain/h_te_skew",
    "/gt3l/land_segments/terrain/h_te_skew",
    "/gt3r/land_segments/terrain/h_te_skew"
  ],
  "h_te_std": [
    "/gt1l/land_segments/terrain/h_te_std",
    "/gt1r/land_segments/terrain/h_te_std",
    "/gt2l/land_segments/terrain/h_te_std",
    "/gt2r/land_segments/terrain/h_te_std",
    "/gt3l/land_segments/terrain/h_te_std",
    "/gt3r/land_segments/terrain/h_te_std"
  ],
  "h_te_uncertainty": [
    "/gt1l/land_segments/terrain/h_te_uncertainty",
    "/gt1r/land_segments/terrain/h_te_uncertainty",
    "/gt2l/land_segments/terrain/h_te_uncertainty",
    "/gt2r/land_segments/terrain/h_te_uncertainty",
    "/gt3l/land_segments/terrain/h_te_uncertainty",
    "/gt3r/land_segments/terrain/h_te_uncertainty"
  ],
  "iter_gnd": [
    "/ancillary_data/land/iter_gnd"
  ],
  "iter_max": [
    "/ancillary_data/land/iter_max"
  ],
  "lan": [
    "/orbit_info/lan"
  ],
  "land": [
    "/ancillary_data/land"
  ],
  "land_segments": [
    "/gt1l/land_segments",
    "/gt1r/land_segments",
    "/gt2l/land_segments",
    "/gt2r/land_segments",
    "/gt3l/land_segments",
    "/gt3r/land_segments"
  ],
  "landsat_flag": [
    "/gt1l/land_segments/canopy/landsat_flag",
    "/gt1r/land_segments/canopy/landsat_flag",
    "/gt2l/land_segments/canopy/landsat_flag",
    "/gt2r/land_segments/canopy/landsat_flag",
    "/gt3l/land_segments/canopy/landsat_flag",
    "/gt3r/land_segments/canopy/landsat_flag"
  ],
  "landsat_perc": [
    "/gt1l/land_segments/canopy/landsat_perc",
    "/gt1r/land_segments/canopy/landsat_perc",
    "/gt2l/land_segments/canopy/landsat_perc",
    "/gt2r/land_segments/canopy/landsat_perc",
    "/gt3l/land_segments/canopy/landsat_perc",
    "/gt3r/land_segments/canopy/landsat_perc"
  ],
  "last_seg_extend": [
    "/gt1l/land_segments/last_seg_extend",
    "/gt1r/land_segments/last_seg_extend",
    "/gt2l/land_segments/last_seg_extend",
    "/gt2r/land_segments/last_seg_extend",
    "/gt3l/land_segments/last_seg_extend",
    "/gt3r/land_segments/last_seg_extend"
  ],
  "latitude": [
    "/gt1l/land_segments/latitude",
    "/gt1r/land_segments/latitude",
    "/gt2l/land_segments/latitude",
    "/gt2r/land_segments/latitude",
    "/gt3l/land_segments/latitude",
    "/gt3r/land_segments/latitude"
  ],
  "layer_flag": [
    "/gt1l/land_segments/layer_flag",
    "/gt1r/land_segments/layer_flag",
    "/gt2l/land_segments/layer_flag",
    "/gt2r/land_segments/layer_flag",
    "/gt3l/land_segments/layer_flag",
    "/gt3r/land_segments/layer_flag"
  ],
  "longitude": [
    "/gt1l/land_segments/longitude",
    "/gt1r/land_segments/longitude",
    "/gt2l/land_segments/longitude",
    "/gt2r/land_segments/longitude",
    "/gt3l/land_segments/longitude",
    "/gt3r/land_segments/longitude"
  ],
  "lseg": [
    "/ancillary_data/land/lseg"
  ],
  "lseg_buf": [
    "/ancillary_data/land/lseg_buf"
  ],
  "lw_filt_bnd": [
    "/ancillary_data/land/lw_filt_bnd"
  ],
  "lw_gnd_bnd": [
    "/ancillary_data/land/lw_gnd_bnd"
  ],
  "lw_toc_bnd": [
    "/ancillary_data/land/lw_toc_bnd"
  ],
  "lw_toc_cut": [
    "/ancillary_data/land/lw_toc_cut"
  ],
  "max_atl03files": [
    "/ancillary_data/land/max_atl03files"
  ],
  "max_atl09files": [
    "/ancillary_data/land/max_atl09files"
  ],
  "max_peaks": [
    "/ancillary_data/land/max_peaks"
  ],
  "max_try": [
    "/ancillary_data/land/max_try"
  ],
  "min_nphs": [
    "/ancillary_data/land/min_nphs"
  ],
  "msw_flag": [
    "/gt1l/land_segments/msw_flag",
    "/gt1r/land_segments/msw_flag",
    "/gt2l/land_segments/msw_flag",
    "/gt2r/land_segments/msw_flag",
    "/gt3l/land_segments/msw_flag",
    "/gt3r/land_segments/msw_flag"
  ],
  "n_ca_photons": [
    "/gt1l/land_segments/canopy/n_ca_photons",
    "/gt1r/land_segments/canopy/n_ca_photons",
    "/gt2l/land_segments/canopy/n_ca_photons",
    "/gt2r/land_segments/canopy/n_ca_photons",
    "/gt3l/land_segments/canopy/n_ca_photons",
    "/gt3r/land_segments/canopy/n_ca_photons"
  ],
  "n_dec_mode": [
    "/ancillary_data/land/n_dec_mode"
  ],
  "n_seg_ph": [
    "/gt1l/land_segments/n_seg_ph",
    "/gt1r/land_segments/n_seg_ph",
    "/gt2l/land_segments/n_seg_ph",
    "/gt2r/land_segments/n_seg_ph",
    "/gt3l/land_segments/n_seg_ph",
    "/gt3r/land_segments/n_seg_ph"
  ],
  "n_te_photons": [
    "/gt1l/land_segments/terrain/n_te_photons",
    "/gt1r/land_segments/terrain/n_te_photons",
    "/gt2l/land_segments/terrain/n_te_photons",
    "/gt2r/land_segments/terrain/n_te_photons",
    "/gt3l/land_segments/terrain/n_te_photons",
    "/gt3r/land_segments/terrain/n_te_photons"
  ],
  "n_toc_photons": [
    "/gt1l/land_segments/canopy/n_toc_photons",
    "/gt1r/land_segments/canopy/n_toc_photons",
    "/gt2l/land_segments/canopy/n_toc_photons",
    "/gt2r/land_segments/canopy/n_toc_photons",
    "/gt3l/land_segments/canopy/n_toc_photons",
    "/gt3r/land_segments/canopy/n_toc_photons"
  ],
  "night_flag": [
    "/gt1l/land_segments/night_flag",
    "/gt1r/land_segments/night_flag",
    "/gt2l/land_segments/night_flag",
    "/gt2r/land_segments/night_flag",
    "/gt3l/land_segments/night_flag",
    "/gt3r/land_segments/night_flag"
  ],
  "night_thresh": [
    "/ancillary_data/land/night_thresh"
  ],
  "noise_class": [
    "/ancillary_data/land/noise_class"
  ],
  "orbit_info": [
    "/orbit_info"
  ],
  "orbit_number": [
    "/orbit_info/orbit_number"
  ],
  "outlier_filter_switch": [
    "/ancillary_data/land/outlier_filter_switch"
  ],
  "p_static": [
    "/ancillary_data/land/p_static"
  ],
  "ph_h": [
    "/gt1l/signal_photons/ph_h",
    "/gt1r/signal_photons/ph_h",
    "/gt2l/signal_photons/ph_h",
    "/gt2r/signal_photons/ph_h",
    "/gt3l/signal_photons/ph_h",
    "/gt3r/signal_photons/ph_h"
  ],
  "ph_ndx_beg": [
    "/gt1l/land_segments/ph_ndx_beg",
    "/gt1r/land_segments/ph_ndx_beg",
    "/gt2l/land_segments/ph_ndx_beg",
    "/gt2r/land_segments/ph_ndx_beg",
    "/gt3l/land_segments/ph_ndx_beg",
    "/gt3r/land_segments/ph_ndx_beg"
  ],
  "ph_removal_flag": [
    "/gt1l/land_segments/ph_removal_flag",
    "/gt1r/land_segments/ph_removal_flag",
    "/gt2l/land_segments/ph_removal_flag",
    "/gt2r/land_segments/ph_removal_flag",
    "/gt3l/land_segments/ph_removal_flag",
    "/gt3r/land_segments/ph_removal_flag"
  ],
  "ph_removal_percent_limit": [
    "/ancillary_data/land/ph_removal_percent_limit"
  ],
  "ph_segment_id": [
    "/gt1l/signal_photons/ph_segment_id",
    "/gt1r/signal_photons/ph_segment_id",
    "/gt2l/signal_photons/ph_segment_id",
    "/gt2r/signal_photons/ph_segment_id",
    "/gt3l/signal_photons/ph_segment_id",
    "/gt3r/signal_photons/ph_segment_id"
  ],
  "photon_rate_can": [
    "/gt1l/land_segments/canopy/photon_rate_can",
    "/gt1r/land_segments/canopy/photon_rate_can",
    "/gt2l/land_segments/canopy/photon_rate_can",
    "/gt2r/land_segments/canopy/photon_rate_can",
    "/gt3l/land_segments/canopy/photon_rate_can",
    "/gt3r/land_segments/canopy/photon_rate_can"
  ],
  "photon_rate_te": [
    "/gt1l/land_segments/terrain/photon_rate_te",
    "/gt1r/land_segments/terrain/photon_rate_te",
    "/gt2l/land_segments/terrain/photon_rate_te",
    "/gt2r/land_segments/terrain/photon_rate_te",
    "/gt3l/land_segments/terrain/photon_rate_te",
    "/gt3r/land_segments/terrain/photon_rate_te"
  ],
  "proc_geoseg": [
    "/ancillary_data/land/proc_geoseg"
  ],
  "psf": [
    "/ancillary_data/land/psf"
  ],
  "psf_flag": [
    "/gt1l/land_segments/psf_flag",
    "/gt1r/land_segments/psf_flag",
    "/gt2l/land_segments/psf_flag",
    "/gt2r/land_segments/psf_flag",
    "/gt3l/land_segments/psf_flag",
    "/gt3r/land_segments/psf_flag"
  ],
  "qa_at_interval": [
    "/ancillary_data/qa_at_interval"
  ],
  "qa_granule_fail_reason": [
    "/quality_assessment/qa_granule_fail_reason"
  ],
  "qa_granule_pass_fail": [
    "/quality_assessment/qa_granule_pass_fail"
  ],
  "quality_assessment": [
    "/quality_assessment"
  ],
  "ref_dem_limit": [
    "/ancillary_data/land/ref_dem_limit"
  ],
  "ref_finalground_limit": [
    "/ancillary_data/land/ref_finalground_limit"
  ],
  "release": [
    "/ancillary_data/release"
  ],
  "relief_hbot": [
    "/ancillary_data/land/relief_hbot"
  ],
  "relief_htop": [
    "/ancillary_data/land/relief_htop"
  ],
  "rgt": [
    "/gt1l/land_segments/rgt",
    "/gt1r/land_segments/rgt",
    "/gt2l/land_segments/rgt",
    "/gt2r/land_segments/rgt",
    "/gt3l/land_segments/rgt",
    "/gt3r/land_segments/rgt",
    "/orbit_info/rgt"
  ],
  "sat_flag": [
    "/gt1l/land_segments/sat_flag",
    "/gt1r/land_segments/sat_flag",
    "/gt2l/land_segments/sat_flag",
    "/gt2r/land_segments/sat_flag",
    "/gt3l/land_segments/sat_flag",
    "/gt3r/land_segments/sat_flag"
  ],
  "sc_orient": [
    "/orbit_info/sc_orient"
  ],
  "sc_orient_time": [
    "/orbit_info/sc_orient_time"
  ],
  "segment_id_beg": [
    "/gt1l/land_segments/segment_id_beg",
    "/gt1r/land_segments/segment_id_beg",
    "/gt2l/land_segments/segment_id_beg",
    "/gt2r/land_segments/segment_id_beg",
    "/gt3l/land_segments/segment_id_beg",
    "/gt3r/land_segments/segment_id_beg"
  ],
  "segment_id_end": [
    "/gt1l/land_segments/segment_id_end",
    "/gt1r/land_segments/segment_id_end",
    "/gt2l/land_segments/segment_id_end",
    "/gt2r/land_segments/segment_id_end",
    "/gt3l/land_segments/segment_id_end",
    "/gt3r/land_segments/segment_id_end"
  ],
  "segment_landcover": [
    "/gt1l/land_segments/segment_landcover",
    "/gt1r/land_segments/segment_landcover",
    "/gt2l/land_segments/segment_landcover",
    "/gt2r/land_segments/segment_landcover",
    "/gt3l/land_segments/segment_landcover",
    "/gt3r/land_segments/segment_landcover"
  ],
  "segment_snowcover": [
    "/gt1l/land_segments/segment_snowcover",
    "/gt1r/land_segments/segment_snowcover",
    "/gt2l/land_segments/segment_snowcover",
    "/gt2r/land_segments/segment_snowcover",
    "/gt3l/land_segments/segment_snowcover",
    "/gt3r/land_segments/segment_snowcover"
  ],
  "segment_watermask": [
    "/gt1l/land_segments/segment_watermask",
    "/gt1r/land_segments/segment_watermask",
    "/gt2l/land_segments/segment_watermask",
    "/gt2r/land_segments/segment_watermask",
    "/gt3l/land_segments/segment_watermask",
    "/gt3r/land_segments/segment_watermask"
  ],
  "shp_param": [
    "/ancillary_data/land/shp_param"
  ],
  "sig_rsq_search": [
    "/ancillary_data/land/sig_rsq_search"
  ],
  "sigma_across": [
    "/gt1l/land_segments/sigma_across",
    "/gt1r/land_segments/sigma_across",
    "/gt2l/land_segments/sigma_across",
    "/gt2r/land_segments/sigma_across",
    "/gt3l/land_segments/sigma_across",
    "/gt3r/land_segments/sigma_across"
  ],
  "sigma_along": [
    "/gt1l/land_segments/sigma_along",
    "/gt1r/land_segments/sigma_along",
    "/gt2l/land_segments/sigma_along",
    "/gt2r/land_segments/sigma_along",
    "/gt3l/land_segments/sigma_along",
    "/gt3r/land_segments/sigma_along"
  ],
  "sigma_atlas_land": [
    "/gt1l/land_segments/sigma_atlas_land",
    "/gt1r/land_segments/sigma_atlas_land",
    "/gt2l/land_segments/sigma_atlas_land",
    "/gt2r/land_segments/sigma_atlas_land",
    "/gt3l/land_segments/sigma_atlas_land",
    "/gt3r/land_segments/sigma_atlas_land"
  ],
  "sigma_h": [
    "/gt1l/land_segments/sigma_h",
    "/gt1r/land_segments/sigma_h",
    "/gt2l/land_segments/sigma_h",
    "/gt2r/land_segments/sigma_h",
    "/gt3l/land_segments/sigma_h",
    "/gt3r/land_segments/sigma_h"
  ],
  "sigma_topo": [
    "/gt1l/land_segments/sigma_topo",
    "/gt1r/land_segments/sigma_topo",
    "/gt2l/land_segments/sigma_topo",
    "/gt2r/land_segments/sigma_topo",
    "/gt3l/land_segments/sigma_topo",
    "/gt3r/land_segments/sigma_topo"
  ],
  "signal_photons": [
    "/gt1l/signal_photons",
    "/gt1r/signal_photons",
    "/gt2l/signal_photons",
    "/gt2r/signal_photons",
    "/gt3l/signal_photons",
    "/gt3r/signal_photons"
  ],
  "snr": [
    "/gt1l/land_segments/snr",
    "/gt1r/land_segments/snr",
    "/gt2l/land_segments/snr",
    "/gt2r/land_segments/snr",
    "/gt3l/land_segments/snr",
    "/gt3r/land_segments/snr"
  ],
  "solar_azimuth": [
    "/gt1l/land_segments/solar_azimuth",
    "/gt1r/land_segments/solar_azimuth",
    "/gt2l/land_segments/solar_azimuth",
    "/gt2r/land_segments/solar_azimuth",
    "/gt3l/land_segments/solar_azimuth",
    "/gt3r/land_segments/solar_azimuth"
  ],
  "solar_elevation": [
    "/gt1l/land_segments/solar_elevation",
    "/gt1r/land_segments/solar_elevation",
    "/gt2l/land_segments/solar_elevation",
    "/gt2r/land_segments/solar_elevation",
    "/gt3l/land_segments/solar_elevation",
    "/gt3r/land_segments/solar_elevation"
  ],
  "sseg": [
    "/ancillary_data/land/sseg"
  ],
  "start_cycle": [
    "/ancillary_data/start_cycle"
  ],
  "start_delta_time": [
    "/ancillary_data/start_delta_time"
  ],
  "start_geoseg": [
    "/ancillary_data/start_geoseg"
  ],
  "start_gpssow": [
    "/ancillary_data/start_gpssow"
  ],
  "start_gpsweek": [
    "/ancillary_data/start_gpsweek"
  ],
  "start_orbit": [
    "/ancillary_data/start_orbit"
  ],
  "start_region": [
    "/ancillary_data/start_region"
  ],
  "start_rgt": [
    "/ancillary_data/start_rgt"
  ],
  "stat_thresh": [
    "/ancillary_data/land/stat_thresh"
  ],
  "subset_can_flag": [
    "/gt1l/land_segments/canopy/subset_can_flag",
    "/gt1r/land_segments/canopy/subset_can_flag",
    "/gt2l/land_segments/canopy/subset_can_flag",
    "/gt2r/land_segments/canopy/subset_can_flag",
    "/gt3l/land_segments/canopy/subset_can_flag",
    "/gt3r/land_segments/canopy/subset_can_flag"
  ],
  "subset_te_flag": [
    "/gt1l/land_segments/terrain/subset_te_flag",
    "/gt1r/land_segments/terrain/subset_te_flag",
    "/gt2l/land_segments/terrain/subset_te_flag",
    "/gt2r/land_segments/terrain/subset_te_flag",
    "/gt3l/land_segments/terrain/subset_te_flag",
    "/gt3r/land_segments/terrain/subset_te_flag"
  ],
  "surf_type": [
    "/gt1l/land_segments/surf_type",
    "/gt1r/land_segments/surf_type",
    "/gt2l/land_segments/surf_type",
    "/gt2r/land_segments/surf_type",
    "/gt3l/land_segments/surf_type",
    "/gt3r/land_segments/surf_type"
  ],
  "tc_thresh": [
    "/ancillary_data/land/tc_thresh"
  ],
  "te_class": [
    "/ancillary_data/land/te_class"
  ],
  "terrain": [
    "/gt1l/land_segments/terrain",
    "/gt1r/land_segments/terrain",
    "/gt2l/land_segments/terrain",
    "/gt2r/land_segments/terrain",
    "/gt3l/land_segments/terrain",
    "/gt3r/land_segments/terrain"
  ],
  "terrain_flg": [
    "/gt1l/land_segments/terrain_flg",
    "/gt1r/land_segments/terrain_flg",
    "/gt2l/land_segments/terrain_flg",
    "/gt2r/land_segments/terrain_flg",
    "/gt3l/land_segments/terrain_flg",
    "/gt3r/land_segments/terrain_flg"
  ],
  "terrain_slope": [
    "/gt1l/land_segments/terrain/terrain_slope",
    "/gt1r/land_segments/terrain/terrain_slope",
    "/gt2l/land_segments/terrain/terrain_slope",
    "/gt2r/land_segments/terrain/terrain_slope",
    "/gt3l/land_segments/terrain/terrain_slope",
    "/gt3r/land_segments/terrain/terrain_slope"
  ],
  "toc_class": [
    "/ancillary_data/land/toc_class"
  ],
  "toc_roughness": [
    "/gt1l/land_segments/canopy/toc_roughness",
    "/gt1r/land_segments/canopy/toc_roughness",
    "/gt2l/land_segments/canopy/toc_roughness",
    "/gt2r/land_segments/canopy/toc_roughness",
    "/gt3l/land_segments/canopy/toc_roughness",
    "/gt3r/land_segments/canopy/toc_roughness"
  ],
  "up_filt_bnd": [
    "/ancillary_data/land/up_filt_bnd"
  ],
  "up_gnd_bnd": [
    "/ancillary_data/land/up_gnd_bnd"
  ],
  "up_toc_bnd": [
    "/ancillary_data/land/up_toc_bnd"
  ],
  "up_toc_cut": [
    "/ancillary_data/land/up_toc_cut"
  ],
  "urban_flag": [
    "/gt1l/land_segments/urban_flag",
    "/gt1r/land_segments/urban_flag",
    "/gt2l/land_segments/urban_flag",
    "/gt2r/land_segments/urban_flag",
    "/gt3l/land_segments/urban_flag",
    "/gt3r/land_segments/urban_flag"
  ],
  "version": [
    "/ancillary_data/version"
  ]
}

def parse_args():
    parser = argparse.ArgumentParser(description="A utility to return a list of ICESat-2 granule variables given a set of variable ids.")
    parser.add_argument('var_ids', type=str, nargs="*",
                        help="ICESat-2 argument ids, separated by spaces. Run with --list_var_args to get a complete list of arguments for the given dataset.")
    parser.add_argument('-dataset_name', '-d', metavar="ATLXX", default="ATL03", help="Short name for the dataset. Default 'ATL03'.")
    parser.add_argument('-separator', '-s', metavar="CHAR", type=str, default='<newline>', help="Separator character for return values (default '<newline>'). For a newline, use '<newline>' (without the quotes). For space, use '<space>'. For any other character, just put the character (no quotes).")
    parser.add_argument('--list_var_ids', '-l', default=False, action="store_true", help="List all the dataset variable IDs for the given dataset DATASET_NAME. Overrides var_ids.")

    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()

    # print(args.var_ids)
    separator = args.separator.lower()
    if separator.find("newline") >= 0:
        separator = '\n'
    elif separator.find("space") >= 0:
        separator = ' '
    else:
        separator = args.separator

    if args.list_var_ids:
        print(separator.join(list_argument_ids(args.dataset_name)), end="")

    else:
        print(separator.join(accrue_variables(args.var_ids,
                                              dataset_name=args.dataset_name)), end="")
