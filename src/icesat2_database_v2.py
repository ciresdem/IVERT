# Functionality for reading ICESat-2 data and saving it in a tiled database.

import datetime
import dateparser
import geopandas
import numpy
import numexpr
import os
import pandas
import rasterio
import shapely
import shutil
import subprocess
import typing
import xarray

import icesat2_query
import utils.pickle_blosc
import utils.configfile
import utils.cuboid_funcs
import coastline_mask_v2

class IS2Database:

    def __init__(self,
                 ivert_config: typing.Union[utils.configfile.Config, None] = None):
        # Define the structure of the object.
        if ivert_config is None:
            self.config = utils.configfile.Config()
        else:
            self.config = ivert_config

        self.db_fname = self.config.icesat2_granules_gpkg
        self.db_fname_compressed = self.config.icesat2_granules_blosc
        self.gdf = None
        self.last_gdf_bbox = None
        self.last_gdf_date_range = None
        self.last_gdf_result = None

        # For now, we only build this database in WGS84 / EGM2008 coordinates.
        # Can experiment with other coordinate systems later.
        self.crs = "EPSG:4326+3855"

        self.granules_dir = self.config.icesat2_granules_directory
        self.icesat2_download_dir = self.config.icesat2_download_directory

    def create_new_database(self,
                            populate: bool = True,
                            overwrite: bool = False) -> geopandas.GeoDataFrame:
        """Create a new database from scratch.

        Parameters
        ----------
        populate : bool
            Whether to populate the database with the data from the tiles.
        overwrite : bool
            Whether to overwrite the database if it already exists.

        Raises
        ------
        OSError if database file cannot be created or already exists and overwrite is False.

        Returns
        -------
        geopandas.GeoDataFrame containing the photon tiles from the database.
        """
        if overwrite:
            if os.path.exists(self.db_fname):
                print("Removing old", os.path.basename(self.db_fname), end="")
                os.remove(self.db_fname)
            if os.path.exists(self.db_fname_compressed):
                print(" and", os.path.basename(self.db_fname_compressed), end="")
                os.remove(self.db_fname_compressed)
                print()
            else:
                print()

        elif os.path.exists(self.db_fname):
            raise OSError("Database file already exists. Use overwrite=True to overwrite it.")

        if populate:
            cdf_files = sorted([os.path.join(self.granules_dir, fn) for fn in os.listdir(self.granules_dir) if os.path.splitext(fn)[-1].lower() == ".cdf"])
            granule_ids = [None] * len(cdf_files)
            filenames = [None] * len(cdf_files)
            lasers = [None] * len(cdf_files)
            query_bboxes = [None] * len(cdf_files)
            data_bboxes = [None] * len(cdf_files)
            zbounds = [None] * len(cdf_files)
            numphotons = numpy.zeros((len(cdf_files),), dtype=int)
            numphotons_unclassified = numpy.zeros((len(cdf_files),), dtype=int)
            numphotons_noise = numpy.zeros((len(cdf_files),), dtype=int)
            numphotons_ground = numpy.zeros((len(cdf_files),), dtype=int)
            numphotons_canopy = numpy.zeros((len(cdf_files),), dtype=int)
            numphotons_canopy_top = numpy.zeros((len(cdf_files),), dtype=int)
            numphotons_bathy_floor = numpy.zeros((len(cdf_files),), dtype=int)
            numphotons_bathy_surface = numpy.zeros((len(cdf_files),), dtype=int)
            numphotons_buildings = numpy.zeros((len(cdf_files),), dtype=int)
            downloaded_on = numpy.zeros((len(cdf_files),), dtype=int)
            geometries = [None] * len(cdf_files)

            for i, cdf in enumerate(cdf_files):
                filenames[i] = os.path.basename(cdf)
                xds = xarray.open_dataset(cdf)

                xarray.set_options(display_max_rows=20)

                # print(xds)

                granule_ids[i] = xds.attrs["granule_id"]
                lasers[i] = xds.attrs["laser_name"]
                query_bboxes[i] = xds.attrs["query_bbox"]
                data_bbox = xds.attrs["data_bbox"]
                data_bboxes[i] = data_bbox
                zbounds[i] = xds.attrs["zbounds"]
                numphotons[i] = xds.attrs["numphotons"]
                numphotons_unclassified[i] = xds.attrs["numphotons_unclassified"]
                numphotons_noise[i] = xds.attrs["numphotons_noise"]
                numphotons_ground[i] = xds.attrs["numphotons_ground"]
                numphotons_canopy[i] = xds.attrs["numphotons_canopy"]
                numphotons_canopy_top[i] = xds.attrs["numphotons_canopy_top"]
                numphotons_bathy_floor[i] = xds.attrs["numphotons_bathy_floor"]
                numphotons_bathy_surface[i] = xds.attrs["numphotons_bathy_surface"]
                numphotons_buildings[i] = xds.attrs["numphotons_buildings"]
                downloaded_on[i] = xds.attrs["downloaded_on"]

                geometries[i] = shapely.box(data_bbox[0], data_bbox[2], data_bbox[1], data_bbox[3])

            db_dict = {"granule_id": granule_ids,
                       "filename": filenames,
                       "laser_name": lasers,
                       "query_bbox": query_bboxes,
                       "data_bbox": data_bboxes,
                       "zbounds": zbounds,
                       "numphotons": numphotons,
                       "numphotons_unclassified": numphotons_unclassified,
                       "numphotons_noise": numphotons_noise,
                       "numphotons_ground": numphotons_ground,
                       "numphotons_canopy": numphotons_canopy,
                       "numphotons_canopy_top": numphotons_canopy_top,
                       "numphotons_bathy_floor": numphotons_bathy_floor,
                       "numphotons_bathy_surface": numphotons_bathy_surface,
                       "numphotons_buildings": numphotons_buildings,
                       "downloaded_on": downloaded_on,
                       "geometry": geometries,
                       }

            gdf = geopandas.GeoDataFrame(db_dict, crs=self.crs, geometry="geometry")

        else: ## !populate, just create a blank one from scratch
            # Create an empty database and write it here. Don't bother writing the compressed version.
            db_dict = {"granule_id": ["foobar"],
                       "filename": ["foobar"],
                       "laser_name": ["gtl1"],
                       "query_bbox": [[0.0,0.0,0.0,0.0,0,0]],
                       "data_bbox": [[0.0,0.0,0.0,0.0,0,0]],
                       "zbounds": [[0.0, 0.0]],
                       "numphotons": [0],
                       "numphotons_unclassified": [0],
                       "numphotons_noise": [0],
                       "numphotons_ground": [0],
                       "numphotons_canopy": [0],
                       "numphotons_canopy_top": [0],
                       "numphotons_bathy_floor": [0],
                       "numphotons_bathy_surface": [0],
                       "numphotons_buildings": [0],
                       "downloaded_on": [0],
                       "geometry": shapely.box(0.0,0.0,1.0,1.0),
                      }
            gdf = geopandas.GeoDataFrame(db_dict, crs=self.crs, geometry="geometry").drop(labels=0, axis="rows")

        gdf.to_file(self.db_fname, driver="GPKG")
        if os.path.exists(self.db_fname):
            print("Created", os.path.basename(self.db_fname), "with", len(gdf), "records.")
        else:
            raise OSError("Failed to create", os.path.basename(self.db_fname))

        if len(gdf) > 0:
            utils.pickle_blosc.write(gdf, self.db_fname_compressed)
            if os.path.exists(self.db_fname_compressed):
                print("Created compressed", os.path.basename(self.db_fname_compressed), "with", len(gdf), "records.")
            else:
                print("Failed to create compressed", os.path.basename(self.db_fname_compressed) + ".")

        # This becomes the new database for this object.
        self.gdf = gdf

        return gdf

    def open_gdf(self,
                 read_compressed: typing.Union[str, bool] = "only_if_newer",
                 force_reread: bool = False,
                 verbose: bool = True) -> typing.Union[geopandas.GeoDataFrame, None]:
        """Get a GeoDataFrame from the database.

        Parameters
        ----------
        force_reread : bool
            If True, read the file again even if we've already read the database into memory.
        read_compressed: bool or string
            If True, read the compressed version of the databse if it exists.
            If False, read the uncompressed version of the database.
            If "only_if_newer", read the compressed version of the database if it exists and is newer than the uncompressed version.
        verbose: bool
            Output text if the file was read.

        Returns
        -------
        geopandas.GeoDataFrame containing the photon tiles from the database that fix the bounding box and date range.
            None if no current database file exists locally.
        """
        if self.gdf is not None and not force_reread:
            return self.gdf

        if read_compressed == "only_if_newer":
            if os.path.exists(self.db_fname_compressed) and os.path.exists(self.db_fname):
                if os.path.getmtime(self.db_fname_compressed) > os.path.getmtime(self.db_fname):
                    read_compressed = True
        elif read_compressed:
            read_compressed = os.path.exists(self.db_fname_compressed)

        if read_compressed:
            self.gdf = utils.pickle_blosc.read(self.db_fname_compressed)
            if verbose:
                print("Loaded", os.path.basename(self.db_fname_compressed), "with", len(self.gdf), "records.")
        else:
            if not os.path.exists(self.db_fname):
                raise FileNotFoundError("Database file", self.db_fname, "does not exist. Call 'create_new_database()' to create it.")

            self.gdf = geopandas.read_file(self.db_fname, driver="GPKG")
            if verbose:
                print("Loaded", os.path.basename(self.db_fname), "with", len(self.gdf), "records.")

        return self.gdf

    def read_database_file(self,
                           bbox: typing.Union[list, tuple, None] = None,
                           date_range: typing.Union[list, tuple, None] = None):
        """Read the master database into a GeoDataFrame.

        Subset list of granules by bounding box and date range of the data (not the query box).

        Return the subset of the database read off of disk."""
        if os.path.exists(self.db_fname):
            gdf_subset = geopandas.read_file(self.db_fname, bbox=bbox)

            if date_range is not None:
                date_range = self.convert_date_range(date_range)
                gdf_subset = gdf_subset[(gdf_subset["start_date_YYYYMMDD"] >= date_range[0])
                                        & (gdf_subset["end_date_YYYYMMDD"] <= date_range[1])] # TODO: Look at this again

            self.last_gdf_date_range = date_range
            self.last_gdf_bbox = tuple(bbox)


            return gdf_subset

        else:
            return None

    @staticmethod
    def omit_photons_from_exclusion_bbox(dataframe, bbox_to_exclude) -> pandas.DataFrame:
        """Exclude any photons that fall within the particular bounding box."""
        x = dataframe["x"]
        y = dataframe["y"]
        dt = dataframe["delta_time"]

        if len(bbox_to_exclude) == 6:
            bbox_dt_min = icesat2_query.yyyymmdd_to_delta_time(bbox_to_exclude[4])
            bbox_dt_max = icesat2_query.yyyymmdd_to_delta_time(bbox_to_exclude[5])
            df_sub = dataframe[(x < bbox_to_exclude[0]) |
                               (x >= bbox_to_exclude[1]) |
                               (y < bbox_to_exclude[2]) |
                               (y >= bbox_to_exclude[3]) |
                               (dt < bbox_dt_min) |
                               (dt >= bbox_dt_max) ]

        elif len(bbox_to_exclude) == 4:
            df_sub = dataframe[(x < bbox_to_exclude[0]) |
                               (x >= bbox_to_exclude[1]) |
                               (y < bbox_to_exclude[2]) |
                               (y >= bbox_to_exclude[3]) ]

        else:
            raise ValueError("Bounding boxes must be either 4 values or 6 values, in format (xmin, xmax, ymin, ymax, [tmin, tmax]).")

        return df_sub

    @staticmethod
    def read_granule(granule_fn,
                     subset_bbox: typing.Union[list, tuple, None] = None,
                     photon_classes: typing.Union[list, tuple, None] = None)\
            -> pandas.DataFrame:
        """Read a granule from the database.

        Filter if a bbox and/or photon classes are provided."""
        if subset_bbox is not None:
            assert len(subset_bbox) == 6, "subset_bbox must be a list or tuple of length 6 (xmin, ymin, xmax, ymax, tmin, tmax)."
            subset_bbox = tuple(subset_bbox)

        granule_ds = xarray.open_dataset(granule_fn)
        # print(granule_ds)
        # print(len(granule_ds["index"]))
        # TODO: Do this purely in xarray, forget converting to a dataframe. But I know how to do it in a dataframe right now.
        granule_df = granule_ds.to_dataframe()

        # Subset the granule dataframe within an (xmin, xmax, ymin, ymax, tmin, tmax) bounding box.
        # For now make base assumptions that all variables are provided (will loosen this later)
        x = granule_df["x"]
        y = granule_df["y"]
        dt = granule_df["delta_time"]
        class_code = granule_df["class_code"]

        # Convert the bounding box YYYYMMDD values to delta_times.
        bbox_dt_min = icesat2_query.yyyymmdd_to_delta_time(subset_bbox[4])
        bbox_dt_max = icesat2_query.yyyymmdd_to_delta_time(subset_bbox[5])

        # If no photon classes are specified, choose ground (1) and bathy_floor (40) photons only, by default.
        if photon_classes is None:
            photon_classes = (1,40)

        granule_df_sub = granule_df[(x >= subset_bbox[0]) &
                                    (x < subset_bbox[1]) &
                                    (y >= subset_bbox[2]) &
                                    (y < subset_bbox[3]) &
                                    (dt >= bbox_dt_min) &
                                    (dt < bbox_dt_max) &
                                    (class_code.isin(photon_classes))]

        # print(f"before ({numpy.count_nonzero(granule_df["class_code" == 1]):,} gr, "
        #       f"{numpy.count_nonzero(granule_df["class_code" == 40]):,} ba), "
        #       f"after ({numpy.count_nonzero(granule_df_sub["class_code" == 1]):,} gr, "
        #       f"{numpy.count_nonzero(granule_df_sub["class_code" == 40]):,} ba)")

        # print(len(granule_ds["index"]), "to", len(granule_df_sub), "photons.")

        return granule_df_sub


    @staticmethod
    def is_iterable(obj) -> bool:
        try:
            some_iterator = iter(obj)
            return True
        except TypeError:
            return False


    def query_photons(self,
                      bbox: typing.Union[list, tuple, None] = None,
                      photon_classes: typing.Union[list, tuple, None] = (1,40),
                      min_bathy_confidence = 0.75,
                      omit_bboxes = [],
                      # download_new_data: bool = False,
                      ) \
            -> typing.Union[pandas.DataFrame, None]:
        """Query the database for photons in a given bounding box and date range.

        Parameters
        ----------
            bbox : list, tuple, or None
                Bounding box to limit the data to, in [xmin, ymin, xmax, ymax, tmin, tmax]. Must be in WGS84 (EPSG: 4326)
                coordinates, and yyyymmdd integers for the date. Date range is not inclusive of the max date.
            photon_classes : list, tuple, or None
                Photon classes to include in the query. Type "dlim --modules icesat2" to see all options. Defaults to
                (1,40) (ground and bathy_floor photons only).
            min_bathy_confidence : float
                The minimum confidence for bathymetric points to use.
            # download_new_data : bool
            #     Whether to download new ICESat-2 data from NASA if the current database doesn't contain the entire bounding box.

        Returns
        -------
            pandas.DataFrame containing classified photons that fit in the bounding box and date range.
            If no photons are found, return None.
        """

        assert len(bbox) == 6, "bbox must be a list or tuple of length 6 (xmin, ymin, xmax, ymax, tmin, tmax)."

        gdf_subset = self.query_granules(bbox)

        print(f"Reading {len(gdf_subset)} granules overlapping {repr(bbox)}.")

        # print(gdf_subset)
        fnames = gdf_subset["filename"].apply(lambda x: os.path.join(self.granules_dir, x))
        print(numpy.count_nonzero(fnames.apply(os.path.exists)), "granules exist with",
              f"{gdf_subset["numphotons_ground"].sum():,}", "ground photons and",
              f"{gdf_subset["numphotons_bathy_floor"].sum():,}", "bathy_floor photons.")


        granule_dfs = []
        for idx, granule_line in gdf_subset.iterrows():
            fpath = os.path.join(self.granules_dir, granule_line["filename"])
            # print(os.path.basename(fpath))
            granule_dfs.append(self.read_granule(fpath, subset_bbox=bbox, photon_classes=photon_classes))
            # print()

        if len(granule_dfs) == 0:
            return None

        photons_df = pandas.concat(granule_dfs, ignore_index=True)

        if min_bathy_confidence > 0.0:
            photons_df = photons_df[(photons_df['class_code'] != 40) |
                                    ((photons_df['class_code'] == 40) & (photons_df['bathy_confidence'] >= min_bathy_confidence))]

        # If we're given a single bounding box of exclusions as a 4- or 6-tuple of numbers (not iterables), put it in a 1-length list.
        if len(omit_bboxes) in (4,6) and not numpy.any([self.is_iterable(num) for num in omit_bboxes]):
            omit_bboxes = [omit_bboxes]

        if len(omit_bboxes) >= 1:
            for omit_bb in omit_bboxes:
                photons_df = self.omit_photons_from_exclusion_bbox(photons_df, omit_bb)

        if len(photons_df) > 0:
            print(f"Trimmed granules from {gdf_subset["numphotons"].sum():,} to {len(photons_df):,} photons "
                  f"({numpy.count_nonzero(photons_df["class_code"] == 1):,} ground, "
                  f"{numpy.count_nonzero(photons_df["class_code"] == 40):,} bathy).\n")
        else:
            print("No photons in bbox.\n")

        # all of this subsetting can create a fractured dataframe that is a subset-of-subset-of... iteration.
        # If we simply copy the dataframe upon returning it will be cleaner, without pointing to larger datasets and masks.
        return photons_df.copy()


    def convert_date_range(self,
                           date_range: typing.Union[list, tuple, None]) -> typing.Union[list, tuple, None]:
        """Convert date range to the format required by the database."""
        if date_range is None:
            return None
        elif len(date_range) == 2:
            return self.convert_date_to_yyyymmdd(date_range[0]), self.convert_date_to_yyyymmdd(date_range[1])
        else:
            raise ValueError("Date range must be a list or tuple of length 2.")


    def convert_date_to_yyyymmdd(self, date: typing.Union[int, str, datetime.datetime, datetime.date]) -> int:
        """Convert date to the YYYYMMDD integer format required by the database."""
        if isinstance(date, int):
            # If it's an integer, make sure it's 8 digits and then return as-is.
            if len(str(date)) != 8:
                raise ValueError("Date must be an 8 digit integer in YYYYMMDD.")
            return date
        elif isinstance(date, str):
            try:
                # If it's a string in "YYYYMMDD" format, convert it to an int.
                date_int = int(date)
                return self.convert_date_to_yyyymmdd(date_int)
            except ValueError:
                # If it isn't a YYYYMMDD string, parse it with dateparser.
                return int(dateparser.parse(date).strftime("%Y%m%d"))
        elif isinstance(date, datetime.datetime) or isinstance(date, datetime.date):
            return int(date.strftime("%Y%m%d"))
        else:
            raise ValueError("Date must be an int, str, datetime.datetime, or datetime.date.")


    def query_granules(self,
                       bbox: typing.Union[list, tuple]) \
            -> typing.Union[pandas.DataFrame, None]:
        """Return a sub-dataframe of granules in the database that possibly intersect the bounding box, using data bounding boxes."""
        gdf = self.open_gdf()
        if gdf is None or len(gdf) == 0:
            return None

        # To assess intersection, we must first increment the tmin of both the incoming bboxes and the query bbox by 1
        # to make it a non-inclusive limit.
        # query_bbox = tuple(bbox[0:5]) + (self.increment_yyyymmdd_by_n(bbox[5], 1),)

        bbox = (float(bbox[0]), float(bbox[1]), float(bbox[2]), float(bbox[3]), int(bbox[4]), int(bbox[5]))

        # Add 1 to each
        int_mask = gdf["data_bbox"].apply(
            lambda b: utils.cuboid_funcs.cuboids_intersect(b,
                                                           bbox,
                                                           bbox_order="axis"))

        # Return the subset of the dataframe of granules whose data bounding-box intersects the query bounding box.
        # return gdf[intersect_func(data_bboxes)]
        return gdf[int_mask]


    def download_new_granules(self,
                              bbox: typing.Union[list, tuple],
                              classes_to_keep=(1, 2, 3, 7, 40, 41),
                              split_big_bboxes: bool = True,
                              tile_size_deg=2.0,
                              max_tile_scale_factor=1.5,
                              min_bathy_confidence=0.01,
                              cache_subdir: typing.Union[str, None] = None):
        """Download ICESat-2 granules from NASA to the granules directory, only for bboxes not already in the database,
        and enter the new files into the database."""
        # Filter out any areas of the given bbox that have already been filled by other queries. This returns a list of
        # zero or more boxes.

        # TODO: Un-comment this out once we've finished the CRM_vol8 code
        # bboxes = self.filter_query_bbox(bbox)
        bboxes = [bbox]

        if len(bboxes) == 0:
            print("All required granules already exist in the database. Nothing new to download.")
            return

        # If the bounding box is greater than 3 degrees on any given size, split it up.
        if split_big_bboxes:
            bboxes_split = []
            for bb in bboxes:
                bboxes_split.extend(split_bbox_into_parts(bb,
                                                          tile_size_deg=tile_size_deg,
                                                          max_tile_scale_factor=max_tile_scale_factor)
                                    )

            bboxes = bboxes_split

        print(f"Downloading granules over {bbox} in {len(bboxes)} parts.")

        for i, sbbox in enumerate(bboxes):
            print("=====================================================================================")
            print(f"""Part {i+1} of {len(bboxes)}: {sbbox}""")
            print("=====================================================================================")

            # Clear out the cache with previous icesat-2 downloads. They can cause conflicts.
            self.delete_cache(delete_cudem_cache=True,
                              delete_already_processed_txt=False, #delete_already_processed_txt=True,
                              delete_cmr=False,
                              cache_subdir=cache_subdir)

            cache_dir = os.path.join(self.icesat2_download_dir, cache_subdir) if cache_subdir is not None else self.icesat2_download_dir
            if not os.path.exists(cache_dir):
                print("Creating cache directory", cache_dir)
                os.makedirs(cache_dir)

            new_granule_meta_df = \
                icesat2_query.download_granules(sbbox,
                                                self.granules_dir,
                                                cache_dir,
                                                classifications_to_keep = classes_to_keep,
                                                other_columns = {}, # {"/gtx/heights/delta_time": "delta_time"},
                                                classify_water=True,
                                                classify_buildings=True,
                                                min_bathy_confidence=min_bathy_confidence, # Keep all the bathy_floor data with any confidence at all, we'll screen it out later.
                                                trim_to_bbox=True,
                                                overwrite=False)

            # From the dataframe returned, convert the data_bbox into a 2D geometry bounding-box polygon.
            def _get_poly_from_bbox(bb):
                return shapely.box(float(bb[0]), float(bb[1]), float(bb[2]), float(bb[3]))

            if new_granule_meta_df is None or len(new_granule_meta_df) == 0:
                # TODO: Place an "EMPTY" marker in the database with the correct query_bbox
                #  (and data_bbox outside any reasonable search area) to keep IVERT from ever trying to rebuild that
                #  section of database.
                continue

            new_granule_meta_df["geometry"] = new_granule_meta_df["data_bbox"].apply(_get_poly_from_bbox)

            # Convert it into a geodataframe.
            new_granule_gdf = geopandas.GeoDataFrame(new_granule_meta_df, crs=self.crs, geometry="geometry")

            # Read whatever the existing dataframe is.
            existing_gdf = self.open_gdf(read_compressed=True)

            # Combine the new entries with the existing dataframe.
            self.gdf = geopandas.GeoDataFrame(pandas.concat([existing_gdf, new_granule_gdf], ignore_index=True), crs=self.crs, geometry="geometry")

            self.gdf.to_file(self.db_fname, driver="GPKG")
            if os.path.exists(self.db_fname):
                print("Updated", os.path.basename(self.db_fname), "with", len(self.gdf), "records.")
            else:
                if os.path.exists(self.db_fname_compressed):
                    os.remove(self.db_fname_compressed)

                raise OSError("Failed to create", os.path.basename(self.db_fname))

            if os.path.exists(self.db_fname_compressed):
                os.remove(self.db_fname_compressed)
            if len(self.gdf) > 0:
                utils.pickle_blosc.write(self.gdf, self.db_fname_compressed)
                if os.path.exists(self.db_fname_compressed):
                    print("Updated compressed", os.path.basename(self.db_fname_compressed), "with", len(self.gdf), "records.")

        self.delete_cache()


    def bounds(self,
               axis: str,
               data_or_query: str = "data") -> typing.Union[tuple, None]:
        """Return the min, max bounds of each entry in the database, on the axis requested ('x', 'y', or 't').
        
        Parameters
        ----------
        axis : str
            The axis to get bounds for. Must be one of 'x', 'y', or 't'.
        data_or_query : str, optional
            Whether to use the data bounds or query-box bounds, by default "data".
            Must be one of "data" or "query".

        Raises
        ------
        ValueError if parameters are invalid.

        Returns
        -------
        list or None
            A 2-tuple containing (min, max) values for the requested axis.
            Returns None if no data is available or if invalid parameters are provided.
        """
        gdf = self.open_gdf()
        if gdf is None or len(gdf) == 0:
            return None

        data_or_query = data_or_query.lower().strip()
        if data_or_query == "data":
            bboxes = gdf["data_bbox"]
        elif data_or_query == "query":
            bboxes = gdf["query_bbox"]
        else:
            raise ValueError("Invalid data_or_query parameter. Must be one of 'data' or 'query'.")

        axis = axis.lower().strip()
        if axis == "x":
            mins = bboxes.apply(lambda x: x[0])
            maxs = bboxes.apply(lambda x: x[1])
        elif axis == "y":
            mins = bboxes.apply(lambda x: x[2])
            maxs = bboxes.appyl(lambda x: x[3])
        elif axis == "t":
            mins = bboxes.apply(lambda x: x[4]).astype(int)
            maxs = bboxes.apply(lambda x: x[5]).astype(int)
        else:
            raise ValueError("Invalid axis parameter. Must be one of 'x', 'y', or 't'.")

        return mins, maxs


    def unique_bboxes(self,
                      gdf: typing.Union[geopandas.GeoDataFrame, None] = None,
                      data_or_query: str = "query") -> typing.Union[list, None]:
        """Return a numpy array of unique query bounding boxes in the database.

        This is useful to see what query bounding-boxes have already been populated in the database.
        
        Parameters
        ----------
        data_or_query : str, optional
            Whether to use the data bounds or query-box bounds, by default "query".
            Must be one of "data" or "query".

        Raises
        ------
        ValueError
            If data_or_query parameter is invalid or not one of 'data' or 'query'.

        Returns
        -------
        list or None
            List of unique bounding boxes from the database, where each box is a 6-tuple
            containing (xmin, xmax, ymin, ymax, tmin, tmax).
            Returns None if no data is available in the database.
        """
        if gdf is None:
            gdf = self.open_gdf()
        if gdf is None or len(gdf) == 0:
            return None

        data_or_query = data_or_query.lower().strip()
        if data_or_query == "data":
            field = "data_bbox"
        elif data_or_query == "query":
            field = "query_bbox"
        else:
            raise ValueError("Invalid data_or_query parameter. Must be one of 'data' or 'query'.")

        # Create a tuple from each bbox, make a set of those (unique values), turn back into a list and a numpy array
        bboxes = sorted(list(set([tuple(x) for x in gdf[field]])))
        # For each of the bboxes, turn the last two numbers (tmin, tmax) into integers.
        for i, bb in enumerate(bboxes):
            bboxes[i] = bb[:-2] + (int(bb[-2]), int(bb[-1]))

        # Return it as a list of bbox tuples.
        return bboxes

    def delete_cache(self,
                     delete_everything: bool = False,
                     delete_cmr: bool = True,
                     delete_already_processed_txt: bool = True,
                     delete_cudem_cache: bool = True,
                     cache_subdir: typing.Union[str, None] = None) -> None:
        """Delete the icesat-2 data downloads and clears the cache directory."""
        # If we only want to get rid of previous ICESat-2 downloads, clearing the CMR sub-directory will do that.
        if cache_subdir is None:
            cache_dir = self.icesat2_download_dir
        else:
            cache_dir = os.path.join(self.icesat2_download_dir, cache_subdir)

        if delete_everything and os.path.exists(cache_dir):
            for fname in [os.path.join(cache_dir, fn) for fn in os.listdir(cache_dir)]:
                shutil.rmtree(fname)

        else:

            if delete_cmr:
                if os.path.exists(os.path.join(cache_dir, "cmr")):
                    shutil.rmtree(os.path.join(cache_dir, "cmr"))
                if os.path.exists(os.path.join(cache_dir, ".cudem_cache", "cmr")):
                    shutil.rmtree(os.path.join(cache_dir, ".cudem_cache", "cmr"))

            if delete_already_processed_txt and os.path.exists(os.path.join(cache_dir, "already_processed.txt")):
                    os.remove(os.path.join(cache_dir, "already_processed.txt"))

            if delete_cudem_cache and os.path.exists(os.path.join(cache_dir, ".cudem_cache")):
                shutil.rmtree(os.path.join(cache_dir, ".cudem_cache"))


    @staticmethod
    def bbox_valid(bbox: typing.Union[list, tuple]) -> bool:
        """Validate a bounding box. Make sure all min-max values are correctly ordered.

        Parameters:
        -----------
        bbox : list or tuple
            A 6-item bounding box in [xmin, xmax, ymin, ymax, tmin, tmax] format where t is YYYYMMDD.
            In each case, the following cases must be true:
                xmin < mmax
                ymin < ymax
                tmin <= tmax

        Returns:
        --------
            Boolean, True if all conditions are met, False if not.
        """
        xmin, xmax, ymin, ymax, tmin, tmax = bbox
        return (xmin < xmax) and (ymin < ymax) and (tmin <= tmax)


    def filter_query_bbox(self,
                          query_bbox: typing.Union[list, tuple]) -> list[tuple]:
        """Given an (x,y,t) ICESat-2 bounding box, remove existing regions and return bboxes for the rest of the data.
        
        Parameters
        ----------
        query_bbox : list or tuple
            The input bounding box to filter, in [xmin, xmax, ymin, ymax, tmin, tmax] format where t is YYYYMMDD.

        Raises
        ------
        ValueError
            If query_bbox is not in the correct format or contains invalid values.

        Returns
        -------
        List of bounding boxes that represent areas not already in the database,
            where each box is a 6-tuple containing (xmin, xmax, ymin, ymax, tmin, tmax).
            tmin and tmax are in YYYYMMDD format and are inclusive.
            Returns an empty list if the entire query_bbox is already present in the database.
        """
        if not self.bbox_valid(query_bbox):
            raise ValueError("query_bbox must be a non-zero-volume valid 6-tuple or 6-value bbox, with values in the correct order.")

        # First, get a list of the active unique query cuboids within the current database
        existing_bboxes = self.unique_bboxes(data_or_query="query")
        if existing_bboxes is None or len(existing_bboxes) == 0:
            return [query_bbox]

        # For the purpose of merging, increase the tmaxes by 1 day to make all boxes non-inclusive
        # (This makes adjoining bounding-boxes actually border each other in coordinate space rather than be 1 day apart)
        # e_bboxes = [tuple(bb[:5]) + (self.increment_yyyymmdd_by_n(bb[5], 1),) for bb in existing_bboxes]

        # Simplify by merging these bboxes together (could have been gathered on a number of queries).
        e_bboxes = utils.cuboid_funcs.merge_cuboids(existing_bboxes, bbox_order="axis")

        # Now, increment the query_box tmax by 1 to make it non-inclusive as well (for cuboid subtraction)
        # query_bbox = tuple(query_bbox[:5]) + (self.increment_yyyymmdd_by_n(query_bbox[5], 1),)

        # Now do a cuboid subtraction of query_bbox by all the e_bboxes:
        query_bboxes = [query_bbox]
        for e_bbox in e_bboxes:
            new_bboxes = []
            for q_bbox in query_bboxes:
                new_bboxes.extend(utils.cuboid_funcs.subtract_cuboids(q_bbox, e_bbox, bbox_order="axis"))

            query_bboxes = new_bboxes

        # Do a quick merger on all the remaining bboxes to make sure they're simplified
        query_bboxes = utils.cuboid_funcs.merge_cuboids(query_bboxes, bbox_order="axis")

        # Now, decrement the tmax day by 1 to make the ranges inclusive again.
        # query_bboxes = [tuple(bb[:5]) + (self.increment_yyyymmdd_by_n(bb[5], -1),) for bb in query_bboxes]

        return query_bboxes


    @staticmethod
    def increment_yyyymmdd_by_n(yyyymmdd: typing.Union[int, float, str],
                                days: int) -> int:
        """Increment a YYYYMMDD integer by N calendar days (positive or negative).."""
        ymd = int(yyyymmdd)
        ymd_dt = datetime.datetime.strptime(str(ymd), "%Y%m%d") + datetime.timedelta(days=int(days))
        return int(ymd_dt.strftime("%Y%m%d"))


def split_bbox_into_parts(bbox: typing.Union[list, tuple],
                          tile_size_deg: float = 2.0,
                          max_tile_scale_factor: float = 1.5) -> typing.Union[list, None]:
    """Split a bounding box into parts of size approximately deg_size degrees.."""
    # if we included a 6-value bbox, save the last two and append them at the end.
    tmin, tmax = None, None
    if len(bbox) == 6:
        tmin, tmax = bbox[4], bbox[5]
        bbox = bbox[:4]
    assert len(bbox) == 4, "bbox must be a 4-tuple or 6-tuple."

    xmin, xmax, ymin, ymax = bbox
    max_deg_size = tile_size_deg * max_tile_scale_factor

    xbins = numpy.arange(xmin, xmax, tile_size_deg)
    ybins = numpy.arange(ymin, ymax, tile_size_deg)

    if xbins[-1] < xmax:
        if len(xbins) == 1 or ((xmax - xbins[-2]) > max_deg_size):
            xbins = numpy.append(xbins, xmax)
        else:
            xbins[-1] = xmax

    if ybins[-1] < ymax:
        if len(ybins) == 1 or ((ymax - ybins[-2]) > max_deg_size):
            ybins = numpy.append(ybins, ymax)
        else:
            ybins[-1] = ymax

    binxs, binys = numpy.meshgrid(xbins, ybins)
    bin_xmins = binxs[:-1, :-1].flatten()
    bin_xmaxs = binxs[1:, 1:].flatten()
    bin_ymins = binys[:-1, :-1].flatten()
    bin_ymaxs = binys[1:, 1:].flatten()

    if tmin is not None and tmax is not None:
        bboxes = [(float(xbmin), float(xbmax), float(ybmin), float(ybmax), tmin, tmax) for (xbmin, xbmax, ybmin, ybmax) in zip(bin_xmins, bin_xmaxs, bin_ymins, bin_ymaxs)]
    else:
        bboxes = [(float(xbmin), float(xbmax), float(ybmin), float(ybmax)) for (xbmin, xbmax, ybmin, ybmax) in zip(bin_xmins, bin_xmaxs, bin_ymins, bin_ymaxs)]

    return bboxes

if __name__ == "__main__":
    ivert_db = IS2Database()

    # gdf = ivert_db.open_gdf()
    #
    # print(gdf["geometry"].iloc[0])
    #
    # print(gdf)
    # print(f"{gdf["numphotons"].sum():,}", "total photons.")
    # print(f"{gdf["numphotons_ground"].sum():,}", "total ground photons.")
    # print(f"{gdf["numphotons_bathy_floor"].sum():,}", "total bathy floor photons.")
    # print(f"{gdf["numphotons_bathy_surface"].sum():,}", "total bathy surface photons.")

    # print(ivert_db.bounds("t"))
    gdf = ivert_db.create_new_database(populate=True, overwrite=True)
    # print(gdf)

    # Create a new database and populate it over the CRM vol8 extent.
    # ivert_db.create_new_database(populate=True, overwrite=True)
    # print(ivert_db.unique_bboxes(data_or_query="query"))

    # Get a set of CUDEM tiles to validate.
    # Mike's tiles (N Oregon)
    # dem_dir = "/home/mmacferrin/Research/DEMs/CUDEMs_1_9_Oregon_2025/dems/2025.10.10_w_metadata"
    # dems = [os.path.join(dem_dir, fn) for fn in os.listdir(dem_dir) if fn.endswith("2025v1.tif")]
    # Chris' tiles (S Oregon)
    # dem_dir = "/home/mmacferrin/Research/DEMs/CUDEMs_1_9_Oregon_2025/chris/9-29-2025"
    # dems = [os.path.join(dem_dir, fn) for fn in os.listdir(dem_dir) if fn.endswith(".tif")]
    # Matt's tiles (N WA)
    # dem_dir = "/home/mmacferrin/Research/DEMs/CUDEMs_1_9_Oregon_2025/matt/9-26-2025/DEM"
    # dems = [os.path.join(dem_dir, fn) for fn in os.listdir(dem_dir) if fn.endswith("2025v1.tif")]

    # print(len(dems), "total DEMs.")

    # gdf = ivert_db.open_gdf()
    # print(gdf)
    # print(gdf.columns)
    # Get the x,y bounding box of each DEM file
    # def get_dem_bbox(fn):
    #     with rasterio.open(fn) as r:
    #         # print(os.path.basename(fn), r.bounds)
    #         return (r.bounds[0], r.bounds[2], r.bounds[1], r.bounds[3], 20240301, 20250301)
    #
    # bboxes = [get_dem_bbox(fn) for fn in dems]
    # for dem, bbox in zip(dems, bboxes):
    #     print(dem, bbox)
    #     ivert_db.query_photons(bbox)

    # NOTE: The best CUDEM is:
    # Mike's DEMs: ncei19_n45x50_w124x00_2025v1.tif (104,757 ground, 3,974 bathy)
    # Matt's DEMs: ncei19_n48x25_w125x00_2025v1.tif (63,737 ground, 2,090 bathy)
    # Chris' DEMs: B3.tif (74,662 ground, 1,651 bathy)
    #              C2.tif (6,111 ground, 3,177 bathy)
    #              B2.tif (40,634 ground, 2,145 bathy)

    # Small test area on the Oregon coast.
    # ivert_db.download_new_granules(bbox=(-124.15, -123.85, 45.0, 45.25, 2024_07_20, 2024_08_10)) # Smaller test area
    # CRM, volume 8 data (all)

    import sys
    do_i = int(sys.argv[1])

    # CRM Vol8 total area, split into 9 parts.
    bbox = (-127.01, -121.74, 43.99, 49.01, 2024_03_01, 2025_03_01)
    # bbox = (-127.01, -121.74, 43.99, 49.01, 2024_03_15, 2024_03_26) # Smaller date window for testing.
    bboxes = split_bbox_into_parts(bbox, tile_size_deg=2.0, max_tile_scale_factor=1.5)
    # Order bboxes to go from right to left (descending longitude), to process land first.
    bboxes = sorted(bboxes, key=lambda x: x[0], reverse=True)

    # Test area over B.C. to flesh out coastline masking.
    # bbox = (-122.93, -122.79, 48.68, 49.0, 2024_03_15, 2024_03_26)
    # bboxes = [bbox]
    # do_i = 0


    # Smaller test area, partially bordering both Chris' and my CUDEM tiles.
    # bbox = (-124.5, -123.75, 44.75, 45.25, 20241201, 20250301)
    # bboxes = [bbox]
    # do_i = 1

    for i, smaller_bbox in enumerate(bboxes):
        if do_i > 0 and i != (do_i - 1):
            continue

        # if do_i in []: # (2,3,5,6,9):
        #     # For some stupid fucking reason, subset #6 isn't working, just hangs on processing coastline.
        #     # Try splitting it up into several smaller sections.
        #     smaller_boxes = split_bbox_into_parts(smaller_bbox, tile_size_deg=1.0, max_tile_scale_factor=1.5)
        #
        #     for j, even_smaller_bbox in enumerate(smaller_boxes):
        #         print()
        #         print(f"=================== {i + 1} of {len(bboxes)} ({j + 1} of {len(smaller_boxes)}) ====================")
        #         # clear_cache_cmd = ["rm", "-rf", os.path.expanduser("~/.ivert/cache/*")]
        #         # print(" ".join(clear_cache_cmd))
        #         # subprocess.run(clear_cache_cmd)
        #         ivert_db.download_new_granules(bbox=even_smaller_bbox,
        #                                        split_big_bboxes=False,
        #                                        subset_granules=True,
        #                                        classes_to_keep=(1, 40),
        #                                        tile_size_deg=1.0,
        #                                        cache_subdir=f"{do_i}{j+1}",
        #                                        max_tile_scale_factor=1.5)
        #
        # else:

        print()
        print(f"=================== {i+1} of {len(bboxes)} ===================")
        # clear_cache_cmd = ["rm", "-rf", os.path.expanduser("~/.ivert/cache/*")]
        # print(" ".join(clear_cache_cmd))
        # subprocess.run(clear_cache_cmd)
        # ivert_db.download_new_granules(bbox=(-127.0, -121.75, 43.9, 49.1, 20240301, 20250301),
        ivert_db.download_new_granules(bbox=smaller_bbox,
                                       split_big_bboxes=False,
                                       classes_to_keep=(1, 40),
                                       tile_size_deg=2.0,
                                       cache_subdir=str(do_i),
                                       min_bathy_confidence=0.9,
                                       max_tile_scale_factor=1.5,
                                       )
