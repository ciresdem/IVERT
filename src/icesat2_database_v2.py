# Functionality for reading ICESat-2 data and saving it in a tiled database.

import datetime
import geopandas
import os
import sqlite3
import typing
import utils.configfile

class IS2Database:

    def __init__(self,
                 ivert_config: typing.Union[utils.configfile.Config, None] = None):
        # Define the structure of the object.
        if ivert_config is None:
            self.config = utils.configfile.Config()
        else:
            self.config = ivert_config

        self.db_fname = self.config.icesat2_photon_database
        self.gdf = None
        self.last_gdf_bbox = None
        self.last_gdf_date_range = None

        self.tile_dir = self.config.icesat2_photon_tiles_directory

    def create_database(self,
                        populate: bool = True,
                        overwrite: bool = False):
        """Create a new database from scratch.

        Parameters
        ----------
        populate : bool
            Whether to populate the database with the data from the tiles.
        overwrite : bool
            Whether to overwrite the database if it already exists.
        """
        # TODO: Implement.

    def get_gdf(self,
                force_read_again: bool = False,
                bbox: typing.Union[list, tuple, None] = None,
                date_range: typing.Union[list, tuple, None] = None):
        """Get a GeoDataFrame from the database.

        Parameters
        ----------
        force_read_again : bool
            Whether to force a re-reading of the database file.
        bbox : list, tuple, or None
            Bounding box to limit the data to, in [xmin, ymin, xmax, ymax]. Must be in WGS84 (EPSG: 4326) coordinates.
        date_range : list, tuple, or None
            Date range to limit the data to, in the format [start_date, end_date]. Can be datetime.datetime objects
            or 8 digit integers or strings in the format YYYYMMDD.

        Returns
        -------
        geopandas.GeoDataFrame containing the photon tiles from the database that fix the bounding box and date range.
        """
        if self.gdf is None or force_read_again or (tuple(bbox) != tuple(self.last_gdf_bbox)):
            self.gdf = self.read_database_file(bbox=bbox)

        return self.gdf

    def read_database_file(self,
                           bbox: typing.Union[list, tuple, None] = None,
                           date_range: typing.Union[list, tuple, None] = None):
        """Read the database into a GeoDataFrame."""
        if os.path.exists(self.db_fname):
            gdf_subset = geopandas.read_file(self.db_fname, bbox=bbox)

            if date_range is not None:
                date_range = self.convert_date_range(date_range)
                gdf_subset = gdf_subset[(gdf_subset["start_date_YYYYMMDD"] >= date_range[0])
                                        & (gdf_subset["end_date_YYYYMMDD"] <= date_range[1])]

            self.last_gdf_date_range = date_range
            self.last_gdf_bbox = tuple(bbox)

            return gdf_subset

    def query_photons(self,
                      bbox: typing.Union[list, tuple, None] = None,
                      date_range: typing.Union[list, tuple, None] = None,
                      photon_classes: typing.Union[list, tuple, None] = None)\
            -> typing.Union[geopandas.GeoDataFrame, None]:
        """Query the database for photons in a given bounding box and date range.

        Parameters
        ----------
            bbox : list, tuple, or None
                Bounding box to limit the data to, in [xmin, ymin, xmax, ymax]. Must be in WGS84 (EPSG: 4326) coordinates.
            date_range : list, tuple, or None
                Date range to limit the data to, in the format [start_date, end_date]. Can be datetime.datetime objects
                or 8 digit integers or strings in the format YYYYMMDD.
            photon_classes : list, tuple, or None
                Photon classes to limit the data to, 1-7. See (TODO: FILL IN) for the full list of classes.

        Returns
        -------
            geopandas.GeoDataFrame containing classified photons that fit in the bounding box and date range.
            If no photons are found, return None.
        """

    def convert_date_range(self,
                           date_range: typing.Union[list, tuple, None]) -> typing.Union[list, tuple, None]:
        """Convert date range to the format required by the database."""
        if date_range is None:
            return None
        elif len(date_range) == 2:
            return self.convert_date_to_YYYYMMDD(date_range[0]), self.convert_date_to_YYYYMMDD(date_range[1])
        else:
            raise ValueError("Date range must be a list or tuple of length 2.")

    def convert_date_to_YYYYMMDD(self, date: typing.Union[int, str, datetime.datetime]) -> int:
        """Convert date to the format required by the database."""