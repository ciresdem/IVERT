# Standar python libraries
import pandas
import geopandas
import os
import multiprocessing as mp
# My scripts
import icesat2_photon_database
import utils.configfile

ivert_config = utils.configfile.config()

class validation_pool_child:
    def __init__(self,
                 connection,
                 dem_epsg=None,
                 dem_gt=None,
                 tempdir=None,
                 tile_gpkg=ivert_config.icesat2_photon_geopackage):
        self.tile_dbname = tile_gpkg
        self.connection = connection
        self.dem_epsg = dem_epsg
        self.dem_gt = dem_gt
        self.tile_gdf = None
        self.tempdir = tempdir

        self.validation_loop()

    def get_tile_gdf(self):
        """Return the tile geodataframe. Read from disk if necessary."""
        if self.tile_gdf is None:
            ext = os.path.splitext(self.tile_dbname)[1].lower()
            if ext == ".gpkg":
                self.tile_gdf = geopandas.read_file(self.tile_dbname)
            elif ext == ".gz":
                self.tile_gdf = pandas.read_pickle(self.tile_dbname)

        return self.tile_gdf

    def validation_loop(self):
        """The main loop to keep processing input from the parent."""
        tile_gdf = get_tile_gdf()



    def validate_dem_child_process(self, input_heights, input_i, input_j, photon_codes, connection, photon_limit=None,
                                   measure_coverage=False, input_x=None, input_y=None, num_subdivisions=15):
        pass


class validation_pool_manager()