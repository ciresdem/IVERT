import geopandas
import dask_geopandas
import time
import timeit
import shapely.geometry
import os

import utils.configfile

ivert_config = utils.configfile.config()

# Define a polygon, which should intersect exactly 1 tile outline in southern Antarctica
poly_square = shapely.geometry.Polygon(shell=[(0, -88), (0, -87.99), (0.01, -87.99), (0.01, -88), (0, -88)])
# print(poly_square.bounds)

photon_tile_gpkg = ivert_config.icesat2_photon_geopackage
dask_dir = os.path.join(os.path.dirname(photon_tile_gpkg), "photon_tiles_gdf_dask")

gdf = geopandas.read_file(photon_tile_gpkg)
print("1) gdf has_sindex:", gdf.has_sindex)
# print("1) ddf.has_sindex:", ddf.has_sindex)
# Create the spatial index on the file.
_ = gdf.sindex
print("2) gdf has_sindex:", gdf.has_sindex)
# print("2) ddf.has_sindex:", ddf.has_sindex)

# Create a Dask geodataframe, partitioned by geometry into 8 files.
ddf = dask_geopandas.from_geopandas(gdf, npartitions=8)
ddf = ddf.spatial_shuffle(by="geohash") # For these squares in lat/lon geometry, use geohash. See https://dask-geopandas.readthedocs.io/en/latest/guide/spatial-partitioning.html for more info.

# Test spatial queries.
def subset_example_gdf(gdf, poly):
    gdf_sub = gdf.loc[gdf.sindex.query(poly)]
    return gdf_sub[~gdf_sub.touches(poly)]

t1 = time.time()
sub_gdf = subset_example_gdf(gdf, poly_square).filename.tolist()
t2 = time.time()
print(sub_gdf, "{0:0.3f}s".format(t2 - t1))

def subset_example_ddf(ddf, poly):
    # bounds = poly.bounds
    ddf_sub = ddf.loc[ddf.intersects(poly)]
    return ddf_sub[~ddf_sub.touches(poly)].compute()

t1 = time.time()
sub_ddf = subset_example_ddf(ddf, poly_square)
t2 = time.time()
print(sub_ddf.filename.tolist(), "{0:0.3f}s".format(t2 - t1))

# print("GDF: {0:0.3}s per iteration".format(timeit.timeit(lambda: subset_example_gdf(gdf, poly_square), number=100) / 100.))
# print("DDF: {0:0.3}s per iteration".format(timeit.timeit(lambda: subset_example_ddf(ddf, poly_square), number=100) / 100.))
# Okay, the spatial r-tree indexing in Geopandas is way, way better. BUT, can dask help us with read/write times, and possible with saving the spatial index.
# Results:
# GDF: 0.003257s per iteration
# DDF: 2.558s per iteration

# Test read times, and sizes. And, can it save a spatial fucking index??
