# Testing the read/write/execute efficiency of rtrees for the icesat2_photon_database.

import timeit
import os
import pandas
import geopandas
import shapely
import rtree

# Get the configfile for the project.
import utils.sizeof_format
import utils.configfile
ivert_config = utils.configfile.config()

# Pick a few representative feather files to run these tests on. Some big, some small.
# 412.9 MB
t1 = os.path.join(ivert_config.icesat2_photon_tiles_directory, "photon_tile_S88.00_W179.50_S87.75_W179.25.feather")
# 56.9 MB
t2 = os.path.join(ivert_config.icesat2_photon_tiles_directory, "photon_tile_N80.00_W098.75_N80.25_W098.50.feather")
# 8.4 MB
t3 = os.path.join(ivert_config.icesat2_photon_tiles_directory, "photon_tile_N45.00_W123.25_N45.25_W123.00.feather")
# 37.2 KB
t4 = os.path.join(ivert_config.icesat2_photon_tiles_directory, "photon_tile_N00.00_W079.25_N00.25_W079.00.feather")

print("Test databases:")
print(*[(f"  t{i + 1}: " + os.path.basename(tn) + " " + utils.sizeof_format.sizeof_fmt(os.path.getsize(tn))) for (i, tn) \
        in enumerate((t1, t2, t3, t4))],
      sep="\n")
# print([len(pandas.read_feather(tn)) for tn in (t1, t2, t3)])

# First, what's it take to convert it to a geodatabase? Does it read/write any faster/slower after initial implementation? How about the file sizes?

# 1) Convert files to geodatabases, save back in feather format using the same compression options as before.
# What's the size difference?
# How much longer does it take to load?

# 2) Put a spatial index on it.
# How long does that take (to create)? Is it worth just re-creating every time?
# Can we save the geopandas spatial index to disk?
# How long do searches take w/ the spatial index, rather than just the (i,j) searches as currently done? Which is faster?

# 3) If the above spatial indices don't work great:
# Can we use an rtree implementation on the database?
# What's the file size?
# How long does it take to load? To search?