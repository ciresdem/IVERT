import os
import rtree.index
import pickle
import pandas
import time
import sys
import gzip
import tarfile

import utils.sizeof_format
import utils.progress_bar

# idx_test_file = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "scratch_data", "rtree_save_test", "rtree_file"))
# idx_idx_file = idx_test_file + ".idx"
# if os.path.exists(idx_idx_file):
#     os.remove(idx_idx_file)
# idx_dat_file = idx_test_file + '.dat'
# if os.path.exists(idx_dat_file):
#     os.remove(idx_dat_file)
#
# props = rtree.index.Property()
# props.overwrite = True
# props.dat_extension = "dat"
# props.idx_extension = "idx"
#
# idx = rtree.index.Index(idx_test_file, properties=props)
# idx.insert(0, (1,2,3,4))
# idx.insert(1, (5,6))
# print(idx.get_bounds())
# idx.close()
# if os.path.exists(idx_test_file):
#     print(idx_test_file, "written.")
#
# idx2 = rtree.index.Index(idx_test_file)
# print(idx2.get_bounds())
#
# sys.exit()

# df_file = "/home/mmacferrin/Research/DATA/ETOPO/data/icesat2/photon_tiles/photon_tile_S67.00_E090.00_S66.75_E090.25.feather"
df_file = "/home/mmacferrin/Research/DATA/ETOPO/data/icesat2/photon_tiles/photon_tile_N00.00_E020.00_N00.25_E020.25.feather"
df = pandas.read_feather(df_file)
print(os.path.basename(df_file), "read with {0:,} records.".format(len(df)), utils.sizeof_format.sizeof_fmt(df_file))
# print(df.columns)
# print(df.index)

# Now, create a generator function (see https://rtree.readthedocs.io/en/latest/performance.html#performance for details)

#
# def generator_function(dataframe):
#     for (idx, row) in dataframe.iterrows():
#         yield (idx, (row.longitude, row.latitude, row.longitude, row.latitude), None)


# THIS METHOD WORKS.
# 1) Create an rtree file, (cannot use generator function unfortunately. Annoying you can either use a generaor, *or*
# serialize it to a file. But not both. WTF.
# 2) tar.gz those files into a .tar.gz file. Cuts storage down to 25% of original index
# 3) When wanting to read, unzip the .tar.gz files, and read the index.

idx_test_file = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                             "..", "scratch_data", "rtree_save_test", "rtree_test.idx"))

# assert os.path.exists(os.path.dirname(idx_test_file))
idx_idx_file = idx_test_file[:-4] + ".idx"
# if os.path.exists(idx_idx_file):
#     os.remove(idx_idx_file)
idx_dat_file = idx_test_file[:-4] + ".dat"
# if os.path.exists(idx_dat_file):
#     os.remove(idx_dat_file)
#
# props = rtree.index.Property()
# # props.overwrite = True
# props.dat_extension = "dat"
# props.idx_extension = "idx"
# # props.dimension = 2
# # props.filename = os.path.splitext(idx_test_file)[0]
#
# print("Generating Spatial Index")
# ridx = rtree.index.Index(os.path.splitext(idx_test_file)[0])
#
# for ix, row in df.iterrows():
#     # Couldn't get this to work with the generator function. But it is working with "insert()", even though it's pretty slow.
#     ridx.insert(ix, (row.longitude, row.latitude), None)
#     if (((ix + 1) % 5000) == 0) or ((ix + 1) == len(df)):
#         utils.progress_bar.ProgressBar(ix + 1, len(df), suffix="{0:,}/{1:,}".format(ix + 1, len(df)))
#
# ridx.close()
#
# print("Done at", utils.sizeof_format.sizeof_fmt(os.path.getsize(idx_test_file) + os.path.getsize(os.path.splitext(idx_test_file)[0] + ".dat")))
#
# idx_copy = rtree.index.Index(os.path.splitext(idx_test_file)[0])
# print(idx_copy)
#
# ridx = rtree.index.Index(generator_function(df)) #, properties=props)
#
# gz_file = os.path.splitext(idx_test_file)[0] + ".tar.gz"
# if os.path.exists(gz_file):
#     os.remove(gz_file)
#
# tfile = tarfile.open(gz_file, "w:gz")
# tfile.add(idx_idx_file, arcname=os.path.basename(idx_idx_file))
# tfile.add(idx_dat_file, arcname=os.path.basename(idx_dat_file))
# tfile.close()
#
#
# # print("Done at", utils.sizeof_format.sizeof_fmt(os.path.getsize(idx_test_file) + os.path.getsize(os.path.splitext(idx_test_file)[0] + ".dat")))
# print("Compression done at", utils.sizeof_format.sizeof_fmt(gz_file))
#
#
# os.remove(idx_idx_file)
# os.remove(idx_dat_file)
#
# tfile = tarfile.open(gz_file)
# tfile.extractall(path=os.path.relpath(os.path.dirname(idx_idx_file)), numeric_owner=True)
# tfile.close()
assert os.path.exists(idx_idx_file) and os.path.exists(idx_dat_file)


idx_copy = rtree.index.Index(os.path.splitext(idx_test_file)[0])

print(idx_copy)
print(df.loc[idx_copy.intersection((20.2, 0.0, 20.25, 0.05), objects=False)])
