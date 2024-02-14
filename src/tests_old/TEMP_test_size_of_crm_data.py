import geopandas
import os
import shapely.ops
import shutil
import utils.sizeof_format

import icesat2_photon_database

crm_extent_shp = os.path.join(os.path.dirname(__file__),
                              "..", "data", "crm_subset_extents",
                              "fwdproposed19andcrmextents", "crmWest_extents_cja2.shp")

cudem_extent_shp = os.path.join(os.path.dirname(__file__),
                                "..", "data", "crm_subset_extents",
                                "central_ca_19as_tiles", "central_ca_19as_tiles.shp")

db = icesat2_photon_database.ICESat2_Database()


for shpfile in [crm_extent_shp]:
    # [cudem_extent_shp, crm_extent_shp]:
    shp_gdf = geopandas.read_file(shpfile)
    print(shp_gdf)
    outline = shp_gdf["geometry"].unary_union
    flist = db.query_geopackage(outline, return_whole_records=False)
    # The .h5 have been changed to .feather
    flist = [os.path.splitext(fn)[0] + ".feather" for fn in flist]

    total_size = sum([os.path.getsize(fn) for fn in flist])
    print(len(flist), "tiles,", utils.sizeof_format.sizeof_fmt(total_size))

    print()

    # Put files out to a textfile. Then copy the files into a new directory.
    textfile_name = os.path.abspath(os.path.join(os.path.dirname(crm_extent_shp),
                                                 "..",
                                                 "ivert_tiles",
                                                 "CRM_IVERT_tile_list.txt"))
    with open(textfile_name, 'w') as f:
        f.write("\n".join([os.path.basename(fn) for fn in flist]))
        f.close()
        print(textfile_name, "written.")

    # Now copy all the files over.
    tile_dir = os.path.join(os.path.dirname(textfile_name))
    for source_fname in flist:
        target_fname = os.path.join(tile_dir, os.path.basename(source_fname))
        shutil.copyfile(source_fname, target_fname)
    print(len(flist), "tiles moved to", tile_dir)