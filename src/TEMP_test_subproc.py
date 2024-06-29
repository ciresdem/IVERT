# Just testing. Delete this later.
import icesat2_photon_database

import multiprocessing as mp
import utils.pickle_blosc

def write_obj():
    fname = "../scratch_data/simplestr.blosc2"
    obj = "foobar_Mike2" * int(1e6)
    utils.pickle_blosc.write(obj, fname)

def read_gdf(db):
    fname = "/home/mmacferrin/Research/DATA/ETOPO/data/icesat2/photon_tiles.blosc2"
    # fname = "../scratch_data/simplestr.blosc2"
    if db is None:
        db = icesat2_photon_database.ICESat2_Database()

    obj = db.get_gdf(verbose=True)
    print(obj)

    fout = "../scratch_data/photon_tiles_test.gpkg"
    print("Writing", fout)
    obj.to_file("../scratch_data/photon_tiles_test.gpkg", driver="GPKG")
    print(fout, "written.")

    return

    # df = icesat2_photon_database.ICESat2_Database()
    # return df.get_gdf(verbose=True)

SUBPROC = True

if SUBPROC:

    # fname = "/home/mmacferrin/Research/DATA/ETOPO/data/icesat2/photon_tiles.blosc2"
    # obj = utils.pickle_blosc.read(fname)
    db = icesat2_photon_database.ICESat2_Database()
    obj = db.get_gdf(verbose=True)

    proc = mp.Process(target=read_gdf, args=(db,))
    proc.start()
    print(proc.name, proc.pid)
    proc.join()
    print(proc.exitcode)
    proc.close()

else:
    # write_obj()
    read_gdf(None)

print("Done.")