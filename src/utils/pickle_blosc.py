## pickle_blosc.py -- write and read compressed picklefiles using blosc library compression
import pickle
import blosc


def write(obj, fname, flags='wb'):
    pickled_data = pickle.dumps(obj)
    compressed_pickle = blosc.compress(pickled_data)
    with open(fname, flags) as f:
        f.write(compressed_pickle)
    return


def read(fname, flags='rb'):
    with open(fname, flags) as f:
        compressed_pickle = f.read()

    depressed_pickle = blosc.decompress(compressed_pickle)
    data = pickle.loads(depressed_pickle)
    return data
