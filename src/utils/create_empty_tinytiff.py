# Quick utility to creatwe an empty .tif file to use for the IVERT test utility.

import configfile
import os
from osgeo import gdal

ivert_config = configfile.config()
tiff_location = ivert_config.empty_tiff

if not os.path.exists(tiff_location):
    ds = gdal.GetDriverByName('GTiff').Create(tiff_location, 1, 1, 1, gdal.GDT_Float32)

    ds = None

    print(f"Created {tiff_location}, {os.path.getsize(tiff_location)} bytes")

else:
    print(f"{tiff_location} already exists.")